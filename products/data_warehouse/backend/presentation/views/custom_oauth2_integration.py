from typing import Any

from django.db.models import Q

import structlog
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema_field
from prometheus_client import Counter
from rest_framework import serializers, viewsets
from rest_framework.exceptions import PermissionDenied

from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.api.scoped_related_fields import TeamScopedPrimaryKeyRelatedField
from posthog.models.scoping import team_scope
from posthog.security.url_validation import is_url_allowed

from products.warehouse_sources.backend.facade.models import CustomOAuth2Integration, ExternalDataSource

logger = structlog.get_logger(__name__)

# Credential lifecycle events on the custom OAuth2 store, so secret creation and rotation are graphable.
# "created" = a new integration with secrets; "reconnected" = a PATCH that rotated the client secret
# or refresh token (the recovery path for a revoked/expired token).
custom_oauth2_integration_credential_counter = Counter(
    "warehouse_custom_oauth2_integration_credential",
    "Number of custom OAuth2 integration credential lifecycle events from the API",
    labelnames=["action"],
)

_GRANT_TYPE_CHOICES = [("client_credentials", "client_credentials"), ("refresh_token", "refresh_token")]
_CLIENT_AUTH_METHOD_CHOICES = [("body", "body"), ("basic", "basic")]


class CustomOAuth2ConfigSerializer(serializers.Serializer):
    """The non-secret OAuth2 client config — the exact knobs the worker's OAuth2 auth engine accepts.

    Only the fields declared here round-trip; unknown keys are dropped on write (so the API can't pollute
    the stored config) and hidden on read. Secrets (client_secret, refresh_token) are never part of this.
    """

    client_id = serializers.CharField(help_text="OAuth2 client ID of the customer-owned client.")
    token_url = serializers.URLField(
        help_text="Token endpoint the worker POSTs to mint access tokens. Receives the client secret, so "
        "it must be a trusted public host; internal/loopback hosts are rejected."
    )
    grant_type = serializers.ChoiceField(
        choices=_GRANT_TYPE_CHOICES,
        default="client_credentials",
        help_text="OAuth2 grant. client_credentials (machine-to-machine) or refresh_token (a "
        "pre-obtained refresh token the customer supplies). authorization_code is not supported.",
    )
    scopes = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        help_text="Space-separated OAuth2 scopes, if the provider needs them.",
    )
    access_token_name = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        help_text="Response field holding the access token, when it isn't the standard 'access_token'.",
    )
    expires_in_name = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        help_text="Response field holding the token TTL, when it isn't the standard 'expires_in'.",
    )
    expiry_date_format = serializers.CharField(
        required=False,
        allow_blank=True,
        allow_null=True,
        help_text="strptime format to parse an absolute-datetime expiry, for providers that return one "
        "instead of a TTL in seconds.",
    )
    extra_token_request_params = serializers.DictField(
        child=serializers.CharField(),
        required=False,
        help_text="Extra form params added to the token request body (e.g. an 'audience' some providers require).",
    )
    token_request_headers = serializers.DictField(
        child=serializers.CharField(),
        required=False,
        help_text="Extra headers sent on the token request, for providers that need them.",
    )
    client_auth_method = serializers.ChoiceField(
        choices=_CLIENT_AUTH_METHOD_CHOICES,
        default="body",
        help_text="How the client credentials are sent: 'body' (form params) or 'basic' (HTTP Basic).",
    )
    refreshed_at = serializers.IntegerField(
        read_only=True,
        required=False,
        allow_null=True,
        help_text="Unix seconds of the last successful token mint, set by the sync worker.",
    )

    def validate_token_url(self, value: str) -> str:
        allowed, reason = is_url_allowed(value)
        if not allowed:
            raise serializers.ValidationError(reason or "token_url is not an allowed destination.")
        return value


class CustomOAuth2IntegrationSerializer(serializers.ModelSerializer):
    """Read/write a custom REST source's customer-owned OAuth2 integration.

    The encrypted `sensitive_config` (client secret + tokens) is deliberately absent from `fields` —
    redaction by omission. Secrets are accepted as write-only inputs and never returned; the stored
    booleans below report presence only.
    """

    config = CustomOAuth2ConfigSerializer(
        required=False, help_text="Non-secret OAuth2 client config. Required on create."
    )
    external_data_source = TeamScopedPrimaryKeyRelatedField(
        queryset=ExternalDataSource.objects.all(),
        required=False,
        allow_null=True,
        help_text="The custom source this integration backs, if already created. The source points back via "
        "its `auth_oauth2_integration_id`; this is the reverse link for cleanup and the unique constraint.",
    )
    client_secret = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
        help_text="OAuth2 client secret (write-only; never returned). Provide on create or to reconnect.",
    )
    refresh_token = serializers.CharField(
        write_only=True,
        required=False,
        allow_blank=True,
        help_text="Pre-obtained refresh token for the refresh_token grant (write-only; never returned). "
        "PATCH a new value to reconnect a source whose refresh token expired or was revoked.",
    )
    has_client_secret = serializers.SerializerMethodField(
        help_text="Whether a client secret is stored (the secret itself is never returned)."
    )
    has_refresh_token = serializers.SerializerMethodField(
        help_text="Whether a refresh token is stored (the secret itself is never returned)."
    )
    errors = serializers.CharField(  # type: ignore[assignment]  # field name shadows BaseSerializer.errors; DRF moves it off the class
        read_only=True,
        help_text="Non-empty (TOKEN_REFRESH_FAILED) while the stored token is failing to refresh; cleared on "
        "a successful reconnect or sync.",
    )

    class Meta:
        model = CustomOAuth2Integration
        fields = [
            "id",
            "external_data_source",
            "config",
            "client_secret",
            "refresh_token",
            "has_client_secret",
            "has_refresh_token",
            "errors",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "errors", "created_at", "updated_at"]

    @extend_schema_field(OpenApiTypes.BOOL)
    def get_has_client_secret(self, obj: CustomOAuth2Integration) -> bool:
        return bool(obj.sensitive_config.get("client_secret"))

    @extend_schema_field(OpenApiTypes.BOOL)
    def get_has_refresh_token(self, obj: CustomOAuth2Integration) -> bool:
        return bool(obj.sensitive_config.get("refresh_token"))

    def validate_external_data_source(self, value: ExternalDataSource | None) -> ExternalDataSource | None:
        # The model's UniqueConstraint(team, external_data_source) only enforces non-null links. `team`
        # isn't a serializer field, so DRF skips the auto UniqueTogetherValidator and a duplicate link
        # would surface as an IntegrityError (500). Enforce it here as a 400 instead. The queryset is
        # team-scoped so it can never leak whether another team already links the same source.
        if value is None:
            return value
        team = self.context["get_team"]()
        clash = CustomOAuth2Integration.objects.for_team(team.pk).filter(external_data_source=value)
        if self.instance is not None:
            clash = clash.exclude(pk=self.instance.pk)
        if clash.exists():
            raise serializers.ValidationError("This source already has an OAuth2 integration.")
        return value

    def validate(self, attrs: dict[str, Any]) -> dict[str, Any]:
        is_create = self.instance is None
        config = attrs.get("config")
        if is_create and not config:
            raise serializers.ValidationError({"config": "Required when creating an integration."})

        grant_type = (
            config.get("grant_type", "client_credentials")
            if config
            else (
                self.instance.config.get("grant_type", "client_credentials") if self.instance else "client_credentials"
            )
        )
        if is_create and grant_type == "refresh_token" and not attrs.get("refresh_token"):
            raise serializers.ValidationError(
                {"refresh_token": "Required when creating a refresh_token-grant integration."}
            )

        # token_url is immutable after create: it receives the client secret, so repointing it while
        # keeping the stored secret would let an editor exfiltrate that secret to a host they control.
        if self.instance is not None and config:
            new_url = config.get("token_url")
            if new_url and new_url != self.instance.config.get("token_url"):
                raise serializers.ValidationError(
                    {"config": {"token_url": "token_url cannot be changed after creation."}}
                )
        return attrs

    def create(self, validated_data: dict[str, Any]) -> CustomOAuth2Integration:
        sensitive_config: dict[str, Any] = {}
        client_secret = validated_data.pop("client_secret", None)
        refresh_token = validated_data.pop("refresh_token", None)
        if client_secret:
            sensitive_config["client_secret"] = client_secret
        if refresh_token:
            sensitive_config["refresh_token"] = refresh_token
        validated_data["sensitive_config"] = sensitive_config
        instance = super().create(validated_data)
        custom_oauth2_integration_credential_counter.labels("created").inc()
        logger.info(
            "Created custom OAuth2 integration",
            integration_id=str(instance.pk),
            team_id=instance.team_id,
            grant_type=instance.config.get("grant_type", "client_credentials"),
        )
        return instance

    def update(self, instance: CustomOAuth2Integration, validated_data: dict[str, Any]) -> CustomOAuth2Integration:
        client_secret = validated_data.pop("client_secret", None)
        refresh_token = validated_data.pop("refresh_token", None)
        # `config` maps onto a JSONField, so super().update() would setattr() the whole dict, wiping any
        # key the client didn't resend — including worker-written ones (`refreshed_at`) and the stored
        # `grant_type` (silently downgrading a refresh_token integration). Merge instead so a partial
        # config PATCH only overlays the keys it carries.
        incoming_config = validated_data.pop("config", None)
        if incoming_config is not None:
            instance.config = {**(instance.config or {}), **incoming_config}
        rotated_secret = False
        if client_secret:
            instance.sensitive_config["client_secret"] = client_secret
            rotated_secret = True
        if refresh_token:
            instance.sensitive_config["refresh_token"] = refresh_token
            # A new refresh token invalidates the cached access token; drop it so the next sync re-mints.
            instance.sensitive_config.pop("access_token", None)
            instance.sensitive_config.pop("token_expiry", None)
            rotated_secret = True
        if rotated_secret:
            # Reconnecting clears the broken-token state so the source stops surfacing the error.
            instance.errors = ""
        updated = super().update(instance, validated_data)
        if rotated_secret:
            custom_oauth2_integration_credential_counter.labels("reconnected").inc()
            logger.info(
                "Reconnected custom OAuth2 integration",
                integration_id=str(updated.pk),
                team_id=updated.team_id,
                rotated_client_secret=bool(client_secret),
                rotated_refresh_token=bool(refresh_token),
            )
        return updated


class CustomOAuth2IntegrationViewSet(TeamAndOrgViewSetMixin, viewsets.ModelViewSet):
    """Manage the customer-owned OAuth2 integrations that back custom REST sources.

    Create one, then point a custom source at it via the source's `auth_oauth2_integration_id`. PATCH a
    new `refresh_token` (or `client_secret`) to reconnect a source whose token expired or was revoked —
    that clears the broken-token error without rebuilding the source. Secrets are write-only and never
    returned; reads expose the non-secret config plus presence booleans.
    """

    scope_object = "external_data_source"
    # `.unscoped()` is import-safe (the fail-closed manager raises on `.all()` without team context); the
    # mixin scopes every request to the team, and `safely_get_queryset` re-applies the filter.
    queryset = CustomOAuth2Integration.objects.unscoped()
    serializer_class = CustomOAuth2IntegrationSerializer
    ordering = "-created_at"

    def safely_get_queryset(self, queryset: Any) -> Any:
        queryset = queryset.filter(team_id=self.team_id)
        # `scope_object = "external_data_source"` but this serves CustomOAuth2Integration rows, which aren't
        # in the RBAC model→resource map — so object-level checks no-op and a user with access to ANY source
        # could otherwise see every integration in the team. Gate each integration on its linked source's
        # visibility instead. Unlinked rows (the brief create→link window) stay team-visible since they
        # don't yet back a restricted source.
        accessible_sources = self.user_access_control.filter_queryset_by_access_level(
            ExternalDataSource.objects.filter(team_id=self.team_id)
        )
        queryset = queryset.filter(
            Q(external_data_source__isnull=True) | Q(external_data_source__in=accessible_sources)
        )
        return queryset.order_by(self.ordering)

    def _assert_can_edit_source(self, source: ExternalDataSource | None) -> None:
        # Reads are gated by safely_get_queryset (source visibility), but rotating a secret, retargeting, or
        # deleting an integration must require editor — not merely viewer — access to the source it backs.
        # Unlinked rows have no source to gate on (the creator manages them until a source points at them).
        if source is not None and not self.user_access_control.check_access_level_for_object(source, "editor"):
            raise PermissionDenied("You do not have edit access to the data source this integration backs.")

    def perform_create(self, serializer: serializers.BaseSerializer) -> None:
        self._assert_can_edit_source(serializer.validated_data.get("external_data_source"))
        # team_scope(): ModelActivityMixin re-queries through the model's fail-closed manager on save,
        # and the team-nested mixin scopes querysets by explicit team_id rather than setting the ambient
        # context that manager reads — so the save needs the scope set explicitly.
        with team_scope(self.team_id):
            serializer.save(team_id=self.team_id, created_by=self.request.user)

    def perform_update(self, serializer: serializers.BaseSerializer) -> None:
        # A PATCH can also rebind the integration onto a different source, so require editor access to both
        # the current source and the requested one — otherwise an editor of one source could reserve the
        # integration against a restricted source they can't edit.
        assert serializer.instance is not None  # perform_update always has a bound instance
        current_source = serializer.instance.external_data_source
        requested_source = serializer.validated_data.get("external_data_source", current_source)
        self._assert_can_edit_source(current_source)
        self._assert_can_edit_source(requested_source)
        with team_scope(self.team_id):
            serializer.save()

    def perform_destroy(self, instance: CustomOAuth2Integration) -> None:
        self._assert_can_edit_source(instance.external_data_source)
        with team_scope(self.team_id):
            instance.delete()
