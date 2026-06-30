from typing import Any

from posthog.test.base import APIBaseTest
from unittest.mock import patch

from parameterized import parameterized
from rest_framework import status

from posthog.models.personal_api_key import PersonalAPIKey, hash_key_value
from posthog.models.scoping import team_scope
from posthog.models.utils import generate_random_token_personal

from products.warehouse_sources.backend.facade.models import CustomOAuth2Integration, ExternalDataSource

VIEW_MODULE = "products.data_warehouse.backend.presentation.views.custom_oauth2_integration"


@patch(f"{VIEW_MODULE}.is_url_allowed", return_value=(True, None))
class TestCustomOAuth2IntegrationAPI(APIBaseTest):
    def _url(self, suffix: str = "") -> str:
        return f"/api/projects/{self.team.pk}/custom_oauth2_integrations/{suffix}"

    def _create_payload(self, **config_overrides: Any) -> dict[str, Any]:
        return {
            "config": {
                "client_id": "cid",
                "token_url": "https://auth.example.com/token",
                "grant_type": "refresh_token",
                **config_overrides,
            },
            "client_secret": "super-secret",
            "refresh_token": "refresh-orig",
        }

    def test_create_redacts_secrets_and_persists_them(self, _mock):
        response = self.client.post(self._url(), self._create_payload(), format="json")
        assert response.status_code == status.HTTP_201_CREATED, response.json()
        body = response.json()
        # The secrets must never come back, and the encrypted blob must not be exposed at all.
        assert "client_secret" not in body
        assert "refresh_token" not in body
        assert "sensitive_config" not in body
        # Presence is reported without revealing the values.
        assert body["has_client_secret"] is True
        assert body["has_refresh_token"] is True
        assert body["config"]["client_id"] == "cid"
        # ...but they were stored, encrypted, on the row.
        row = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=body["id"])
        assert row.sensitive_config["client_secret"] == "super-secret"
        assert row.sensitive_config["refresh_token"] == "refresh-orig"
        assert row.created_by_id == self.user.pk

    def test_reconnect_replaces_refresh_token_drops_cached_token_and_clears_error(self, _mock):
        create = self.client.post(self._url(), self._create_payload(), format="json")
        integration_id = create.json()["id"]
        # Simulate a broken token with a stale cached access token, as a failing sync would leave it.
        # team_scope: a direct model update outside a request needs the ambient scope (ModelActivityMixin).
        row = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=integration_id)
        row.sensitive_config.update({"access_token": "stale-AT", "token_expiry": "2020-01-01T00:00:00+00:00"})
        row.errors = "TOKEN_REFRESH_FAILED"
        with team_scope(self.team.pk):
            row.save(update_fields=["sensitive_config", "errors"])

        response = self.client.patch(self._url(f"{integration_id}/"), {"refresh_token": "refresh-new"}, format="json")
        assert response.status_code == status.HTTP_200_OK, response.json()

        fresh = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=integration_id)
        assert fresh.sensitive_config["refresh_token"] == "refresh-new"
        # The cached access token is invalidated so the next sync re-mints with the new refresh token.
        assert "access_token" not in fresh.sensitive_config
        assert "token_expiry" not in fresh.sensitive_config
        assert fresh.errors == ""

    def test_token_url_is_immutable_after_create(self, _mock):
        create = self.client.post(self._url(), self._create_payload(), format="json")
        integration_id = create.json()["id"]
        response = self.client.patch(
            self._url(f"{integration_id}/"),
            {"config": {"client_id": "cid", "token_url": "https://evil.example.com/token"}},
            format="json",
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "token_url" in str(response.json())

    def test_refresh_token_grant_requires_a_refresh_token_on_create(self, _mock):
        payload = self._create_payload()
        del payload["refresh_token"]
        response = self.client.post(self._url(), payload, format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "refresh_token" in str(response.json())

    def test_rejects_unsafe_token_url(self, mock_is_url_allowed):
        mock_is_url_allowed.return_value = (False, "Host is not allowed")
        response = self.client.post(self._url(), self._create_payload(), format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "token_url" in str(response.json())

    def test_cannot_read_another_teams_integration(self, _mock):
        other_team = self.create_team_with_organization(self.organization)
        other = CustomOAuth2Integration.objects.for_team(other_team.pk).create(
            team=other_team, config={"client_id": "x"}, sensitive_config={"client_secret": "s"}
        )
        response = self.client.get(self._url(f"{other.id}/"))
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_config_patch_merges_and_preserves_worker_written_and_unsent_keys(self, _mock):
        create = self.client.post(self._url(), self._create_payload(scopes="read write"), format="json")
        integration_id = create.json()["id"]
        # Simulate the worker having stamped `refreshed_at` after a successful mint.
        row = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=integration_id)
        row.config["refreshed_at"] = 1700000000
        with team_scope(self.team.pk):
            row.save(update_fields=["config"])

        # PATCH a single config knob. token_url is immutable, so it must be resent unchanged.
        response = self.client.patch(
            self._url(f"{integration_id}/"),
            {"config": {"token_url": "https://auth.example.com/token", "client_auth_method": "basic"}},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK, response.json()

        fresh = CustomOAuth2Integration.objects.for_team(self.team.pk).get(id=integration_id)
        # The single knob that was sent is applied...
        assert fresh.config["client_auth_method"] == "basic"
        # ...while the worker-written `refreshed_at`, the grant_type (so it isn't silently downgraded to
        # client_credentials), and unsent knobs (scopes, client_id) all survive the merge.
        assert fresh.config["refreshed_at"] == 1700000000
        assert fresh.config["grant_type"] == "refresh_token"
        assert fresh.config["scopes"] == "read write"
        assert fresh.config["client_id"] == "cid"

    def test_duplicate_source_link_returns_400_not_500(self, _mock):
        source = ExternalDataSource.objects.create(team=self.team, team_id=self.team.pk)
        first = self._create_payload()
        first["external_data_source"] = str(source.id)
        assert self.client.post(self._url(), first, format="json").status_code == status.HTTP_201_CREATED, (
            "first link should succeed"
        )

        second = self._create_payload()
        second["external_data_source"] = str(source.id)
        response = self.client.post(self._url(), second, format="json")
        # The unique constraint must surface as a validation error, not an unhandled IntegrityError.
        assert response.status_code == status.HTTP_400_BAD_REQUEST, response.json()
        assert "external_data_source" in str(response.json())

    def test_null_source_link_allows_multiple_rows(self, _mock):
        assert self.client.post(self._url(), self._create_payload(), format="json").status_code == (
            status.HTTP_201_CREATED
        )
        # A second unlinked integration is fine — the constraint only covers non-null source links.
        assert self.client.post(self._url(), self._create_payload(), format="json").status_code == (
            status.HTTP_201_CREATED
        )

    @parameterized.expand(["patch", "delete", "reconnect"])
    def test_cannot_mutate_another_teams_integration(self, _mock, verb: str):
        other_team = self.create_team_with_organization(self.organization)
        other = CustomOAuth2Integration.objects.for_team(other_team.pk).create(
            team=other_team,
            config={"client_id": "x", "token_url": "https://auth.example.com/token"},
            sensitive_config={"client_secret": "s"},
        )

        if verb == "delete":
            response = self.client.delete(self._url(f"{other.id}/"))
        elif verb == "reconnect":
            response = self.client.patch(
                self._url(f"{other.id}/"), {"refresh_token": "stolen-reconnect"}, format="json"
            )
        else:
            response = self.client.patch(
                self._url(f"{other.id}/"),
                {"config": {"client_id": "hijacked", "token_url": "https://auth.example.com/token"}},
                format="json",
            )
        assert response.status_code == status.HTTP_404_NOT_FOUND

        # The other team's row must be untouched by the cross-team attempt.
        reloaded = CustomOAuth2Integration.objects.for_team(other_team.pk).get(id=other.id)
        assert reloaded.config["client_id"] == "x"
        assert reloaded.sensitive_config["client_secret"] == "s"
        assert "refresh_token" not in reloaded.sensitive_config


@patch(f"{VIEW_MODULE}.is_url_allowed", return_value=(True, None))
class TestCustomOAuth2IntegrationAPIKeyScopes(APIBaseTest):
    def _make_api_key(self, scopes: list[str]) -> str:
        value = generate_random_token_personal()
        PersonalAPIKey.objects.create(
            user=self.user,
            label="test",
            secure_value=hash_key_value(value),
            scopes=scopes,
        )
        return value

    def _url(self, suffix: str = "") -> str:
        return f"/api/projects/{self.team.pk}/custom_oauth2_integrations/{suffix}"

    @parameterized.expand(
        [
            ("external_data_source:read", "GET", True),
            ("external_data_source:read", "PATCH", False),
            ("external_data_source:write", "PATCH", True),
            ("other_scope:read", "GET", False),
            ("other_scope:read", "PATCH", False),
        ]
    )
    def test_api_key_scope_gating(self, _mock, scope: str, method: str, should_have_access: bool):
        integration = CustomOAuth2Integration.objects.for_team(self.team.pk).create(
            team=self.team,
            config={"client_id": "x", "token_url": "https://auth.example.com/token"},
            sensitive_config={"client_secret": "s"},
        )
        self.client.force_authenticate(None)
        headers = {"authorization": f"Bearer {self._make_api_key([scope])}"}

        if method == "GET":
            response = self.client.get(self._url(f"{integration.id}/"), headers=headers)
        else:
            response = self.client.patch(
                self._url(f"{integration.id}/"),
                {"config": {"client_id": "x", "token_url": "https://auth.example.com/token"}},
                format="json",
                headers=headers,
            )

        if should_have_access:
            assert response.status_code != status.HTTP_403_FORBIDDEN, (scope, method, response.status_code)
        else:
            assert response.status_code == status.HTTP_403_FORBIDDEN, (scope, method, response.status_code)
