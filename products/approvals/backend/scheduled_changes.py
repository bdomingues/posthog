"""Creation-time approval gating for feature-flag scheduled changes.

A ``ScheduledChange`` row is created ungated and later applied by the Celery task
``process_scheduled_changes`` via ``FeatureFlag.scheduled_changes_dispatcher``. Without this
module a scheduled change could flip a policy-gated field (enabling a flag, raising rollout)
with no approval. Here we evaluate the same approval actions the request-time gate uses
(``decorators._evaluate_gate``) against the change a scheduled row *would* apply, and — when an
enabled policy requires approval — create a pending ``ChangeRequest`` bound to the scheduled
change. The applier then keys off that binding (see ``process_scheduled_changes``).
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from django.http import HttpRequest

from products.approvals.backend import decorators
from products.approvals.backend.models import ChangeRequest, ChangeRequestState, ValidationStatus
from products.approvals.backend.services import apply_change_request

if TYPE_CHECKING:
    from products.feature_flags.backend.models.feature_flag import FeatureFlag
    from products.feature_flags.backend.models.scheduled_change import ScheduledChange

logger = logging.getLogger(__name__)


def scheduled_change_serializer_data(flag: "FeatureFlag", payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Translate a scheduled-change payload into the serializer-shaped change it would apply.

    Mirrors the operation handling in ``FeatureFlag.scheduled_changes_dispatcher`` so the gate
    sees exactly the change the applier would make. Returns ``None`` when the payload is malformed
    or its operation is unrecognized — the applier surfaces those errors at apply time, the gate
    simply declines to create a CR for a change it cannot interpret.
    """
    operation = payload.get("operation")
    value = payload.get("value") if "value" in payload else None
    if operation is None or "value" not in payload:
        return None

    if operation == "update_status":
        return {"active": value}

    current_filters = flag.get_filters()

    if operation == "add_release_condition":
        new_groups = value.get("groups", []) if isinstance(value, dict) else []
        return {
            "filters": {
                **current_filters,
                "groups": current_filters.get("groups", []) + new_groups,
            }
        }

    if operation == "update_variants":
        if not isinstance(value, dict):
            return None
        new_variants = value.get("variants", [])
        new_payloads = value.get("payloads", {})
        updated_multivariate = current_filters.get("multivariate", {})
        updated_multivariate["variants"] = new_variants
        return {
            "filters": {
                **current_filters,
                "multivariate": updated_multivariate,
                "payloads": new_payloads,
            }
        }

    return None


def gate_scheduled_change(flag: "FeatureFlag", payload: dict[str, Any], user) -> Optional[ChangeRequest]:
    """Evaluate the approval gate for a scheduled change and create a pending CR if required.

    Returns the created ``ChangeRequest`` when an enabled policy gates the change the scheduled
    payload would apply, otherwise ``None`` (no policy, approvals disabled, or the change doesn't
    match any gated action). Reuses the request-time gate's action detection and CR creation so
    creation-time gating and request-time gating stay in lockstep.
    """
    # Deferred: feature_flags' serializer/actions transitively import posthog.tasks, which imports
    # this module — eager imports would create a circular import at startup.
    from products.approvals.backend.actions.feature_flags import (  # noqa: PLC0415
        DisableFeatureFlagAction,
        EnableFeatureFlagAction,
        UpdateFeatureFlagAction,
    )

    from products.feature_flags.backend.api.feature_flag import FeatureFlagSerializer  # noqa: PLC0415

    team = flag.team
    organization = team.organization

    if not decorators._is_approvals_enabled(organization):
        return None

    serializer_data = scheduled_change_serializer_data(flag, payload)
    if serializer_data is None:
        return None

    feature_flag_actions = [EnableFeatureFlagAction, DisableFeatureFlagAction, UpdateFeatureFlagAction]

    # Build a partial-update serializer over the real flag so detect()/extract_intent() read the
    # change out of validated_data exactly as they do for an API-driven update.
    http_request = HttpRequest()
    http_request.user = user or flag.created_by  # type: ignore[assignment]
    http_request.method = "PATCH"
    context = {
        "request": http_request,
        "team_id": team.id,
        "project_id": team.project_id,
        "get_team": lambda: team,
        "get_organization": lambda: organization,
    }
    serializer = FeatureFlagSerializer(instance=flag, data=serializer_data, partial=True, context=context)
    if not serializer.is_valid():
        # An invalid change can't be applied either; let the applier surface the error.
        return None

    # The gate's serializer path reads the validated change from args[1] (update's validated_data).
    gate_args: tuple = (flag, dict(serializer.validated_data))

    matched_action = None
    matched_policy = None
    for action_class in feature_flag_actions:
        try:
            if action_class.detect(http_request, serializer, *gate_args):
                policy = decorators._check_policy_for_action(action_class, team, organization)
                if policy:
                    matched_action = action_class
                    matched_policy = policy
                    break
        except Exception:
            logger.exception(
                "Error detecting action for scheduled change",
                extra={"action": action_class.key, "flag_id": flag.id},
            )

    if not matched_action or not matched_policy:
        return None

    result = decorators._evaluate_gate(
        action_class=matched_action,
        request=http_request,
        team=team,
        organization=organization,
        policy=matched_policy,
        view_or_serializer=serializer,
        args=gate_args,
        kwargs={},
    )

    if result.action in ("require_approval", "duplicate"):
        return result.change_request

    return None


def apply_gated_scheduled_change(scheduled_change: "ScheduledChange") -> bool:
    """Decide what to do with a gated scheduled change at fire time.

    Returns ``True`` when the caller should dispatch the change through the normal
    ``scheduled_changes_dispatcher`` path (no binding CR, or it has already been applied via the
    approved path here), and ``False`` when the change must be skipped (its CR is still pending and
    the fire window has closed, so the CR is expired and nothing is applied).

    - Bound CR is APPROVED and not stale: apply via ``apply_change_request`` (the approved,
      non-re-gating path) and return ``False`` so the caller does not dispatch again.
    - Bound CR is still PENDING: the fire window has closed (the row is only processed once
      ``scheduled_at <= now``), so mark the CR EXPIRED and return ``False`` — skip the change.
    - Bound CR is in any other terminal state (APPLIED/REJECTED/EXPIRED/FAILED): do not re-apply;
      return ``False``.
    - No bound CR: return ``True`` so the caller dispatches as before (ungated).
    """
    change_request = scheduled_change.change_request
    if change_request is None:
        return True

    if (
        change_request.state == ChangeRequestState.APPROVED
        and change_request.validation_status != ValidationStatus.STALE
    ):
        apply_change_request(change_request)
        return False

    if change_request.state == ChangeRequestState.PENDING:
        # Time window closed with no approval — expire the request (terminal) and skip the change.
        change_request.state = ChangeRequestState.EXPIRED
        change_request.save(update_fields=["state"])
        logger.info(
            "Expired pending change request for scheduled change past its fire window",
            extra={
                "scheduled_change_id": scheduled_change.id,
                "change_request_id": str(change_request.id),
            },
        )
        return False

    # APPLIED / REJECTED / EXPIRED / FAILED: nothing to apply.
    return False


def regate_recurring_scheduled_change(scheduled_change: "ScheduledChange") -> Optional[ChangeRequest]:
    """Create a fresh pending CR for the next occurrence of a recurring gated schedule.

    A bound ChangeRequest is single-use — it carries one occurrence's intent. When a recurring
    schedule advances to its next fire, re-run the gate against the flag's current state so the
    next occurrence is independently gated. Returns the new CR (or None if no policy now applies).
    """
    from products.feature_flags.backend.models.feature_flag import FeatureFlag  # noqa: PLC0415

    flag = FeatureFlag.objects.filter(id=scheduled_change.record_id, team_id=scheduled_change.team_id).first()
    if flag is None:
        return None
    return gate_scheduled_change(flag, scheduled_change.payload, scheduled_change.created_by)
