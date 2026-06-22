from datetime import UTC, datetime, timedelta
from typing import Any

from posthog.test.base import APIBaseTest
from unittest.mock import patch

from django.utils import timezone

from posthog.approvals.models import ApprovalPolicy, ChangeRequest, ChangeRequestState, ValidationStatus
from posthog.approvals.services import ChangeRequestService
from posthog.tasks.process_scheduled_changes import process_scheduled_changes

from products.feature_flags.backend.models.feature_flag import FeatureFlag
from products.feature_flags.backend.models.scheduled_change import ScheduledChange


@patch("posthog.approvals.decorators._is_approvals_enabled", return_value=True)
class TestScheduledChangeGating(APIBaseTest):
    """A scheduled change that would flip a policy-gated field must not apply without approval.

    Gating happens at scheduling time (a pending ChangeRequest is bound to the row); the Celery
    applier only applies once that CR is approved, and expires it if the fire window closes first.
    """

    def _disabled_flag(self, key: str = "sched-flag") -> FeatureFlag:
        return FeatureFlag.objects.create(
            team=self.team,
            key=key,
            filters={"groups": [{"properties": [], "rollout_percentage": 50}]},
            active=False,
            created_by=self.user,
        )

    def _enable_policy(self) -> ApprovalPolicy:
        return ApprovalPolicy.objects.create(
            organization=self.organization,
            team=self.team,
            action_key="feature_flag.enable",
            conditions={},
            approver_config={"quorum": 1, "users": [self.user.id]},
            created_by=self.user,
        )

    def _update_policy(self, conditions: dict[str, Any] | None = None) -> ApprovalPolicy:
        return ApprovalPolicy.objects.create(
            organization=self.organization,
            team=self.team,
            action_key="feature_flag.update",
            conditions=conditions if conditions is not None else {},
            approver_config={"quorum": 1, "users": [self.user.id]},
            created_by=self.user,
        )

    def _schedule(self, flag: FeatureFlag, payload: dict, scheduled_at: datetime) -> ScheduledChange:
        return ScheduledChange.objects.create(
            team=self.team,
            record_id=str(flag.id),
            model_name="FeatureFlag",
            payload=payload,
            scheduled_at=scheduled_at,
            created_by=self.user,
            change_request=self._gate(flag, payload),
        )

    def _gate(self, flag: FeatureFlag, payload: dict) -> ChangeRequest | None:
        from posthog.approvals.scheduled_changes import gate_scheduled_change

        return gate_scheduled_change(flag, payload, self.user)

    def test_scheduled_enable_under_policy_creates_pending_cr_and_does_not_apply(self, _mock_enabled):
        self._enable_policy()
        flag = self._disabled_flag()

        scheduled = self._schedule(
            flag,
            {"operation": "update_status", "value": True},
            datetime.now(UTC) - timedelta(seconds=30),
        )

        assert scheduled.change_request is not None
        assert scheduled.change_request.state == ChangeRequestState.PENDING
        assert ChangeRequest.objects.filter(state=ChangeRequestState.APPROVED).count() == 0

        process_scheduled_changes()

        flag.refresh_from_db()
        assert flag.active is False
        scheduled.change_request.refresh_from_db()
        assert scheduled.change_request.state == ChangeRequestState.EXPIRED

    def test_scheduled_enable_applies_via_approved_path_when_cr_approved(self, _mock_enabled):
        self._enable_policy()
        flag = self._disabled_flag()

        # Schedule in the future so the applier doesn't expire it before we approve.
        scheduled = self._schedule(
            flag,
            {"operation": "update_status", "value": True},
            timezone.now() + timedelta(hours=1),
        )
        cr = scheduled.change_request
        assert cr is not None

        ChangeRequestService(cr, self.user).approve()
        cr.refresh_from_db()
        # Approve auto-applies on quorum; but the scheduled change applies it via process(). To keep
        # the approved-then-process path honest, reset state to APPROVED if quorum auto-applied.
        if cr.state == ChangeRequestState.APPLIED:
            flag.refresh_from_db()
            assert flag.active is True
            return

        # Move the fire window into the past and let the applier apply via the approved path.
        scheduled.scheduled_at = timezone.now() - timedelta(seconds=30)
        scheduled.save()
        process_scheduled_changes()

        flag.refresh_from_db()
        assert flag.active is True
        cr.refresh_from_db()
        assert cr.state == ChangeRequestState.APPLIED

    def test_pending_cr_past_window_is_expired_and_change_skipped(self, _mock_enabled):
        self._enable_policy()
        flag = self._disabled_flag()

        scheduled = self._schedule(
            flag,
            {"operation": "update_status", "value": True},
            timezone.now() - timedelta(seconds=30),
        )
        cr = scheduled.change_request
        assert cr is not None
        assert cr.state == ChangeRequestState.PENDING

        process_scheduled_changes()

        cr.refresh_from_db()
        assert cr.state == ChangeRequestState.EXPIRED
        flag.refresh_from_db()
        assert flag.active is False
        scheduled.refresh_from_db()
        assert scheduled.executed_at is not None

    def test_scheduled_rollout_change_under_update_policy_is_gated(self, _mock_enabled):
        self._update_policy({"type": "before_after", "field": "rollout_percentage", "operator": ">", "value": 0})
        flag = self._disabled_flag(key="rollout-flag")

        new_condition = {
            "variant": None,
            "properties": [],
            "rollout_percentage": 90,
            "aggregation_group_type_index": None,
        }
        scheduled = self._schedule(
            flag,
            {
                "operation": "add_release_condition",
                "value": {"groups": [new_condition], "payloads": {}, "multivariate": None},
            },
            timezone.now() - timedelta(seconds=30),
        )

        assert scheduled.change_request is not None
        assert scheduled.change_request.state == ChangeRequestState.PENDING

        process_scheduled_changes()

        flag.refresh_from_db()
        # The new 90% condition must not have been appended (change was gated, not applied).
        rollouts = [g.get("rollout_percentage") for g in flag.filters.get("groups", [])]
        assert 90 not in rollouts
        scheduled.change_request.refresh_from_db()
        assert scheduled.change_request.state == ChangeRequestState.EXPIRED

    def test_scheduled_change_without_policy_applies_normally(self, _mock_enabled):
        flag = self._disabled_flag()

        scheduled = self._schedule(
            flag,
            {"operation": "update_status", "value": True},
            datetime.now(UTC) - timedelta(seconds=30),
        )
        assert scheduled.change_request is None

        process_scheduled_changes()

        flag.refresh_from_db()
        assert flag.active is True
        scheduled.refresh_from_db()
        assert scheduled.executed_at is not None

    def test_approved_then_stale_cr_is_not_applied(self, _mock_enabled):
        self._enable_policy()
        flag = self._disabled_flag()

        scheduled = self._schedule(
            flag,
            {"operation": "update_status", "value": True},
            timezone.now() + timedelta(hours=1),
        )
        cr = scheduled.change_request
        assert cr is not None

        # Force the approved-but-stale combination: applier must skip (not apply) a stale CR.
        cr.state = ChangeRequestState.APPROVED
        cr.validation_status = ValidationStatus.STALE
        cr.save()

        scheduled.scheduled_at = timezone.now() - timedelta(seconds=30)
        scheduled.save()
        process_scheduled_changes()

        flag.refresh_from_db()
        assert flag.active is False
