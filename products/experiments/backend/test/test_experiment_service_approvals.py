from typing import Any

from posthog.test.base import APIBaseTest
from unittest.mock import patch

from rest_framework.test import APIRequestFactory

from posthog.approvals.exceptions import ApprovalRequired
from posthog.approvals.models import ApprovalPolicy
from posthog.constants import AvailableFeature

from products.experiments.backend.experiment_service import ExperimentService
from products.experiments.backend.models.experiment import Experiment


@patch("posthog.approvals.decorators._is_approvals_enabled", return_value=True)
class TestExperimentServiceApprovals(APIBaseTest):
    """launch/pause/resume flip the linked flag's `active` state, which must pass through
    the FeatureFlagSerializer approval gate (action `feature_flag.enable`/`feature_flag.disable`)."""

    _METRIC = {
        "kind": "ExperimentMetric",
        "metric_type": "mean",
        "uuid": "m1",
        "source": {"kind": "EventsNode", "event": "$pageview"},
    }

    def setUp(self):
        super().setUp()
        self.organization.available_product_features = [
            {"key": AvailableFeature.APPROVALS, "name": AvailableFeature.APPROVALS}
        ]
        self.organization.save()

    def _service(self) -> ExperimentService:
        return ExperimentService(team=self.team, user=self.user)

    def _request(self) -> Any:
        request = APIRequestFactory().post("/fake")
        request.user = self.user
        return request

    def _create_enable_policy(self) -> ApprovalPolicy:
        return ApprovalPolicy.objects.create(
            organization=self.organization,
            team=self.team,
            action_key="feature_flag.enable",
            conditions={},
            approver_config={"quorum": 1, "users": [self.user.id]},
            created_by=self.user,
        )

    def _create_disable_policy(self) -> ApprovalPolicy:
        return ApprovalPolicy.objects.create(
            organization=self.organization,
            team=self.team,
            action_key="feature_flag.disable",
            conditions={},
            approver_config={"quorum": 1, "users": [self.user.id]},
            created_by=self.user,
        )

    def _create_update_policy(self) -> ApprovalPolicy:
        return ApprovalPolicy.objects.create(
            organization=self.organization,
            team=self.team,
            action_key="feature_flag.update",
            conditions={},
            approver_config={"quorum": 1, "users": [self.user.id]},
            created_by=self.user,
        )

    def _create_draft_experiment(self, feature_flag_key: str) -> Experiment:
        return self._service().create_experiment(
            name="Approval Test",
            feature_flag_key=feature_flag_key,
            metrics=[self._METRIC],
            primary_metrics_ordered_uuids=["m1"],
            allow_unknown_events=True,
        )

    def _create_launched_experiment(self, feature_flag_key: str) -> Experiment:
        experiment = self._create_draft_experiment(feature_flag_key)
        # Launch without a policy in place so the flag is genuinely active afterwards.
        self._service().launch_experiment(experiment, request=self._request())
        experiment.refresh_from_db()
        return experiment

    def test_launch_under_enable_policy_requires_approval(self, _mock_enabled):
        experiment = self._create_draft_experiment("launch-gated")
        self._create_enable_policy()

        with self.assertRaises(ApprovalRequired):
            self._service().launch_experiment(experiment, request=self._request())

        experiment.refresh_from_db()
        experiment.feature_flag.refresh_from_db()
        assert experiment.feature_flag.active is False
        assert experiment.start_date is None

    def test_pause_under_disable_policy_requires_approval(self, _mock_enabled):
        experiment = self._create_launched_experiment("pause-gated")
        assert experiment.feature_flag.active is True
        self._create_disable_policy()

        with self.assertRaises(ApprovalRequired):
            self._service().pause_experiment(experiment, request=self._request())

        experiment.feature_flag.refresh_from_db()
        assert experiment.feature_flag.active is True

    def test_resume_under_enable_policy_requires_approval(self, _mock_enabled):
        experiment = self._create_launched_experiment("resume-gated")
        # Pause it first (no policy yet) so it's genuinely paused.
        self._service().pause_experiment(experiment, request=self._request())
        experiment.feature_flag.refresh_from_db()
        assert experiment.feature_flag.active is False

        self._create_enable_policy()

        with self.assertRaises(ApprovalRequired):
            self._service().resume_experiment(experiment, request=self._request())

        experiment.feature_flag.refresh_from_db()
        assert experiment.feature_flag.active is False

    def test_launch_without_policy_flips_flag_and_sets_start_date(self, _mock_enabled):
        experiment = self._create_draft_experiment("launch-ungated")

        launched = self._service().launch_experiment(experiment, request=self._request())

        assert launched.start_date is not None
        launched.feature_flag.refresh_from_db()
        assert launched.feature_flag.active is True

    def test_ship_variant_under_update_policy_requires_approval(self, _mock_enabled):
        # ship_variant rewrites the flag's variant rollout (50/50 -> 100/0), a rollout_percentage
        # change gated by feature_flag.update. It routes through FeatureFlagSerializer, so the gate
        # must fire and leave the flag's filters untouched.
        experiment = self._create_launched_experiment("ship-gated")
        original_filters = experiment.feature_flag.filters
        original_variants = original_filters["multivariate"]["variants"]
        assert any(v["key"] == "test" and v["rollout_percentage"] == 50 for v in original_variants)

        self._create_update_policy()

        with self.assertRaises(ApprovalRequired):
            self._service().ship_variant(experiment, variant_key="test", request=self._request())

        experiment.refresh_from_db()
        experiment.feature_flag.refresh_from_db()
        # Flag distribution unchanged and experiment not ended.
        assert experiment.feature_flag.filters["multivariate"]["variants"] == original_variants
        assert experiment.end_date is None
