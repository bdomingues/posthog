"""Tests for the slash command webhook view.

The parser and dispatcher are covered by their own tests — this file exercises
the new entry point's responsibilities: request validation, region routing,
user resolution, and the bridge into ``dispatch_rules_command``.
"""

from typing import Any
from urllib.parse import urlencode

from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.test.client import RequestFactory

from rest_framework.test import APIClient

from posthog.helpers.slack_scopes import REQUIRED_SLACK_SCOPES
from posthog.models.integration import Integration
from posthog.models.organization import Organization, OrganizationMembership
from posthog.models.team.team import Team
from posthog.models.user import User

from products.slack_app.backend.models import SlackUserProfileCache
from products.slack_app.backend.tests.helpers import sign_slack_request

SIGNING_SECRET = "posthog-code-test-secret"
SLASH_COMMAND_PATH = "/slack/command-callback/"


class _SlashCommandTestBase(TestCase):
    def setUp(self) -> None:
        cache.clear()
        self.client = APIClient()
        self.factory = RequestFactory()
        self.organization = Organization.objects.create(name="Test Org")
        self.team = Team.objects.create(organization=self.organization, name="Test Team")
        self.user = User.objects.create(email="dev@example.com", distinct_id="user-1")
        OrganizationMembership.objects.create(organization=self.organization, user=self.user)
        self.user.current_organization = self.organization
        self.user.current_team = self.team
        self.user.save()
        self.integration = Integration.objects.create(
            team=self.team,
            kind="slack",
            integration_id="T12345",
            config={"scope": ",".join(sorted(REQUIRED_SLACK_SCOPES))},
            sensitive_config={"access_token": "xoxb-posthog-code-test"},
        )

        from django.utils import timezone

        SlackUserProfileCache.objects.create(
            integration=self.integration,
            slack_user_id="U123",
            email="dev@example.com",
            display_name="Dev",
            real_name="Dev User",
            refreshed_at=timezone.now(),
        )

    def _post_slash_command(self, payload: dict[str, str], **extra_headers: str) -> Any:
        body = urlencode(payload).encode()
        signature, ts = sign_slack_request(body, SIGNING_SECRET)
        return self.client.post(
            SLASH_COMMAND_PATH,
            data=body,
            content_type="application/x-www-form-urlencoded",
            HTTP_X_SLACK_SIGNATURE=signature,
            HTTP_X_SLACK_REQUEST_TIMESTAMP=ts,
            **extra_headers,
        )

    def _default_payload(self, **overrides: str) -> dict[str, str]:
        payload = {
            "command": "/posthog",
            "team_id": "T12345",
            "user_id": "U123",
            "channel_id": "C001",
            "text": "",
            "response_url": "https://hooks.slack.example/abc",
            "trigger_id": "trig-1",
        }
        payload.update(overrides)
        return payload


class TestSlashCommandWebhookValidation(_SlashCommandTestBase):
    def test_method_not_allowed_on_get(self) -> None:
        response = self.client.get(SLASH_COMMAND_PATH)
        assert response.status_code == 405

    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_rejects_bad_signature(self, mock_config: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": "different-secret"}
        response = self._post_slash_command(self._default_payload(text="help"))
        assert response.status_code == 403

    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_missing_required_payload_fields(self, mock_config: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        response = self._post_slash_command(self._default_payload(team_id="", user_id=""))
        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "Missing Slack payload" in body["text"]


class TestSlashCommandDispatch(_SlashCommandTestBase):
    @patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_help_invokes_dispatch_with_help_action(
        self, mock_config: Any, mock_slack_cls: Any, mock_dispatch: Any
    ) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        mock_slack_cls.return_value.missing_scopes.return_value = frozenset()
        mock_slack_cls.slack_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

        response = self._post_slash_command(self._default_payload(text="help"))

        assert response.status_code == 200
        assert response.content == b""
        mock_dispatch.assert_called_once()
        call_kwargs = mock_dispatch.call_args
        parsed = call_kwargs.args[0]
        assert parsed.action == "help"
        assert call_kwargs.kwargs["slack_user_id"] == "U123"
        assert call_kwargs.kwargs["slack_workspace_id"] == "T12345"

    @patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_empty_text_falls_back_to_help(self, mock_config: Any, mock_slack_cls: Any, mock_dispatch: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        mock_slack_cls.return_value.missing_scopes.return_value = frozenset()
        mock_slack_cls.slack_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

        response = self._post_slash_command(self._default_payload(text=""))

        assert response.status_code == 200
        mock_dispatch.assert_called_once()
        assert mock_dispatch.call_args.args[0].action == "help"

    @patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_unknown_sub_command_returns_help_text(self, mock_config: Any, mock_dispatch: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

        response = self._post_slash_command(self._default_payload(text="frobnicate the widgets"))

        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "didn't recognize" in body["text"]
        mock_dispatch.assert_not_called()

    @patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_rules_list_dispatches_list_action(self, mock_config: Any, mock_slack_cls: Any, mock_dispatch: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        mock_slack_cls.return_value.missing_scopes.return_value = frozenset()
        mock_slack_cls.slack_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

        response = self._post_slash_command(self._default_payload(text="rules list"))

        assert response.status_code == 200
        mock_dispatch.assert_called_once()
        assert mock_dispatch.call_args.args[0].action == "list"

    @patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration")
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_project_set_parses_team_id(self, mock_config: Any, mock_slack_cls: Any, mock_dispatch: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        mock_slack_cls.return_value.missing_scopes.return_value = frozenset()
        mock_slack_cls.slack_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

        response = self._post_slash_command(self._default_payload(text=f"project {self.team.id}"))

        assert response.status_code == 200
        mock_dispatch.assert_called_once()
        parsed = mock_dispatch.call_args.args[0]
        assert parsed.action == "project_set"
        assert parsed.project_team_id == self.team.id


class TestSlashCommandWorkspaceMissing(_SlashCommandTestBase):
    @patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
    def test_unknown_workspace_returns_not_connected_message(self, mock_config: Any) -> None:
        mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        response = self._post_slash_command(self._default_payload(team_id="T_UNKNOWN", text="help"))
        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "isn't connected" in body["text"]
