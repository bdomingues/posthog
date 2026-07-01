"""Tests for the slash command webhook view.

The parser and dispatcher are covered by their own tests — this file exercises
the new entry point's responsibilities: request validation, retry handling,
region routing, user resolution, and the bridge into ``dispatch_rules_command``.
"""

from typing import Any
from urllib.parse import urlencode

from unittest.mock import MagicMock, patch

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

        # Every test in this file relies on the same signing-secret / SlackIntegration mocks;
        # lifting them into setUp via ``enterContext`` removes per-test decorator stacks and
        # keeps the test bodies focused on the slash-command behavior under exercise.
        self._mock_config = self.enterContext(
            patch("products.slack_app.backend.views.slack_command.SlackIntegration.slack_config")
        )
        self._mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}

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

    def test_rejects_bad_signature(self) -> None:
        self._mock_config.return_value = {"SLACK_APP_SIGNING_SECRET": "different-secret"}
        response = self._post_slash_command(self._default_payload(text="help"))
        assert response.status_code == 403

    def test_missing_required_payload_fields(self) -> None:
        response = self._post_slash_command(self._default_payload(team_id="", user_id=""))
        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "Missing Slack payload" in body["text"]


class TestSlashCommandDispatch(_SlashCommandTestBase):
    def setUp(self) -> None:
        super().setUp()
        # Replacing ``SlackIntegration`` wholesale shadows the per-attribute ``slack_config``
        # patch on the base class — re-stub ``slack_config`` on the class mock so signature
        # validation still finds the test signing secret.
        mock_slack_cls = self.enterContext(patch("products.slack_app.backend.views.slack_command.SlackIntegration"))
        mock_slack_cls.slack_config.return_value = {"SLACK_APP_SIGNING_SECRET": SIGNING_SECRET}
        mock_slack_cls.return_value.missing_scopes.return_value = frozenset()
        self.mock_dispatch = self.enterContext(
            patch("products.slack_app.backend.views.slack_command.dispatch_rules_command")
        )
        # Slash-command dispatch runs on a background thread so Slack's 3-second ack budget
        # is never blocked by ``users.info`` / ``chat_postMessage``. In tests we drop the
        # thread and run the target inline so assertions can still see ``dispatch_rules_command``
        # being called before the test method returns. ``close_old_connections`` in the worker
        # would close the outer test-case transaction's connection, so stub it out.
        self.enterContext(patch("products.slack_app.backend.views.slack_command.close_old_connections"))
        thread_patch = self.enterContext(patch("products.slack_app.backend.views.slack_command.threading.Thread"))

        def _run_inline(*_args: Any, target: Any, kwargs: dict, **_thread_kwargs: Any) -> Any:
            thread = MagicMock()
            thread.start.side_effect = lambda: target(**kwargs)
            return thread

        thread_patch.side_effect = _run_inline

    def test_help_invokes_dispatch_with_help_action(self) -> None:
        response = self._post_slash_command(self._default_payload(text="help"))

        assert response.status_code == 200
        assert response.content == b""
        self.mock_dispatch.assert_called_once()
        call = self.mock_dispatch.call_args
        parsed = call.args[0]
        assert parsed.action == "help"
        assert call.kwargs["slack_user_id"] == "U123"
        assert call.kwargs["slack_workspace_id"] == "T12345"
        # ``command_prefix`` is what surfaces in user-facing help/error copy — must
        # match the entry point so the strings tell users to type ``/posthog ...``.
        assert call.kwargs["command_prefix"] == "/posthog"

    def test_empty_text_falls_back_to_help(self) -> None:
        response = self._post_slash_command(self._default_payload(text=""))

        assert response.status_code == 200
        self.mock_dispatch.assert_called_once()
        assert self.mock_dispatch.call_args.args[0].action == "help"

    def test_unknown_sub_command_returns_help_text(self) -> None:
        response = self._post_slash_command(self._default_payload(text="frobnicate the widgets"))

        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "didn't recognize" in body["text"]
        self.mock_dispatch.assert_not_called()

    def test_rules_list_dispatches_list_action(self) -> None:
        response = self._post_slash_command(self._default_payload(text="rules list"))

        assert response.status_code == 200
        self.mock_dispatch.assert_called_once()
        assert self.mock_dispatch.call_args.args[0].action == "list"

    def test_project_set_parses_team_id(self) -> None:
        response = self._post_slash_command(self._default_payload(text=f"project {self.team.id}"))

        assert response.status_code == 200
        self.mock_dispatch.assert_called_once()
        parsed = self.mock_dispatch.call_args.args[0]
        assert parsed.action == "project_set"
        assert parsed.project_team_id == self.team.id

    def test_thread_ts_flows_through_to_dispatcher(self) -> None:
        """Slash commands invoked inside a thread carry ``thread_ts`` on the payload;
        passing it through keeps the bot's reply in-thread instead of dropping it at
        the bottom of the channel."""
        response = self._post_slash_command(self._default_payload(text="rules list", thread_ts="1700000000.001"))

        assert response.status_code == 200
        self.mock_dispatch.assert_called_once()
        assert self.mock_dispatch.call_args.kwargs["thread_ts"] == "1700000000.001"


class TestSlashCommandWorkspaceMissing(_SlashCommandTestBase):
    def test_unknown_workspace_returns_not_connected_message(self) -> None:
        response = self._post_slash_command(self._default_payload(team_id="T_UNKNOWN", text="help"))
        assert response.status_code == 200
        body = response.json()
        assert body["response_type"] == "ephemeral"
        assert "isn't connected" in body["text"]
