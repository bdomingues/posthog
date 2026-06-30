"""Slack slash command entry point.

Handles ``POST /slack/command-callback`` — the webhook Slack hits when a user
runs ``/posthog ...`` in a channel or DM. The vocabulary mirrors the
``@PostHog <command>`` mention path (``help``, ``rules ...``, ``project ...``);
free-text task creation stays on the mention path because slash commands lack
the thread context the task workflow depends on.

Slack imposes a 3-second response budget on slash commands. Dispatch is kept
synchronous to match the event-callback path; the heavy posting work already
goes through ``chat_postMessage`` / ``chat_postEphemeral``, so the bot's reply
in the channel is independent of the HTTP response Slack sees here.
"""

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

import structlog

from posthog.models.integration import SlackIntegration, SlackIntegrationError, validate_slack_request

from products.slack_app.backend.analytics import capture_slack_event
from products.slack_app.backend.api import (
    REQUIRED_SLACK_SCOPES,
    ROUTE_HANDLED_LOCALLY,
    ROUTE_NO_INTEGRATION,
    ROUTE_PROXY_FAILED,
    SLACK_INTEGRATION_KIND,
    _cross_region_routing_enabled,
    _is_us_host,
    _other_region_domain,
    _parse_rules_command,
    _resolve_region_or_terminal_route,
    _was_proxied,
)
from products.slack_app.backend.services.commands import dispatch_rules_command, resolve_command_target
from products.slack_app.backend.services.integration_resolver import (
    format_project_candidate_list,
    load_integrations,
    resolve_user_for_workspace,
)

logger = structlog.get_logger(__name__)

# Slash commands address the bot through a single namespaced entry point so the
# whole sub-command vocabulary lives behind one Slack manifest registration.
SLASH_COMMAND_NAME = "/posthog"


@csrf_exempt
def posthog_code_command_handler(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponse(status=405)

    try:
        slack_config = SlackIntegration.slack_config()
        validate_slack_request(request, slack_config["SLACK_APP_SIGNING_SECRET"])
    except SlackIntegrationError as e:
        logger.warning("slack_app_slash_command_invalid_request", error=str(e))
        return HttpResponse("Invalid request", status=403)

    payload = request.POST
    slack_team_id = payload.get("team_id", "")
    slack_user_id = payload.get("user_id", "")
    channel_id = payload.get("channel_id", "")
    raw_text = (payload.get("text") or "").strip()
    command_name = payload.get("command") or SLASH_COMMAND_NAME

    logger.info(
        "slack_app_slash_command_received",
        slack_team_id=slack_team_id,
        slack_user_id=slack_user_id,
        channel_id=channel_id,
        command=command_name,
        sub_command=raw_text.split()[0].lower() if raw_text else "",
    )

    if not slack_team_id or not slack_user_id:
        return _ephemeral_response("Missing Slack payload fields.")

    # Treat a bare ``/posthog`` as ``/posthog help`` — Slack convention is that
    # an argument-less invocation should at least explain what the command does.
    sub_command_text = raw_text or "help"
    parsed = _parse_rules_command(sub_command_text)
    if parsed is None:
        return _ephemeral_response(_unknown_command_help(command_name))

    incoming_host = request.get_host()
    proxied = _was_proxied(request)
    other_domain = _other_region_domain(incoming_host)
    can_defer = _cross_region_routing_enabled() and not _is_us_host(incoming_host) and not proxied

    workspace_result = load_integrations(
        slack_team_id=slack_team_id,
        kinds=[SLACK_INTEGRATION_KIND],
        slack_user_id=slack_user_id,
    )
    region_route = _resolve_region_or_terminal_route(
        request,
        slack_team_id,
        candidates_present=bool(workspace_result.candidates),
        kinds=[SLACK_INTEGRATION_KIND],
        proxied=proxied,
        other_domain=other_domain,
        incoming_host=incoming_host,
        can_defer=can_defer,
    )
    if region_route is not None:
        if region_route == ROUTE_NO_INTEGRATION:
            return _ephemeral_response(
                "This Slack workspace isn't connected to a PostHog organization. "
                "Connect it from a project's *Integrations* page first."
            )
        if region_route == ROUTE_PROXY_FAILED:
            return _ephemeral_response("Couldn't reach the PostHog backend — try again in a moment.")
        # Anything else (ROUTE_PROXIED) means the sibling region is handling
        # the work and will post the bot's reply itself. Ack Slack with 200.
        if region_route != ROUTE_HANDLED_LOCALLY:
            return HttpResponse(status=200)

    user_resolution = resolve_user_for_workspace(
        workspace_result=workspace_result,
        slack_team_id=slack_team_id,
        slack_user_id=slack_user_id,
    )
    if user_resolution.user is None:
        return _ephemeral_response(_user_resolution_failure_text(user_resolution.slack_email))

    workspace_candidates, target_resolution = resolve_command_target(
        slack_team_id=slack_team_id,
        command=parsed,
        slack_user_id=slack_user_id,
        user_id=user_resolution.user.id,
        channel=channel_id,
        thread_ts="",
    )
    integration = target_resolution.integration
    if integration is None:
        return _ephemeral_response(_pick_a_project_text(target_resolution.candidates, command_name))

    slack = SlackIntegration(integration)
    missing = slack.missing_scopes(REQUIRED_SLACK_SCOPES)
    if missing:
        return _ephemeral_response(
            "PostHog is missing Slack scopes: "
            f"`{', '.join(sorted(missing))}`. Reinstall the PostHog app from a project's "
            "*Integrations* page to grant them."
        )

    try:
        dispatch_rules_command(
            parsed,
            slack,
            integration,
            channel=channel_id,
            thread_ts="",
            slack_user_id=slack_user_id,
            slack_workspace_id=slack_team_id,
            user_id=user_resolution.user.id,
            workspace_candidates=workspace_candidates,
        )
        capture_slack_event(
            integration,
            "slack_app_slash_command",
            slack_user_id=slack_user_id,
            command=command_name,
            sub_command=parsed.action,
        )
    except Exception:
        logger.exception(
            "slack_app_slash_command_dispatch_failed",
            slack_team_id=slack_team_id,
            action=parsed.action,
        )
        return _ephemeral_response("Something went wrong — try again in a moment.")

    # Dispatch already posted the reply through the Slack client. Returning an
    # empty 200 avoids posting a duplicate message via the response body.
    return HttpResponse(status=200)


def _ephemeral_response(text: str) -> JsonResponse:
    return JsonResponse({"response_type": "ephemeral", "text": text})


def _unknown_command_help(command_name: str) -> str:
    return (
        "I didn't recognize that sub-command. Try one of:\n"
        f"• `{command_name} help`\n"
        f"• `{command_name} rules list`\n"
        f'• `{command_name} rules add "description" org/repo`\n'
        f"• `{command_name} rules remove <number(s)>`\n"
        f"• `{command_name} project [<id>]`"
    )


def _user_resolution_failure_text(slack_email: str | None) -> str:
    if slack_email:
        return (
            f"I couldn't find a PostHog account for `{slack_email}` in any organization connected "
            "to this Slack workspace. Link your account from *App Home → Settings* and try again."
        )
    return (
        "I couldn't find your PostHog account from your Slack identity. Link it from "
        "*App Home → Settings* and try again."
    )


def _pick_a_project_text(candidates: list, command_name: str) -> str:
    if not candidates:
        return "I couldn't find your PostHog account in any organization connected to this Slack workspace."
    return (
        "You haven't set a default project for this Slack workspace yet. Available PostHog projects:\n"
        f"{format_project_candidate_list(candidates)}\n\n"
        f"Set one with `{command_name} project <id>`."
    )
