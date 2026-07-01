"""Slack slash command entry point.

Handles ``POST /slack/command-callback`` — the webhook Slack hits when a user
runs ``/posthog ...`` in a channel or DM. The vocabulary mirrors the
``@PostHog <command>`` mention path (``help``, ``rules ...``, ``project ...``);
free-text task creation stays on the mention path because slash commands lack
the thread context the task workflow depends on.

Slack imposes a hard 3-second response budget on slash commands — a slow first
response surfaces as ``/posthog … failed with the error 'operation_timeout'``
to the user, even when the work eventually succeeds. Cheap validation (signature,
sub-command parsing, workspace lookup, region routing) stays inline; the slower
steps (Slack ``users.info``, membership queries, ``chat_postMessage``) run on a
background thread that ack's Slack with an immediate 200 and posts errors back
via the slash command's ``response_url`` (which works without bot channel
membership).
"""

import threading

from django.db import close_old_connections
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

import requests
import structlog

from posthog.models.integration import SlackIntegration, SlackIntegrationError, validate_slack_request

from products.slack_app.backend.analytics import capture_slack_event
from products.slack_app.backend.api import (
    REQUIRED_SLACK_SCOPES,
    ROUTE_NO_INTEGRATION,
    ROUTE_PROXY_FAILED,
    SLACK_INTEGRATION_KIND,
    RulesCommand,
    cross_region_routing_enabled,
    is_us_host,
    other_region_domain,
    parse_rules_command,
    resolve_region_or_terminal_route,
    was_proxied,
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

# Slack's response_url accepts POSTs for up to 30 minutes / 5 replies per invocation.
# 5 seconds is generous for the acks we send (a single ephemeral text payload) and
# short enough that a stalled Slack edge doesn't leak the worker thread indefinitely.
_RESPONSE_URL_TIMEOUT_SECONDS = 5


@csrf_exempt
def slack_app_command_handler(request: HttpRequest) -> HttpResponse:
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
    # ``thread_ts`` is only present when the slash command was invoked from
    # inside a thread; passing it through keeps the bot's reply in-thread
    # instead of dropping it at the bottom of the channel.
    thread_ts = payload.get("thread_ts", "") or ""
    raw_text = (payload.get("text") or "").strip()
    command_name = payload.get("command") or SLASH_COMMAND_NAME
    response_url = payload.get("response_url", "")

    if not slack_team_id or not slack_user_id:
        return _ephemeral_response("Missing Slack payload fields.")

    # Treat a bare ``/posthog`` as ``/posthog help`` — Slack convention is that
    # an argument-less invocation should at least explain what the command does.
    sub_command_text = raw_text or "help"
    parsed = parse_rules_command(sub_command_text)

    logger.info(
        "slack_app_slash_command_received",
        slack_team_id=slack_team_id,
        slack_user_id=slack_user_id,
        channel_id=channel_id,
        command=command_name,
        # ``""`` distinguishes bare ``/posthog`` from explicit ``/posthog <unknown>``
        # in analytics; the parser maps both to a ``help`` action downstream.
        sub_command=parsed.action if parsed is not None else "" if not raw_text else "unknown",
    )
    if parsed is None:
        return _ephemeral_response(_unknown_command_help(command_name))

    # Region routing is cheap (single DB query + a short cross-region probe that
    # is disabled outside Cloud US/EU) so it stays inline — the sync response
    # gives the invoker immediate feedback when the workspace isn't connected.
    incoming_host = request.get_host()
    proxied = was_proxied(request)
    other_domain = other_region_domain(incoming_host)
    can_defer = cross_region_routing_enabled() and not is_us_host(incoming_host) and not proxied

    workspace_result = load_integrations(
        slack_team_id=slack_team_id,
        kinds=[SLACK_INTEGRATION_KIND],
        slack_user_id=slack_user_id,
    )
    region_route = resolve_region_or_terminal_route(
        request,
        slack_team_id,
        candidates_present=bool(workspace_result.candidates),
        kinds=[SLACK_INTEGRATION_KIND],
        proxied=proxied,
        other_domain=other_domain,
        incoming_host=incoming_host,
        can_defer=can_defer,
    )
    # resolve_region_or_terminal_route returns ``None`` when the caller should
    # keep handling locally, or one of ROUTE_NO_INTEGRATION / ROUTE_PROXY_FAILED
    # / ROUTE_PROXIED on terminal exits.
    if region_route == ROUTE_NO_INTEGRATION:
        return _ephemeral_response(
            "This Slack workspace isn't connected to a PostHog organization. "
            "Connect it from a project's *Integrations* page first."
        )
    if region_route == ROUTE_PROXY_FAILED:
        return _ephemeral_response("Couldn't reach the PostHog backend — try again in a moment.")
    if region_route is not None:
        # ROUTE_PROXIED: the sibling region already accepted the forwarded payload
        # and will post the bot's reply through its own Slack client. Ack with 200.
        return HttpResponse(status=200)

    # From here on the work involves a Slack ``users.info`` lookup (possibly cold),
    # a membership query, and a ``chat_postMessage`` — comfortably over 3 seconds
    # on a cold path. Hand off to a background thread and ack Slack immediately so
    # the user never sees ``operation_timeout``.
    threading.Thread(
        target=_run_command_async,
        kwargs={
            "workspace_candidates": workspace_result.candidates,
            "parsed": parsed,
            "slack_team_id": slack_team_id,
            "slack_user_id": slack_user_id,
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "command_name": command_name,
            "response_url": response_url,
        },
        daemon=True,
        name=f"slack-cmd-{parsed.action}",
    ).start()
    return HttpResponse(status=200)


def _run_command_async(
    *,
    workspace_candidates: list,
    parsed: RulesCommand,
    slack_team_id: str,
    slack_user_id: str,
    channel_id: str,
    thread_ts: str,
    command_name: str,
    response_url: str,
) -> None:
    """Off-request worker: run user resolution + dispatch. Errors go back to
    Slack via ``response_url`` (works without bot channel membership); the
    dispatcher's happy-path ``chat_postMessage`` still lands in-channel."""

    # Django checks out a connection lazily per thread; releasing it (before and
    # after) keeps this fire-and-forget worker from hoarding pooled connections.
    close_old_connections()
    try:
        from products.slack_app.backend.services.integration_resolver import ResolutionResult

        workspace_result = ResolutionResult(integration=None, source="needs_picker", candidates=workspace_candidates)
        user_resolution = resolve_user_for_workspace(
            workspace_result=workspace_result,
            slack_team_id=slack_team_id,
            slack_user_id=slack_user_id,
        )
        if user_resolution.user is None:
            _post_deferred_ephemeral(response_url, _user_resolution_failure_text(user_resolution.slack_email))
            return

        resolved_candidates, target_resolution = resolve_command_target(
            slack_team_id=slack_team_id,
            command=parsed,
            slack_user_id=slack_user_id,
            user_id=user_resolution.user.id,
            channel=channel_id,
            thread_ts=thread_ts,
        )
        integration = target_resolution.integration
        if integration is None:
            _post_deferred_ephemeral(
                response_url,
                _pick_a_project_text(target_resolution.candidates, command_name),
            )
            return

        slack = SlackIntegration(integration)
        missing = slack.missing_scopes(REQUIRED_SLACK_SCOPES)
        if missing:
            _post_deferred_ephemeral(
                response_url,
                (
                    "PostHog is missing Slack scopes: "
                    f"`{', '.join(sorted(missing))}`. Reinstall the PostHog app from a project's "
                    "*Integrations* page to grant them."
                ),
            )
            return

        try:
            dispatch_rules_command(
                parsed,
                slack,
                integration,
                channel=channel_id,
                thread_ts=thread_ts,
                slack_user_id=slack_user_id,
                slack_workspace_id=slack_team_id,
                user_id=user_resolution.user.id,
                workspace_candidates=resolved_candidates,
                command_prefix=command_name,
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
            _post_deferred_ephemeral(response_url, "Something went wrong — try again in a moment.")
    except Exception:
        logger.exception(
            "slack_app_slash_command_worker_crashed",
            slack_team_id=slack_team_id,
            action=getattr(parsed, "action", None),
        )
        _post_deferred_ephemeral(response_url, "Something went wrong — try again in a moment.")
    finally:
        close_old_connections()


def _ephemeral_response(text: str) -> JsonResponse:
    return JsonResponse({"response_type": "ephemeral", "text": text})


def _post_deferred_ephemeral(response_url: str, text: str) -> None:
    """Slack's ``response_url`` accepts a normal ephemeral-shaped JSON payload
    and delivers it back to the invoking user without needing bot channel
    membership. Best-effort — a delivery failure is logged but never crashes
    the worker."""
    if not response_url:
        return
    try:
        requests.post(
            response_url,
            json={"response_type": "ephemeral", "text": text},
            timeout=_RESPONSE_URL_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        logger.warning("slack_app_slash_command_response_url_post_failed", exc_info=True)


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
