from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from posthog.schema import (
    AssistantFunnelsQuery,
    AssistantHogQLQuery,
    AssistantLifecycleQuery,
    AssistantPathsQuery,
    AssistantRetentionQuery,
    AssistantStickinessQuery,
    AssistantTrendsQuery,
    ChartDisplayType,
    DataTableNode,
    DataVisualizationNode,
    FunnelsQuery,
    HogQLQuery,
    InsightVizNode,
    LifecycleQuery,
    PathsQuery,
    RetentionQuery,
    RevenueAnalyticsGrossRevenueQuery,
    RevenueAnalyticsMetricsQuery,
    RevenueAnalyticsMRRQuery,
    RevenueAnalyticsTopCustomersQuery,
    StickinessQuery,
    TrendsQuery,
)

from .boxplot import BoxPlotResultsFormatter
from .funnel import FunnelResultsFormatter
from .lifecycle import LifecycleResultsFormatter
from .paths import PathsResultsFormatter
from .retention import RetentionResultsFormatter
from .revenue_analytics import (
    RevenueAnalyticsGrossRevenueResultsFormatter,
    RevenueAnalyticsMetricsResultsFormatter,
    RevenueAnalyticsMRRResultsFormatter,
    RevenueAnalyticsTopCustomersResultsFormatter,
)
from .sql import TRUNCATED_MARKER, SQLResultsFormatter
from .stickiness import StickinessResultsFormatter
from .trends import TrendsResultsFormatter

if TYPE_CHECKING:
    from posthog.models import Team


def is_boxplot_query(query: BaseModel) -> bool:
    trends_filter = getattr(query, "trendsFilter", None)
    return trends_filter is not None and getattr(trends_filter, "display", None) == ChartDisplayType.BOX_PLOT


def get_boxplot_results(response: dict[str, Any]) -> list[Any]:
    # TODO: remove boxplot_data fallback once cached responses have rotated (added 2026-04-17)
    results = response.get("results", [])
    return results if results else response.get("boxplot_data", [])


def _warning_field(warning: Any, field: str) -> Any:
    return warning.get(field) if isinstance(warning, dict) else getattr(warning, field, None)


def _format_warning_block(warnings: list[Any], header: str) -> str:
    lines = [header]
    for warning in warnings:
        message = _warning_field(warning, "message")
        if message:
            lines.append(f"- {message}")
    if len(lines) == 1:  # header only, no messages
        return ""
    lines.append("")
    return "\n".join(lines)


def format_warehouse_sync_warnings(response: dict[str, Any]) -> str:
    """Render data warehouse sync warnings from the shared `warnings` field as a leading block.

    Returns empty string when no sync warnings are present.
    """
    # The shared field also carries access control warnings; pick out the sync ones by shape.
    warnings = [w for w in (response.get("warnings") or []) if _warning_field(w, "table_name") is not None]
    return _format_warning_block(
        warnings, "[Data warehouse sync warnings — results may not reflect current source data]"
    )


def format_access_control_warnings(response: dict[str, Any]) -> str:
    """Render object-level access control warnings from the shared `warnings` field as a leading block.

    Filtering is pushed into SQL, so excluded rows never come back. Without this block an agent
    can mistake an access-filtered partial result for the full set. Returns empty string when
    nothing was excluded.
    """
    warnings = [w for w in (response.get("warnings") or []) if _warning_field(w, "resource") is not None]
    return _format_warning_block(
        warnings, "[Access control — this is a partial result set; rows you don't have access to were excluded]"
    )


def format_query_results_for_llm(
    query: BaseModel,
    response: dict[str, Any],
    team: "Team",
    utc_now: datetime | None = None,
) -> str | None:
    """
    Format query results into LLM-friendly text.

    This is a synchronous function that dispatches to the appropriate formatter
    based on query type. Returns None if the query type is not supported.
    """
    if utc_now is None:
        utc_now = datetime.now(UTC)

    # Saved insights store their query wrapped in a presentation envelope (`InsightVizNode` for
    # product-analytics insights, `DataVisualizationNode` / `DataTableNode` for SQL-backed ones).
    # The dispatcher below matches on the underlying query type, so unwrap the `source` first.
    if isinstance(query, InsightVizNode | DataVisualizationNode | DataTableNode):
        query = query.source

    formatted: str | None = None
    if isinstance(query, AssistantTrendsQuery | TrendsQuery):
        if is_boxplot_query(query):
            formatted = BoxPlotResultsFormatter(get_boxplot_results(response)).format()
        else:
            formatted = TrendsResultsFormatter(query, response["results"]).format()
    elif isinstance(query, AssistantFunnelsQuery | FunnelsQuery):
        formatted = FunnelResultsFormatter(query, response["results"], team, utc_now).format()
    elif isinstance(query, AssistantLifecycleQuery | LifecycleQuery):
        formatted = LifecycleResultsFormatter(query, response["results"]).format()
    elif isinstance(query, AssistantPathsQuery | PathsQuery):
        formatted = PathsResultsFormatter(response["results"]).format()
    elif isinstance(query, AssistantStickinessQuery | StickinessQuery):
        formatted = StickinessResultsFormatter(query, response["results"]).format()
    elif isinstance(query, AssistantRetentionQuery | RetentionQuery):
        formatted = RetentionResultsFormatter(query, response["results"]).format()
    elif isinstance(query, AssistantHogQLQuery | HogQLQuery):
        formatted = SQLResultsFormatter(query, response["results"], response["columns"]).format()
    elif isinstance(query, RevenueAnalyticsGrossRevenueQuery):
        formatted = RevenueAnalyticsGrossRevenueResultsFormatter(query, response["results"]).format()
    elif isinstance(query, RevenueAnalyticsMetricsQuery):
        formatted = RevenueAnalyticsMetricsResultsFormatter(query, response["results"]).format()
    elif isinstance(query, RevenueAnalyticsMRRQuery):
        formatted = RevenueAnalyticsMRRResultsFormatter(query, response["results"]).format()
    elif isinstance(query, RevenueAnalyticsTopCustomersQuery):
        formatted = RevenueAnalyticsTopCustomersResultsFormatter(query, response["results"]).format()

    if formatted is None:
        return None
    warning_prefix = format_warehouse_sync_warnings(response) + format_access_control_warnings(response)
    return warning_prefix + formatted if warning_prefix else formatted


__all__ = [
    "BoxPlotResultsFormatter",
    "FunnelResultsFormatter",
    "LifecycleResultsFormatter",
    "PathsResultsFormatter",
    "RetentionResultsFormatter",
    "SQLResultsFormatter",
    "StickinessResultsFormatter",
    "TrendsResultsFormatter",
    "RevenueAnalyticsGrossRevenueResultsFormatter",
    "RevenueAnalyticsMetricsResultsFormatter",
    "RevenueAnalyticsMRRResultsFormatter",
    "RevenueAnalyticsTopCustomersResultsFormatter",
    "TRUNCATED_MARKER",
    "format_access_control_warnings",
    "format_query_results_for_llm",
    "format_warehouse_sync_warnings",
]
