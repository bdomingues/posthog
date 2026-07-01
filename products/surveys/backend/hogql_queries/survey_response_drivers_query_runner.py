import math
from datetime import UTC, datetime
from functools import cached_property
from typing import Any

from posthog.schema import (
    CachedSurveyResponseDriversQueryResponse,
    SurveyResponseDriversQuery,
    SurveyResponseDriversQueryResponse,
)

from posthog.hogql import ast
from posthog.hogql.constants import LimitContext
from posthog.hogql.parser import parse_select
from posthog.hogql.query import execute_hogql_query

from posthog.hogql_queries.insights.paginators import HogQLHasMorePaginator
from posthog.hogql_queries.query_runner import AnalyticsQueryRunner

from products.surveys.backend.models import Survey
from products.surveys.backend.responses.fetch_rows import resolve_question_metadata
from products.surveys.backend.util import SurveyEventName, get_archived_response_uuids

# Same statistical guards funnel correlation ships: the prior keeps every odds ratio
# finite (and mildly shrinks extreme ratios from tiny cells), the minimum-sample rule
# drops events too sparse to say anything about.
PRIOR_COUNT = 1
MIN_SAMPLE_COUNT = 25
MIN_SAMPLE_PERCENTAGE = 0.02
# Beyond a 10:1 promoter:detractor imbalance, odds ratios mostly reflect the imbalance.
SKEW_RATIO = 10
TOP_DRIVERS_PER_DIRECTION = 10

# NPS buckets: detractor 0-6, passive 7-8, promoter 9-10 (matches the frontend's
# NPS_PROMOTER_VALUES / NPS_PASSIVE_VALUES constants).
NPS_PROMOTER_MIN = 9
NPS_PASSIVE_MIN = 7
NPS_SCALE = 10

DEFAULT_DAYS_AROUND_RESPONSE = 30
# Only the most frequent events are scanned; hasMore=true means the ranking may be
# incomplete beyond them.
MAX_EVENTS_SCANNED = 300

RESPONSE_COLUMNS = ["event", "odds_ratio", "direction", "confidence", "population"]

# uniqueSurveySubmissionsFilter requires constant timestamp bounds, so open-ended
# surveys are bounded by a Python-side now() like the other survey response readers.
RESPONDERS_SELECT = """SELECT
        person_id,
        argMax(multiIf(score >= {promoter_min}, 'promoter', score >= {passive_min}, 'passive', 'detractor'), timestamp) AS bucket,
        max(timestamp) AS response_ts
    FROM (
        SELECT
            person_id,
            timestamp,
            toFloatOrNull(getSurveyResponse({q_idx}, {q_id})) AS score
        FROM events
        WHERE event = 'survey sent'
            AND properties.`$survey_id` = {survey_id}
            AND timestamp >= {date_from}
            AND timestamp <= {date_to}
            AND uniqueSurveySubmissionsFilter({survey_id}, {date_from}, {date_to})
            ARCHIVED_CLAUSE
    )
    WHERE score IS NOT NULL AND score >= 0 AND score <= {scale_max}
    GROUP BY person_id"""


class SurveyResponseDriversQueryRunner(AnalyticsQueryRunner[SurveyResponseDriversQueryResponse]):
    query: SurveyResponseDriversQuery
    cached_response: CachedSurveyResponseDriversQueryResponse
    paginator: HogQLHasMorePaginator

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.paginator = HogQLHasMorePaginator.from_limit_context(
            limit_context=LimitContext.QUERY,
            limit=MAX_EVENTS_SCANNED,
            offset=0,
        )

    @cached_property
    def survey(self) -> Survey:
        return Survey.objects.get(id=self.query.surveyId, team=self.team)

    @cached_property
    def question(self) -> dict[str, Any]:
        questions = resolve_question_metadata(self.survey)
        raw_by_index = {
            index: question for index, question in enumerate(self.survey.questions or []) if isinstance(question, dict)
        }

        if self.query.questionId is not None or self.query.questionIndex is not None:
            for question in questions:
                if self.query.questionId is not None:
                    if question["id"] != self.query.questionId:
                        continue
                elif question["index"] != self.query.questionIndex:
                    continue
                raw = raw_by_index.get(question["index"], {})
                if question["type"] != "rating" or raw.get("scale") != NPS_SCALE:
                    raise ValueError("Response drivers currently supports 0-10 rating (NPS) questions only")
                return question
            raise ValueError("Question not found on survey")

        # Default to the survey's first NPS question. A missing isNpsQuestion counts as
        # NPS when the scale is 10, matching the results UI's detection.
        for question in questions:
            raw = raw_by_index.get(question["index"], {})
            if question["type"] == "rating" and raw.get("scale") == NPS_SCALE and raw.get("isNpsQuestion") is not False:
                return question
        raise ValueError("Survey has no NPS (0-10 rating) question. Pass questionId to pick a question explicitly.")

    def _calculate(self) -> SurveyResponseDriversQueryResponse:
        with self.timings.measure("survey_response_drivers_totals_hogql_execute"):
            totals_result = execute_hogql_query(
                query=self._totals_query(),
                team=self.team,
                query_type="SurveyResponseDriversTotals",
                timings=self.timings,
                modifiers=self.modifiers,
            )

        total_detractors, total_promoters, total_passives = (
            totals_result.results[0] if totals_result.results else (0, 0, 0)
        )
        totals = {"promoters": total_promoters, "passives": total_passives, "detractors": total_detractors}
        threshold = min(MIN_SAMPLE_COUNT, MIN_SAMPLE_PERCENTAGE * (total_detractors + total_promoters))
        sample_threshold = math.ceil(threshold) if threshold > 0 else MIN_SAMPLE_COUNT

        # With an empty bucket there is no comparison group: report totals, flag skew,
        # and skip the drivers query rather than fabricating ratios against zero.
        if total_detractors == 0 or total_promoters == 0:
            return SurveyResponseDriversQueryResponse(
                columns=RESPONSE_COLUMNS,
                results=[],
                totals=totals,
                skewed=total_detractors != total_promoters,
                suppressedEvents=0,
                sampleThreshold=sample_threshold,
                questionId=self.question["id"],
                questionIndex=self.question["index"],
                modifiers=self.modifiers,
            )

        with self.timings.measure("survey_response_drivers_query_hogql_execute"):
            query_result = self.paginator.execute_hogql_query(
                query=self.to_query(),
                team=self.team,
                user=self.user,
                query_type="SurveyResponseDriversQuery",
                timings=self.timings,
                modifiers=self.modifiers,
                limit_context=self.limit_context,
            )

        results, suppressed_events = self._drivers(query_result.results, total_detractors, total_promoters, threshold)
        skewed = total_promoters / total_detractors > SKEW_RATIO or total_detractors / total_promoters > SKEW_RATIO

        return SurveyResponseDriversQueryResponse(
            columns=RESPONSE_COLUMNS,
            results=results,
            totals=totals,
            skewed=skewed,
            suppressedEvents=suppressed_events,
            sampleThreshold=sample_threshold,
            questionId=self.question["id"],
            questionIndex=self.question["index"],
            timings=query_result.timings,
            hogql=query_result.hogql,
            modifiers=self.modifiers,
            **self.paginator.response_params(),
        )

    def _drivers(
        self, rows: list[tuple[str, int, int]], total_detractors: int, total_promoters: int, threshold: float
    ) -> tuple[list[dict[str, Any]], int]:
        detractor_drivers: list[dict[str, Any]] = []
        promoter_drivers: list[dict[str, Any]] = []
        suppressed_events = 0

        for event, detractors_with, promoters_with in rows:
            sampled = detractors_with + promoters_with
            if sampled < threshold:
                suppressed_events += 1
                continue

            detractors_without = total_detractors - detractors_with
            promoters_without = total_promoters - promoters_with
            odds_ratio = ((detractors_with + PRIOR_COUNT) * (promoters_without + PRIOR_COUNT)) / (
                (detractors_without + PRIOR_COUNT) * (promoters_with + PRIOR_COUNT)
            )

            if odds_ratio == 1:
                continue

            driver = {
                "event": event,
                "odds_ratio": odds_ratio,
                "direction": "detractor" if odds_ratio > 1 else "promoter",
                "confidence": "high" if sampled >= MIN_SAMPLE_COUNT else "low",
                "population": {
                    "detractors_with": detractors_with,
                    "detractors_without": detractors_without,
                    "promoters_with": promoters_with,
                    "promoters_without": promoters_without,
                },
            }
            if odds_ratio > 1:
                detractor_drivers.append(driver)
            else:
                promoter_drivers.append(driver)

        detractor_drivers.sort(key=lambda driver: driver["odds_ratio"], reverse=True)
        promoter_drivers.sort(key=lambda driver: driver["odds_ratio"])

        results = detractor_drivers[:TOP_DRIVERS_PER_DIRECTION] + promoter_drivers[:TOP_DRIVERS_PER_DIRECTION]
        return results, suppressed_events

    def _days_around_response(self) -> int:
        days = self.query.daysAroundResponse
        if days is None:
            return DEFAULT_DAYS_AROUND_RESPONSE
        if days < 1:
            raise ValueError("daysAroundResponse must be at least 1")
        return days

    def _placeholders(self) -> tuple[dict[str, ast.Expr], str]:
        date_from = self.survey.start_date or self.survey.created_at
        date_to = self.survey.end_date or datetime.now(UTC)

        placeholders: dict[str, ast.Expr] = {
            "survey_id": ast.Constant(value=str(self.survey.id)),
            "q_idx": ast.Constant(value=self.question["index"]),
            "q_id": ast.Constant(value=self.question["id"]),
            "date_from": ast.Constant(value=date_from),
            "date_to": ast.Constant(value=date_to),
            "days": ast.Constant(value=self._days_around_response()),
            "promoter_min": ast.Constant(value=NPS_PROMOTER_MIN),
            "passive_min": ast.Constant(value=NPS_PASSIVE_MIN),
            "scale_max": ast.Constant(value=NPS_SCALE),
            "survey_events": ast.Constant(value=[event.value for event in SurveyEventName]),
        }

        archived_uuids = get_archived_response_uuids(str(self.survey.id), self.team.pk)
        archived_clause = ""
        if archived_uuids:
            placeholders["archived_uuids"] = ast.Tuple(
                exprs=[ast.Constant(value=uuid) for uuid in sorted(archived_uuids)]
            )
            archived_clause = "AND uuid NOT IN {archived_uuids}"

        return placeholders, archived_clause

    def _totals_query(self) -> ast.SelectQuery | ast.SelectSetQuery:
        placeholders, archived_clause = self._placeholders()
        template = (
            """SELECT
    countIf(bucket = 'detractor') AS total_detractors,
    countIf(bucket = 'promoter') AS total_promoters,
    countIf(bucket = 'passive') AS total_passives
FROM (
    """
            + RESPONDERS_SELECT.replace("ARCHIVED_CLAUSE", archived_clause)
            + """
)"""
        )
        return parse_select(template, placeholders=placeholders)

    def to_query(self) -> ast.SelectQuery | ast.SelectSetQuery:
        placeholders, archived_clause = self._placeholders()
        template = (
            """WITH responders AS (
    """
            + RESPONDERS_SELECT.replace("ARCHIVED_CLAUSE", archived_clause)
            + """
)
SELECT
    event,
    countIf(bucket = 'detractor') AS detractors_with,
    countIf(bucket = 'promoter') AS promoters_with
FROM (
    SELECT e.event AS event, e.person_id AS person_id, any(r.bucket) AS bucket
    FROM events AS e
    INNER JOIN responders AS r ON e.person_id = r.person_id
    WHERE e.timestamp >= {date_from} - toIntervalDay({days})
        AND e.timestamp <= {date_to} + toIntervalDay({days})
        AND e.timestamp >= r.response_ts - toIntervalDay({days})
        AND e.timestamp <= r.response_ts + toIntervalDay({days})
        AND e.event NOT IN {survey_events}
    GROUP BY e.event, e.person_id
)
GROUP BY event
ORDER BY detractors_with + promoters_with DESC"""
        )
        return parse_select(template, placeholders=placeholders)
