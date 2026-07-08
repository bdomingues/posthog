from typing import Any

from freezegun import freeze_time
from posthog.test.base import (
    APIBaseTest,
    ClickhouseTestMixin,
    _create_event,
    _create_person,
    flush_persons_and_events,
    snapshot_clickhouse_queries,
)

from parameterized import parameterized
from rest_framework.exceptions import ValidationError

from posthog.schema import ActorsQuery, SurveyResponseDriversActorsQuery, SurveyResponseDriversQuery

from posthog.hogql.query import execute_hogql_query

from posthog.hogql_queries.query_runner import get_query_runner
from posthog.session_recordings.queries.test.session_replay_sql import produce_replay_summary

from products.surveys.backend.hogql_queries.survey_response_drivers_query_runner import SurveyResponseDriversQueryRunner
from products.surveys.backend.models import Survey, SurveyResponseArchive

NPS_QUESTION = {
    "id": "q-nps",
    "type": "rating",
    "question": "How likely are you to recommend us?",
    "scale": 10,
    "display": "number",
    "isNpsQuestion": True,
}


class TestSurveyResponseDriversQueryRunner(ClickhouseTestMixin, APIBaseTest):
    # Pinned ids: snapshotted SQL embeds the survey id and archived event uuids, so they
    # must be deterministic across runs (the snapshot normalizer does not redact them).
    SURVEY_ID = "01234567-89ab-cdef-0123-456789abcdef"
    ARCHIVED_UUID = "11111111-1111-4111-8111-111111111111"
    ARCHIVED_RESUBMIT_UUID = "22222222-2222-4222-8222-222222222222"

    def _create_survey(self, questions: list[dict[str, Any]] | None = None) -> Survey:
        return Survey.objects.create(
            id=self.SURVEY_ID,
            team=self.team,
            name="NPS survey",
            type="popover",
            questions=[NPS_QUESTION] if questions is None else questions,
            start_date="2024-01-01T00:00:00Z",
        )

    def _seed_responder(
        self,
        survey: Survey,
        distinct_id: str,
        score: int,
        events: tuple[str, ...] = (),
        submission_id: str | None = None,
        response_timestamp: str = "2024-01-10T10:00:00Z",
        event_uuid: str | None = None,
        session_id: str | None = None,
    ) -> str:
        _create_person(distinct_ids=[distinct_id], team_id=self.team.pk)
        response_uuid = _create_event(
            team=self.team,
            event="survey sent",
            distinct_id=distinct_id,
            timestamp=response_timestamp,
            event_uuid=event_uuid,
            properties={
                "$survey_id": str(survey.id),
                "$survey_response": str(score),
                "$survey_submission_id": submission_id or f"{distinct_id}-submission",
                "$survey_completed": True,
            },
        )
        for event in events:
            _create_event(
                team=self.team,
                event=event,
                distinct_id=distinct_id,
                timestamp="2024-01-12T10:00:00Z",
                properties={"$session_id": session_id, "$window_id": "w1"} if session_id else {},
            )
        return response_uuid

    def _calculate(self, survey: Survey, **kwargs: Any) -> dict[str, Any]:
        query = SurveyResponseDriversQuery(kind="SurveyResponseDriversQuery", surveyId=str(survey.id), **kwargs)
        return SurveyResponseDriversQueryRunner(team=self.team, query=query).calculate().model_dump()

    def _drivers_by_event(self, response: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {driver["event"]: driver for driver in response["results"]}

    @freeze_time("2024-01-15T12:00:00Z")
    @snapshot_clickhouse_queries
    def test_response_shape(self) -> None:
        survey = self._create_survey()
        response = self._calculate(survey)
        assert response["columns"] == ["event", "odds_ratio", "direction", "confidence", "population"]
        assert response["results"] == []
        assert response["totals"] == {"promoters": 0, "passives": 0, "detractors": 0}
        assert response["questionId"] == "q-nps"
        assert response["questionIndex"] == 0

    @freeze_time("2024-01-15T12:00:00Z")
    @snapshot_clickhouse_queries
    def test_detects_planted_drivers_with_high_confidence(self) -> None:
        survey = self._create_survey()
        for i in range(30):
            self._seed_responder(survey, f"detractor_{i}", score=2, events=("csv import failed", "browsed docs"))
        for i in range(30):
            self._seed_responder(survey, f"promoter_{i}", score=10, events=("saved view used", "browsed docs"))
        flush_persons_and_events()

        response = self._calculate(survey)
        drivers = self._drivers_by_event(response)

        assert response["totals"] == {"promoters": 30, "passives": 0, "detractors": 30}
        assert response["skewed"] is False

        import_failed = drivers["csv import failed"]
        assert import_failed["direction"] == "detractor"
        assert import_failed["odds_ratio"] > 1
        assert import_failed["confidence"] == "high"
        assert import_failed["population"] == {
            "detractors_with": 30,
            "detractors_without": 0,
            "promoters_with": 0,
            "promoters_without": 30,
        }

        saved_view = drivers["saved view used"]
        assert saved_view["direction"] == "promoter"
        assert saved_view["odds_ratio"] < 1
        assert saved_view["confidence"] == "high"

        # An event both buckets perform equally distinguishes neither and is dropped.
        assert "browsed docs" not in drivers

    @freeze_time("2024-01-15T12:00:00Z")
    def test_suppresses_events_below_sample_threshold(self) -> None:
        survey = self._create_survey()
        self._seed_responder(survey, "detractor_rare", score=1, events=("rare event",))
        for i in range(40):
            self._seed_responder(survey, f"detractor_{i}", score=2, events=("common event",))
        for i in range(40):
            self._seed_responder(survey, f"promoter_{i}", score=10, events=("common event",))
        flush_persons_and_events()

        response = self._calculate(survey)
        drivers = self._drivers_by_event(response)

        assert "rare event" not in drivers
        assert response["suppressedEvents"] >= 1
        assert response["sampleThreshold"] >= 2

    @freeze_time("2024-01-15T12:00:00Z")
    def test_flags_low_confidence_below_min_sample_count(self) -> None:
        survey = self._create_survey()
        for i in range(5):
            self._seed_responder(survey, f"detractor_{i}", score=0, events=("niche failure",))
        for i in range(95):
            self._seed_responder(survey, f"promoter_{i}", score=9, events=())
        flush_persons_and_events()

        response = self._calculate(survey)
        drivers = self._drivers_by_event(response)

        niche = drivers["niche failure"]
        assert niche["direction"] == "detractor"
        assert niche["confidence"] == "low"

    @freeze_time("2024-01-15T12:00:00Z")
    def test_counts_resubmitting_responder_once(self) -> None:
        survey = self._create_survey()
        self._seed_responder(survey, "resubmitter", score=2, submission_id="sub-1")
        _create_event(
            team=self.team,
            event="survey sent",
            distinct_id="resubmitter",
            timestamp="2024-01-10T11:00:00Z",
            properties={
                "$survey_id": str(survey.id),
                "$survey_response": "2",
                "$survey_submission_id": "sub-1",
                "$survey_completed": True,
            },
        )
        self._seed_responder(survey, "promoter_1", score=10)
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["totals"]["detractors"] == 1
        assert response["totals"]["promoters"] == 1

    @freeze_time("2024-01-15T12:00:00Z")
    def test_archiving_canonical_resubmission_excludes_responder(self) -> None:
        # The submission dedupe keeps only argMax(uuid, timestamp) per submission id, so
        # archiving that canonical event must exclude the responder even though an earlier
        # duplicate 'survey sent' row still exists.
        survey = self._create_survey()
        self._seed_responder(survey, "resubmitter", score=2, submission_id="sub-1")
        canonical_uuid = _create_event(
            team=self.team,
            event="survey sent",
            distinct_id="resubmitter",
            timestamp="2024-01-10T11:00:00Z",
            properties={
                "$survey_id": str(survey.id),
                "$survey_response": "2",
                "$survey_submission_id": "sub-1",
                "$survey_completed": True,
            },
        )
        SurveyResponseArchive.objects.create(team=self.team, survey=survey, response_uuid=canonical_uuid)
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["totals"] == {"promoters": 0, "passives": 0, "detractors": 0}

    @freeze_time("2024-01-15T12:00:00Z")
    @snapshot_clickhouse_queries
    def test_excludes_archived_responses(self) -> None:
        survey = self._create_survey()
        archived_uuid = self._seed_responder(
            survey, "archived_detractor", score=0, events=("some event",), event_uuid=self.ARCHIVED_UUID
        )
        self._seed_responder(survey, "kept_detractor", score=1, events=("some event",))
        self._seed_responder(survey, "kept_promoter", score=10, events=("some event",))
        SurveyResponseArchive.objects.create(team=self.team, survey=survey, response_uuid=archived_uuid)
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["totals"]["detractors"] == 1
        assert response["totals"]["promoters"] == 1

    @freeze_time("2024-01-15T12:00:00Z")
    def test_reports_totals_when_responders_have_no_other_events(self) -> None:
        survey = self._create_survey()
        for i in range(3):
            self._seed_responder(survey, f"detractor_{i}", score=2)
        for i in range(3):
            self._seed_responder(survey, f"promoter_{i}", score=10)
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["totals"] == {"promoters": 3, "passives": 0, "detractors": 3}
        assert response["results"] == []

    @freeze_time("2024-01-15T12:00:00Z")
    def test_respects_days_around_response_window(self) -> None:
        survey = self._create_survey()
        for i in range(30):
            distinct_id = f"detractor_{i}"
            self._seed_responder(survey, distinct_id, score=2)
            _create_event(
                team=self.team,
                event="near event",
                distinct_id=distinct_id,
                timestamp="2024-01-10T18:00:00Z",
                properties={},
            )
            _create_event(
                team=self.team,
                event="far event",
                distinct_id=distinct_id,
                timestamp="2024-01-14T10:00:00Z",
                properties={},
            )
        for i in range(30):
            self._seed_responder(survey, f"promoter_{i}", score=10)
        flush_persons_and_events()

        response = self._calculate(survey, daysAroundResponse=1)
        drivers = self._drivers_by_event(response)

        assert "near event" in drivers
        assert "far event" not in drivers

    @freeze_time("2024-01-15T12:00:00Z")
    def test_flags_skewed_totals(self) -> None:
        survey = self._create_survey()
        for i in range(2):
            self._seed_responder(survey, f"detractor_{i}", score=3, events=("event a",))
        for i in range(30):
            self._seed_responder(survey, f"promoter_{i}", score=10, events=("event a",))
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["skewed"] is True

    @freeze_time("2024-01-15T12:00:00Z")
    def test_empty_bucket_returns_no_drivers_and_skew(self) -> None:
        survey = self._create_survey()
        for i in range(10):
            self._seed_responder(survey, f"promoter_{i}", score=10, events=("event a",))
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["results"] == []
        assert response["skewed"] is True
        assert response["totals"]["promoters"] == 10

    @parameterized.expand(
        [
            (0, "detractors"),
            (6, "detractors"),
            (7, "passives"),
            (8, "passives"),
            (9, "promoters"),
            (10, "promoters"),
        ]
    )
    @freeze_time("2024-01-15T12:00:00Z")
    def test_nps_bucket_boundaries(self, score: int, expected_bucket: str) -> None:
        survey = self._create_survey()
        self._seed_responder(survey, "responder", score=score, events=("event a",))
        flush_persons_and_events()

        response = self._calculate(survey)

        assert response["totals"][expected_bucket] == 1
        assert sum(response["totals"].values()) == 1

    @freeze_time("2024-01-15T12:00:00Z")
    def test_requires_nps_question(self) -> None:
        survey = self._create_survey(questions=[{"id": "q-open", "type": "open", "question": "Any feedback?"}])
        with self.assertRaises(ValidationError):
            self._calculate(survey)

    @freeze_time("2024-01-15T12:00:00Z")
    def test_rejects_explicitly_selected_non_nps_question(self) -> None:
        survey = self._create_survey(
            questions=[
                {"id": "q-open", "type": "open", "question": "Any feedback?"},
                {"id": "q-csat", "type": "rating", "question": "How satisfied are you?", "scale": 5},
                NPS_QUESTION,
            ]
        )
        with self.assertRaises(ValidationError):
            self._calculate(survey, questionId="q-open")
        with self.assertRaises(ValidationError):
            self._calculate(survey, questionId="q-csat")

    @freeze_time("2024-01-15T12:00:00Z")
    def test_rejects_invalid_days_around_response(self) -> None:
        survey = self._create_survey()
        with self.assertRaises(ValidationError):
            self._calculate(survey, daysAroundResponse=0)

    @freeze_time("2024-01-15T12:00:00Z")
    def test_explicit_question_selection(self) -> None:
        survey = self._create_survey(
            questions=[
                {"id": "q-open", "type": "open", "question": "Any feedback?"},
                NPS_QUESTION | {"id": "q-nps-2"},
            ]
        )
        response = self._calculate(survey, questionId="q-nps-2")
        assert response["questionId"] == "q-nps-2"
        assert response["questionIndex"] == 1

    def _seed_actors_fixture(self) -> Survey:
        survey = self._create_survey()
        for i in range(3):
            self._seed_responder(survey, f"detractor_with_{i}", score=2, events=("csv import failed",))
        for i in range(2):
            self._seed_responder(survey, f"detractor_without_{i}", score=1)
        for i in range(2):
            self._seed_responder(survey, f"promoter_with_{i}", score=10, events=("csv import failed",))
        for i in range(5):
            self._seed_responder(survey, f"promoter_without_{i}", score=9)

        # Archived performer: must be invisible to every cell.
        archived_uuid = self._seed_responder(
            survey, "archived_detractor", score=0, events=("csv import failed",), event_uuid=self.ARCHIVED_UUID
        )
        SurveyResponseArchive.objects.create(team=self.team, survey=survey, response_uuid=archived_uuid)

        # Passive performer: passives are excluded from every cell of the 2x2.
        self._seed_responder(survey, "passive_with", score=8, events=("csv import failed",))

        # Resubmitter: first submission scores as promoter, final resubmission as
        # detractor — must count once, as a detractor performer.
        self._seed_responder(survey, "resubmitter", score=10, events=("csv import failed",), submission_id="sub-actors")
        _create_event(
            team=self.team,
            event="survey sent",
            distinct_id="resubmitter",
            timestamp="2024-01-10T11:00:00Z",
            properties={
                "$survey_id": str(survey.id),
                "$survey_response": "2",
                "$survey_submission_id": "sub-actors",
                "$survey_completed": True,
            },
        )

        # Archived-canonical resubmitter: the dedupe filter keeps only the latest event
        # per submission id, and that canonical event is archived — so this performer
        # must be excluded from every cell, even though the earlier duplicate remains.
        self._seed_responder(
            survey, "archived_resubmitter", score=2, events=("csv import failed",), submission_id="sub-arch"
        )
        archived_resubmit_uuid = _create_event(
            team=self.team,
            event="survey sent",
            distinct_id="archived_resubmitter",
            timestamp="2024-01-10T11:00:00Z",
            event_uuid=self.ARCHIVED_RESUBMIT_UUID,
            properties={
                "$survey_id": str(survey.id),
                "$survey_response": "2",
                "$survey_submission_id": "sub-arch",
                "$survey_completed": True,
            },
        )
        SurveyResponseArchive.objects.create(team=self.team, survey=survey, response_uuid=archived_resubmit_uuid)

        # Boundary responder: only performs the event outside the ±30d window, so they
        # belong to the detractor did-not cell.
        self._seed_responder(survey, "boundary_detractor", score=3)
        _create_event(
            team=self.team,
            event="csv import failed",
            distinct_id="boundary_detractor",
            timestamp="2023-12-05T10:00:00Z",
            properties={},
        )
        flush_persons_and_events()
        return survey

    def _actor_count(self, survey: Survey, event: str, bucket: str, performed: bool) -> int:
        runner = SurveyResponseDriversQueryRunner(
            team=self.team,
            query=SurveyResponseDriversQuery(kind="SurveyResponseDriversQuery", surveyId=str(survey.id)),
        )
        result = execute_hogql_query(
            query=runner.to_actors_query(target_event=event, bucket=bucket, performed=performed),
            team=self.team,
        )
        return len(result.results)

    @parameterized.expand(
        [
            ("detractor", True, 4),
            ("detractor", False, 3),
            ("promoter", True, 2),
            ("promoter", False, 5),
        ]
    )
    @freeze_time("2024-01-15T12:00:00Z")
    def test_actors_match_population_cells(self, bucket: str, performed: bool, expected: int) -> None:
        survey = self._seed_actors_fixture()

        response = self._calculate(survey)
        population = self._drivers_by_event(response)["csv import failed"]["population"]
        cell = population[f"{bucket}s_with" if performed else f"{bucket}s_without"]

        assert cell == expected
        assert self._actor_count(survey, "csv import failed", bucket, performed) == cell

    @freeze_time("2024-01-15T12:00:00Z")
    @snapshot_clickhouse_queries
    def test_actors_query_through_actors_query_runner(self) -> None:
        survey = self._seed_actors_fixture()

        actors_query = ActorsQuery(
            kind="ActorsQuery",
            source=SurveyResponseDriversActorsQuery(
                kind="SurveyResponseDriversActorsQuery",
                source=SurveyResponseDriversQuery(kind="SurveyResponseDriversQuery", surveyId=str(survey.id)),
                event="csv import failed",
                bucket="detractor",
                performed=True,
            ),
            select=["actor"],
        )
        response = get_query_runner(actors_query, self.team).calculate()

        assert len(response.results) == 4
        distinct_ids = {distinct_id for row in response.results for distinct_id in row[0]["distinct_ids"]}
        assert distinct_ids == {"detractor_with_0", "detractor_with_1", "detractor_with_2", "resubmitter"}

    @freeze_time("2024-01-15T12:00:00Z")
    def test_actors_query_rejects_invalid_bucket(self) -> None:
        survey = self._create_survey()
        runner = SurveyResponseDriversQueryRunner(
            team=self.team,
            query=SurveyResponseDriversQuery(kind="SurveyResponseDriversQuery", surveyId=str(survey.id)),
        )
        with self.assertRaises(ValueError):
            runner.to_actors_query(target_event="csv import failed", bucket="passive", performed=True)

    @freeze_time("2024-01-15T12:00:00Z")
    @snapshot_clickhouse_queries
    def test_actors_recordings_enrich_rows_without_changing_membership(self) -> None:
        survey = self._create_survey()
        self._seed_responder(
            survey, "rec_detractor", score=2, events=("csv import failed",), session_id="drivers-rec-1"
        )
        self._seed_responder(survey, "norec_detractor", score=1, events=("csv import failed",))
        produce_replay_summary(
            team_id=self.team.pk,
            session_id="drivers-rec-1",
            distinct_id="rec_detractor",
            first_timestamp="2024-01-12T09:55:00Z",
            last_timestamp="2024-01-12T10:05:00Z",
            ensure_analytics_event_in_session=False,
        )
        flush_persons_and_events()

        actors_query = ActorsQuery(
            kind="ActorsQuery",
            source=SurveyResponseDriversActorsQuery(
                kind="SurveyResponseDriversActorsQuery",
                source=SurveyResponseDriversQuery(kind="SurveyResponseDriversQuery", surveyId=str(survey.id)),
                event="csv import failed",
                bucket="detractor",
                performed=True,
                includeRecordings=True,
            ),
            select=["actor", "matched_recordings"],
        )
        response = get_query_runner(actors_query, self.team).calculate()

        recordings_by_distinct_id = {
            distinct_id: row[1] for row in response.results for distinct_id in row[0]["distinct_ids"]
        }
        assert len(response.results) == 2
        assert [recording["session_id"] for recording in recordings_by_distinct_id["rec_detractor"]] == [
            "drivers-rec-1"
        ]
        assert recordings_by_distinct_id["rec_detractor"][0]["events"]
        assert recordings_by_distinct_id["norec_detractor"] == []
