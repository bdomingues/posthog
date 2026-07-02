import uuid
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from posthog.models.event.util import create_event
from posthog.models.person.util import create_person, create_person_distinct_id, get_person_by_distinct_id
from posthog.models.team.team import Team
from posthog.models.utils import UUIDT
from posthog.personhog_client.caller_tag import personhog_caller_tag
from posthog.persons_db import persons_db_connection
from posthog.persons_seed import insert_seed_distinct_id, insert_seed_person

from products.surveys.backend.models import Survey

# Per-bucket probability that a responder performed each behavioral event. The planted
# correlations give the Drivers tab a clear story: detractors hit failures, promoters use
# power features, everyone views dashboards (odds ratio ~1, dropped), and one event is
# performed by too few people to clear the sample guard (shown as suppressed).
BEHAVIOR_RATES: dict[str, dict[str, float]] = {
    "csv import failed": {"detractor": 0.65, "passive": 0.25, "promoter": 0.08},
    "support ticket opened": {"detractor": 0.35, "passive": 0.20, "promoter": 0.10},
    "saved view used": {"detractor": 0.10, "passive": 0.30, "promoter": 0.60},
    "weekly report shared": {"detractor": 0.08, "passive": 0.20, "promoter": 0.40},
    "dashboard viewed": {"detractor": 0.75, "passive": 0.75, "promoter": 0.75},
}

RARE_EVENT = "beta feature toggled"
RARE_EVENT_COUNT = 2

# Planted below the sample guard entirely — min(25, 2% of scored responders) is ~2 at the
# default seed size, so a single performer gets suppressed and the table's footer note
# has something honest to report.
SUPPRESSED_EVENT = "legacy exporter used"
SUPPRESSED_EVENT_COUNT = 1

BUCKET_WEIGHTS = [("detractor", 0.4), ("passive", 0.2), ("promoter", 0.4)]

BUCKET_SCORES = {
    "detractor": (0, 6),
    "passive": (7, 8),
    "promoter": (9, 10),
}


class Command(BaseCommand):
    help = "Seed an NPS survey with responses and planted behavioral correlations for the response drivers tab"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--team-id", type=int, required=True, help="Team to seed the survey into")
        parser.add_argument("--responders", type=int, default=120, help="Number of survey responders to create")
        parser.add_argument("--days-back", type=int, default=21, help="Survey launch date, this many days ago")
        parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible data")

    def handle(self, *args: Any, **options: Any) -> None:
        rng = random.Random(options["seed"])
        team = Team.objects.get(id=options["team_id"])
        now = datetime.now(UTC)
        start_date = now - timedelta(days=options["days_back"])

        question_id = str(uuid.uuid4())
        survey = Survey.objects.create(
            team=team,
            name="NPS survey (response drivers demo)",
            description="Seeded demo data with planted behavioral correlations",
            type="popover",
            questions=[
                {
                    "id": question_id,
                    "type": "rating",
                    "question": "How likely are you to recommend us to a friend or colleague?",
                    "scale": 10,
                    "display": "number",
                    "isNpsQuestion": True,
                    "lowerBoundLabel": "Unlikely",
                    "upperBoundLabel": "Very likely",
                }
            ],
            created_by=team.organization.members.first(),
            start_date=start_date,
        )

        def create_seed_person(distinct_id: str, properties: dict[str, Any]) -> str:
            # The existence check needs the personhog service; minimal dev setups may not
            # run it, and the seeded distinct ids are namespaced, so treat lookup failure
            # as "person does not exist yet".
            try:
                with personhog_caller_tag("surveys/seed-response-drivers"):
                    existing_person = get_person_by_distinct_id(
                        team_id=team.id, distinct_id=distinct_id, distinct_id_limit=0
                    )
            except RuntimeError:
                existing_person = None
            if existing_person:
                return str(existing_person.uuid)
            person_uuid = str(UUIDT())
            with persons_db_connection(writer=True) as conn:
                person_id = insert_seed_person(
                    conn, team_id=team.id, properties=properties, is_identified=True, uuid=person_uuid
                )
                insert_seed_distinct_id(conn, team_id=team.id, person_id=person_id, distinct_id=distinct_id)
            create_person(team_id=team.id, uuid=person_uuid, version=0, is_identified=True, properties=properties)
            create_person_distinct_id(team_id=team.id, distinct_id=distinct_id, person_id=person_uuid)
            return person_uuid

        bucket_counts = {"detractor": 0, "passive": 0, "promoter": 0}
        rare_event_budget = RARE_EVENT_COUNT
        suppressed_event_budget = SUPPRESSED_EVENT_COUNT

        for i in range(options["responders"]):
            bucket = rng.choices(
                [name for name, _ in BUCKET_WEIGHTS], weights=[weight for _, weight in BUCKET_WEIGHTS]
            )[0]
            bucket_counts[bucket] += 1
            low, high = BUCKET_SCORES[bucket]
            score = rng.randint(low, high)

            distinct_id = f"drivers_demo_user_{i}"
            person_properties = {"email": f"drivers_demo_{i}@example.com", "seeded": True}
            person_uuid = create_seed_person(distinct_id, person_properties)

            response_ts = start_date + timedelta(
                days=rng.uniform(0.5, options["days_back"] - 1), minutes=rng.randint(0, 720)
            )
            create_event(
                event_uuid=uuid.uuid4(),
                event="survey sent",
                team=team,
                distinct_id=distinct_id,
                timestamp=response_ts,
                person_id=uuid.UUID(person_uuid),
                person_properties=person_properties,
                properties={
                    "$survey_id": str(survey.id),
                    "$survey_response": str(score),
                    f"$survey_response_{question_id}": str(score),
                    "$survey_submission_id": str(uuid.uuid4()),
                    "$survey_completed": True,
                },
            )

            performed = [event for event, rates in BEHAVIOR_RATES.items() if rng.random() < rates[bucket]]
            if rare_event_budget > 0 and bucket == "detractor" and rng.random() < 0.15:
                performed.append(RARE_EVENT)
                rare_event_budget -= 1
            if suppressed_event_budget > 0 and bucket == "detractor" and rng.random() < 0.1:
                performed.append(SUPPRESSED_EVENT)
                suppressed_event_budget -= 1
            for event in performed:
                event_ts = response_ts + timedelta(days=rng.uniform(-8, 8))
                create_event(
                    event_uuid=uuid.uuid4(),
                    event=event,
                    team=team,
                    distinct_id=distinct_id,
                    timestamp=max(event_ts, start_date),
                    person_id=uuid.UUID(person_uuid),
                    person_properties=person_properties,
                    properties={"seeded": True},
                )

        self.stdout.write(self.style.SUCCESS(f"Seeded survey {survey.id} ({survey.name})"))
        self.stdout.write(
            f"Responders: {bucket_counts['detractor']} detractors, {bucket_counts['passive']} passives, "
            f"{bucket_counts['promoter']} promoters"
        )
        self.stdout.write(f"Open /surveys/{survey.id}?tab=drivers with the survey-response-drivers flag enabled")
