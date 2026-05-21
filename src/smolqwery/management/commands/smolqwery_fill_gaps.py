import datetime
import json
import logging

from django.core.management import BaseCommand
from django.utils.timezone import now

from smolqwery import ExtractionManager
from smolqwery.config import default_settings


class Command(BaseCommand):
    """
    Detects dates that have missing data in BigQuery (within the range from
    SMOLQWERY_FIRST_DATE to yesterday) and fills those gaps by running the
    extraction for each missing date.

    By default all missing dates for a given extractor are batched into a
    single BigQuery upsert call to minimise the number of BigQuery operations.

    Pass --dry-run to print the rows that would be upserted instead of
    pushing them to BigQuery.

    Pass --max-duration <seconds> to set a wall-clock budget. The command
    will stop processing new dates once the budget is exhausted. Any date
    already committed to BigQuery is safe: re-running the command will skip
    those dates and continue from where it left off.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help=(
                "Print extracted rows to stdout instead of pushing them to "
                "BigQuery. No data is written."
            ),
        )
        parser.add_argument(
            "--max-duration",
            type=float,
            default=None,
            metavar="SECONDS",
            help=(
                "Stop processing new dates after this many seconds. Data "
                "already committed to BigQuery is preserved; re-run the "
                "command to continue filling the remaining gaps."
            ),
        )

    def handle(self, *args, **options):
        logging.root.setLevel(logging.WARNING)

        dry_run = options["dry_run"]
        max_duration = options["max_duration"]

        deadline = None
        if max_duration is not None:
            deadline = now() + datetime.timedelta(seconds=max_duration)

        em = ExtractionManager(default_settings)
        something_new = False

        heading = "Smolqwery gap detection (dry-run):" if dry_run else "Filling Smolqwery gaps:"
        self.stdout.write(self.style.MIGRATE_HEADING(heading) + "\n")

        for info in em.fill_gaps(dry_run=dry_run, deadline=deadline):
            self.stdout.write(
                f"  {self.style.MIGRATE_LABEL(info.table)} - {info.date.isoformat()}\n"
            )
            if dry_run and info.rows:
                for row in info.rows:
                    self.stdout.write(f"    {json.dumps(row, default=str)}\n")
            something_new = True

        if not something_new:
            self.stdout.write("  No gaps found!\n")
