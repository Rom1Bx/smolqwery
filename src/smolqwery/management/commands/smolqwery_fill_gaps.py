import json
import logging

from django.core.management import BaseCommand

from smolqwery import ExtractionManager
from smolqwery.config import default_settings


class Command(BaseCommand):
    """
    Detects dates that have missing data in BigQuery (within the range from
    SMOLQWERY_FIRST_DATE to yesterday) and fills those gaps by running the
    extraction for each missing date.

    All missing dates for a given extractor are batched into a single
    BigQuery upsert call to minimise the number of BigQuery operations.

    Pass --dry-run to print the rows that would be upserted instead of
    pushing them to BigQuery.
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

    def handle(self, *args, **options):
        logging.root.setLevel(logging.WARNING)

        dry_run = options["dry_run"]
        em = ExtractionManager(default_settings)
        something_new = False

        heading = "Smolqwery gap detection (dry-run):" if dry_run else "Filling Smolqwery gaps:"
        self.stdout.write(self.style.MIGRATE_HEADING(heading) + "\n")

        for info in em.fill_gaps(dry_run=dry_run):
            self.stdout.write(
                f"  {self.style.MIGRATE_LABEL(info.table)} - {info.date.isoformat()}\n"
            )
            if dry_run and info.rows:
                for row in info.rows:
                    self.stdout.write(f"    {json.dumps(row, default=str)}\n")
            something_new = True

        if not something_new:
            self.stdout.write("  No gaps found!\n")
