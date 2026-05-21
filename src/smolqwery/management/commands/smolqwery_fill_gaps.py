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
    """

    def handle(self, *args, **options):
        logging.root.setLevel(logging.WARNING)

        em = ExtractionManager(default_settings)
        something_new = False

        self.stdout.write(
            self.style.MIGRATE_HEADING("Filling Smolqwery gaps:") + "\n"
        )

        for table, date in em.fill_gaps():
            self.stdout.write(
                f"  {self.style.MIGRATE_LABEL(table)} - {date.isoformat()}\n"
            )
            something_new = True

        if not something_new:
            self.stdout.write("  No gaps found!\n")
