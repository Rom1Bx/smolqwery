import logging

from django.core.management import BaseCommand

from smolqwery import ExtractionManager
from smolqwery.config import default_settings


class Command(BaseCommand):
    """
    Inserts into the BigQuery tables all the missing data (aka all the data
    between last export and the last revolute day).
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--upsert",
            action="store_true",
            default=False,
            help="Use upsert method for data insertion",
        )

    def handle(self, *args, **options):
        logging.root.setLevel(logging.WARNING)

        use_upsert = options.get("upsert", False)

        em = ExtractionManager(default_settings)
        something_new = False

        self.stdout.write(
            self.style.MIGRATE_HEADING(f"Making new Smolqwery extracts:") + "\n"
        )

        for table, date in em.extract_new(use_upsert=use_upsert):
            self.stdout.write(
                f"  {self.style.MIGRATE_LABEL(table)} - {date.isoformat()}\n"
            )
            something_new = True

        if not something_new:
            self.stdout.write("  Nothing new!\n")
