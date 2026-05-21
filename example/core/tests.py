import datetime
from unittest.mock import patch

from core.models import Contract, EmailMessage, User
from core.smolqwery import UserExtractor
from dateutil.relativedelta import relativedelta
from django.test import TransactionTestCase
from django.utils.dateparse import parse_datetime

from smolqwery import ExtractionManager, default_settings
from smolqwery._utils import date_range
from smolqwery.extractor import BaseExtractor


def flatten_steps(steps):
    for step in steps:
        yield from step.generator


class ExtractorTest(TransactionTestCase):
    def dataset_1(self) -> None:
        User.objects.bulk_create(
            [
                User(
                    date_create=parse_datetime("2022-01-01T00:42:00+0100"),
                    personal_info="a",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T00:00:00+0100"),
                    personal_info="b",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T13:00:00+0100"),
                    personal_info="c",
                ),
                User(
                    date_create=parse_datetime("2022-01-02T14:42:00+0100"),
                    personal_info="d",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T08:09:10+0100"),
                    personal_info="e",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T09:08:07+0100"),
                    personal_info="f",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T23:59:59.999+0100"),
                    personal_info="g",
                ),
                User(
                    date_create=parse_datetime("2022-01-04T11:11:11+0100"),
                    personal_info="h",
                ),
                User(
                    date_create=parse_datetime("2022-01-05T17:02:42+0100"),
                    personal_info="i",
                ),
            ]
        )

        Contract.objects.bulk_create(
            [
                Contract(
                    date_create=parse_datetime("2022-01-01T00:42:10+0100"),
                    date_validate=None,
                    state=Contract.State.created,
                    user=User.objects.get(personal_info="a"),
                    more_personal_info="1",
                    value=10,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-02T14:00:00+0100"),
                    date_validate=parse_datetime("2022-01-04T14:00:00+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="c"),
                    more_personal_info="2",
                    value=20,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-02T15:42:00+0100"),
                    date_validate=parse_datetime("2022-01-03T15:49:00+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="d"),
                    more_personal_info="2",
                    value=20,
                ),
                Contract(
                    date_create=parse_datetime("2022-01-04T00:42:41+0100"),
                    date_validate=parse_datetime("2022-01-05T18:40:12+0100"),
                    state=Contract.State.validated,
                    user=User.objects.get(personal_info="g"),
                    more_personal_info="2",
                    value=30,
                ),
            ]
        )

        for user in User.objects.all():
            EmailMessage.objects.create(
                user=user,
                date_sent=user.date_create + relativedelta(seconds=1),
                type=EmailMessage.Type.registration,
                content="welcome",
            )

        for contract in Contract.objects.all():
            EmailMessage.objects.create(
                user=contract.user,
                date_sent=contract.date_create + relativedelta(seconds=1),
                type=EmailMessage.Type.contract_created,
                content="contract created",
            )

            if contract.date_validate:
                EmailMessage.objects.create(
                    user=contract.user,
                    date_sent=contract.date_create + relativedelta(seconds=1),
                    type=EmailMessage.Type.contract_validated,
                    content="contract validated",
                )

    def setUp(self) -> None:
        self.dataset_1()

    def test_date_range(self):
        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01T03:40:14+0100"),
                    parse_datetime("2022-01-04T00:00:00+0100"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01T03:40:14+0100"),
                    parse_datetime("2022-01-04T08:00:00+0100"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01"),
                    parse_datetime("2022-01-04"),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

        self.assertEqual(
            [
                *date_range(
                    parse_datetime("2022-01-01").date(),
                    parse_datetime("2022-01-04").date(),
                )
            ],
            [
                parse_datetime("2022-01-02").date(),
                parse_datetime("2022-01-03").date(),
            ],
        )

    def test_email_extractor(self):
        em = ExtractionManager(default_settings)

        e = em.extract_at_date(parse_datetime("2022-01-01").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 1, "prospects": 1, "clients": 0, "date": "2022-01-01"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-02").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 4, "prospects": 3, "clients": 0, "date": "2022-01-02"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-03").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 7, "prospects": 3, "clients": 1, "date": "2022-01-03"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-04").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 8, "prospects": 4, "clients": 2, "date": "2022-01-04"}], data
        )

        e = em.extract_at_date(parse_datetime("2022-01-05").date(), [UserExtractor])
        data = [*flatten_steps(e)]
        self.assertEqual(
            [{"users": 9, "prospects": 4, "clients": 3, "date": "2022-01-05"}], data
        )


class FillGapsTest(TransactionTestCase):
    """
    Tests for ExtractionManager.fill_gaps().

    BigQuery interactions are replaced by mocks so that we can exercise the
    gap-detection and batching logic without a real BigQuery connection.
    """

    def setUp(self):
        User.objects.bulk_create(
            [
                User(
                    date_create=parse_datetime("2022-01-01T12:00:00+0100"),
                    personal_info="a",
                ),
                User(
                    date_create=parse_datetime("2022-01-03T12:00:00+0100"),
                    personal_info="b",
                ),
                User(
                    date_create=parse_datetime("2022-01-05T12:00:00+0100"),
                    personal_info="c",
                ),
            ]
        )

    def test_fill_gaps_fills_missing_dates(self):
        """
        When BQ already holds data for Jan 1 and Jan 5, fill_gaps() should
        extract Jan 2, 3, 4 and batch them into a single upsert per extractor.
        """
        em = ExtractionManager(default_settings)

        existing = {datetime.date(2022, 1, 1), datetime.date(2022, 1, 5)}
        upsert_calls = []

        def capture_upsert(table_name, rows, extractor_type, **kwargs):
            upsert_calls.append((table_name, list(rows)))

        with patch.object(BaseExtractor, "get_existing_dates", return_value=existing):
            with patch.object(em.bq, "upsert", side_effect=capture_upsert):
                results = list(
                    em.fill_gaps(
                        timestamp_now=parse_datetime("2022-01-06T00:00:00+0100")
                    )
                )

        result_by_table = {}
        for info in results:
            result_by_table.setdefault(info.table, []).append(info.date)

        expected_missing = [
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 3),
            datetime.date(2022, 1, 4),
        ]

        # Both extractors should report the three missing dates
        self.assertEqual(sorted(result_by_table["user"]), expected_missing)
        self.assertEqual(sorted(result_by_table["email"]), expected_missing)

        # Each extractor must produce exactly ONE upsert call (not one per date)
        self.assertEqual(len(upsert_calls), 2)

        # Verify that the user rows span all three missing dates
        user_rows = next(rows for name, rows in upsert_calls if name == "user")
        row_dates = {row["date"] for row in user_rows}
        for d in expected_missing:
            self.assertIn(d.isoformat(), row_dates)

    def test_fill_gaps_no_gaps(self):
        """
        When all expected dates are already present in BQ, fill_gaps() should
        produce no output and make no upsert calls.
        """
        em = ExtractionManager(default_settings)

        # Every date from Jan 1 to Jan 5 is present
        existing = {datetime.date(2022, 1, d) for d in range(1, 6)}

        with patch.object(BaseExtractor, "get_existing_dates", return_value=existing):
            with patch.object(em.bq, "upsert") as mock_upsert:
                results = list(
                    em.fill_gaps(
                        timestamp_now=parse_datetime("2022-01-06T00:00:00+0100")
                    )
                )

        self.assertEqual(results, [])
        mock_upsert.assert_not_called()

    def test_fill_gaps_deadline_stops_early(self):
        """
        When a deadline in the past is passed, fill_gaps() should stop before
        processing any dates and make no upsert calls, so that data already
        committed on a previous (partial) run is not overwritten.
        """
        em = ExtractionManager(default_settings)

        existing = {datetime.date(2022, 1, 1), datetime.date(2022, 1, 5)}
        # A deadline already in the past means the very first date check fires.
        past_deadline = parse_datetime("2022-01-01T00:00:00+0100")

        with patch.object(BaseExtractor, "get_existing_dates", return_value=existing):
            with patch.object(em.bq, "upsert") as mock_upsert:
                results = list(
                    em.fill_gaps(
                        timestamp_now=parse_datetime("2022-01-06T00:00:00+0100"),
                        deadline=past_deadline,
                    )
                )

        self.assertEqual(results, [])
        mock_upsert.assert_not_called()

    def test_fill_gaps_deadline_per_date_upserts(self):
        """
        When a deadline is provided, fill_gaps() must upsert one date at a
        time (not a single batch per extractor) so that partial progress
        survives a timeout.
        """
        em = ExtractionManager(default_settings)

        existing = {datetime.date(2022, 1, 1), datetime.date(2022, 1, 5)}
        upsert_calls = []

        def capture_upsert(table_name, rows, extractor_type, **kwargs):
            upsert_calls.append((table_name, list(rows)))

        # A deadline far in the future so all dates are processed.
        future_deadline = parse_datetime("2099-01-01T00:00:00+0000")

        with patch.object(BaseExtractor, "get_existing_dates", return_value=existing):
            with patch.object(em.bq, "upsert", side_effect=capture_upsert):
                results = list(
                    em.fill_gaps(
                        timestamp_now=parse_datetime("2022-01-06T00:00:00+0100"),
                        deadline=future_deadline,
                    )
                )

        expected_missing = [
            datetime.date(2022, 1, 2),
            datetime.date(2022, 1, 3),
            datetime.date(2022, 1, 4),
        ]

        result_by_table = {}
        for info in results:
            result_by_table.setdefault(info.table, []).append(info.date)

        self.assertEqual(sorted(result_by_table["user"]), expected_missing)
        self.assertEqual(sorted(result_by_table["email"]), expected_missing)

        # With a deadline, each date must get its own upsert (one per
        # extractor × date, not one per extractor).
        self.assertEqual(len(upsert_calls), len(expected_missing) * 2)

