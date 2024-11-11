import datetime
from abc import ABC, abstractmethod
from dataclasses import MISSING, Field, asdict, dataclass, field, fields
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    Iterator,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Type,
    TypedDict,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from dateutil.relativedelta import relativedelta

from ._utils import (
    date_range,
    exclusive_date,
    json_dates,
    smart_split_name,
    to_snake,
    zero_date,
)
from .bigqwery import BigQwery

try:
    from django.utils.timezone import now
except ImportError:
    now = datetime.datetime.now

if TYPE_CHECKING:
    from .config import BaseConfigProvider


__all__ = [
    "sq_field",
    "D",
    "BaseExtractor",
    "ExtractionManager",
    "dataclass_to_schema",
    "Schema",
    "SchemaField",
    "ExtractorType",
]


# noinspection PyShadowingBuiltins
def sq_field(
    *,
    differentiate: bool = False,
    default=MISSING,
    default_factory=MISSING,
    init=True,
    repr=True,
    hash=None,
    compare=True,
) -> Field:
    """
    Shortcut to dataclass' field() method which allows to define the
    "differentiate" flag on the field. Setting this flag on a field requires
    a date-aggregated extractor and will trigger the creation of a view that
    computes the day-to-day delta.

    So let's say you have this data:

    - Day 1: 100
    - Day 2: 200
    - Day 3: 350

    You'll get the following data in the differentiated view:

    - Day 1: 100
    - Day 2: 100
    - Day 3: 150

    Notes
    -----
    It's just a wrapper. In truth the field() method is called and the
    differentiate metadata is placed using the standard metadata field. You can
    use the regular field() instead, it's just that it's much easier to do it
    with this shortcut.

    Parameters
    ----------
    differentiate
        Enable differentiation on this field
    default
        Default value (see Python doc)
    default_factory
        Default value factory (see Python doc)
    init
        Include in init (see Python doc)
    repr
        Include in repr (see Python doc)
    hash
        Include in hash (see Python doc)
    compare
        Include in comparison (see Python doc)
    """

    # noinspection PyArgumentList
    return field(
        default=default,
        default_factory=default_factory,
        init=init,
        repr=repr,
        hash=hash,
        compare=compare,
        metadata=dict(sq=dict(differentiate=differentiate)),
    )


D = TypeVar("D")


class SchemaField(TypedDict):
    """
    Field for BigQuery schema
    """

    name: str
    type: str
    nullable: bool


class Schema(TypedDict):
    """
    Schema information, used down the line by Coconut and so on to know how
    to generate the BigQuery schema
    """

    regular_fields: Sequence[SchemaField]
    delta_fields: Set[str]
    table_name: str


class ExtractorType(Enum):
    """
    Different types of extractors will have slightly different expectations.

    individual_rows
        Each row is individual, for example one email = one row. In that case
        it is expected to have a timestamp field which contains the date/time
        of the item. The name of the field can be changed by overriding the
        get_date_field() method of the extractor.
    date_aggregated
        There is exactly one row for each day. For example, the count of
        registered users. This avoids leaking into the Google database personal
        information by aggregating it. It also happens to improve the
        performance of the data studio.
    """

    individual_rows = "individual_rows"
    date_aggregated = "date_aggregated"


class BaseExtractor(ABC, Generic[D]):
    """
    In order to create an extractor, you need to:

    0. Have a look at ExtractorType to understand what extractor types are
    1. Create a dataclass that represents one row. If you are in
       date_aggregated mode, the date field will be added automatically
       (because it's not your responsibility to know the date you're
       extracting, you just receive lower/upper bounds but time zone makes it
       complicated). Otherwise don't forget to have a timestamp field. Be
       careful, once your exports started you cannot mutate the data model.
    2. Inherit from this class and implement the abstract methods into your
       code.
    3. Declare the reference to your extractor in the settings ("extractors"
       key, "SMOLQWERY_EXTRACTORS" in Django context).
    """

    def __init__(self, bq: BigQwery, settings: "BaseConfigProvider"):
        self.settings: "BaseConfigProvider" = settings
        self.bq = bq

    @abstractmethod
    def get_dataclass(self) -> Type[D]:
        """
        Return here the dataclass that your extract() method is going to be
        yielding.
        """

        raise NotImplementedError

    @abstractmethod
    def get_extractor_type(self) -> ExtractorType:
        """
        Return here the extractor type that you've chosen. See ExtractorType's
        documentation for guidance.
        """

        raise NotImplementedError

    @abstractmethod
    def extract(
        self, date_start: datetime.datetime, date_end: datetime.datetime
    ) -> Iterator[D]:
        """
        Implement here the actual extraction of data. You receive as an
        argument a start/end bounding dates (the start is inclusive while the
        end is exclusive) and you need to extract data that is true at those
        dates.

        A few examples:

        - If your return a users count, return the number of users that were
          created before the date_end (and mark the field as differentiated).
        - If you return a list of emails, return all emails sent at and after
          date_start but before date_end
        - And so forth

        The date_start and date_end are precise datetime objects because while
        events happen when they happen, the system will choose a timezone in
        which the start/end events will happen. As a result, some ranges might
        be more or less than 24h (because of DST) and other time-related
        weirdness. In a nutshell, trust the range :)

        The system will never:

        - Query overlapping ranges
        - Query the same range twice
        - Query future ranges

        ... unless:

        - You're in "individual_rows" mode and the previous time that range
          was called it returned nothing. That's not an issue since it won't
          return anything this time either (right?).
        - You've changed Django's time zone, which would be a massive fuck-up

        Parameters
        ----------
        date_start
            Inclusive date of the range start
        date_end
            Exclusive date of the range end
        """

        raise NotImplementedError

    def get_date_field(self) -> str:
        """
        We're sending exclusively time series. In the case of date-aggregated
        data the field is "date" while for unique events each of them must have
        a "timestamp" to know exactly when they happened.
        """

        if self.get_extractor_type() == ExtractorType.date_aggregated:
            return "date"
        else:
            return "timestamp"

    def get_table_name(self) -> str:
        """
        Automatically converts the name of the dataclass into a table name.
        You can override it if needed. A possible use case is if you mutated
        your data model and you don't want to rename everything in your code,
        you can simply change the name of the table here to trick the system
        into believing that this is a new table and that the other one has been
        deleted.
        """

        return to_snake(smart_split_name(self.get_dataclass().__name__))

    def get_schema(self) -> Schema:
        """
        Generates the schema from the dataclass. Not sure why you would want
        to override it because it would break the code down the line, but well
        here it is.
        """

        # noinspection PyTypeChecker
        return {
            **dataclass_to_schema(
                self.get_dataclass(),
                date_aggregated=(
                    self.get_extractor_type() == ExtractorType.date_aggregated
                ),
                required_fields={self.get_date_field()},
            ),
            "table_name": self.get_table_name(),
        }

    def get_latest_extract_date(self) -> datetime.date:
        """
        Digs into the table to find the most recent entry's date. This helps
        to know what ranges to extract.
        """

        job = self.bq.client.query(
            f"select `{self.get_date_field()}` as latest_date "
            f"from `{self.bq.get_table_id(self.get_table_name())}` "
            f"order by `{self.get_date_field()}` desc "
            f"limit 1"
        )

        if job.errors:
            raise Exception(
                f'Errors while getting latest extract date: {f"{job.errors}"[:1000]}'
            )

        latest_date = self.settings.first_date - relativedelta(days=1)

        for row in job.result():
            latest_date = row.latest_date

        if isinstance(latest_date, datetime.datetime):
            latest_date = latest_date.date()

        return latest_date


@dataclass(frozen=True)
class ExtractStep(Generic[D]):
    """
    Internal use, to know at which extraction step we are (mostly to help
    with reporting to the CLI interface).
    """

    extractor: BaseExtractor
    generator: Iterator[D]
    date: datetime.date


class ExtractInfo(NamedTuple):
    """
    Extraction progress report for CLI
    """

    table: str
    date: datetime.date


class ExtractionManager:
    """
    Probably your entry point if you want to extract the data, it does query
    stuff all around and triggers the insertion into BigQuery
    """

    def __init__(self, settings: "BaseConfigProvider"):
        self.settings: "BaseConfigProvider" = settings
        self._bq: Optional[BigQwery] = None

    @property
    def bq(self) -> BigQwery:
        """
        Cached BigQuery wrapper
        """

        if not self._bq:
            self._bq = BigQwery(settings=self.settings)

        return self._bq

    def extract_at_date(
        self,
        date: datetime.date,
        extractors: Optional[Sequence[Type[BaseExtractor]]] = None,
    ) -> Sequence[ExtractStep]:
        date_start = zero_date(date)
        date_end = exclusive_date(date)

        if extractors is None:
            extractors = self.settings.get_extractors()

        for e_class in extractors:
            e: BaseExtractor = e_class(self.bq, self.settings)
            yield ExtractStep(
                extractor=e,
                date=date,
                generator=self._generate_json(
                    extractor=e,
                    date=date,
                    generator=e.extract(date_start, date_end),
                ),
            )

    def _generate_json(
        self, extractor: BaseExtractor, date: datetime.date, generator: Iterator[object]
    ) -> Iterator[Dict]:
        """
        Transforms an iterator of dataclasses into an iterator of dicts that
        can be JSON-ified into the BigQuery tables.

        Notes
        -----
        If we're on a date-aggregated extractor, we'll automatically add the
        date to the JSON at this point. Otherwise it's up to the extractor to
        care about their own timestamp.

        Parameters
        ----------
        extractor
            The extractor we're reading from right now
        date
            Date that we're currently extracting
        generator
            Running generator of data
        """

        for row_obj in generator:
            # noinspection PyDataclass
            yield json_dates(
                {
                    **asdict(row_obj),
                    **(
                        {"date": date}
                        if extractor.get_extractor_type()
                        == ExtractorType.date_aggregated
                        else {}
                    ),
                }
            )

    def _extract_new(
        self, timestamp_now: Optional[datetime.datetime] = None
    ) -> Iterator[ExtractStep]:
        """
        Does all the plumbing of calling all the extractors on all the "fresh"
        dates that need to be extracted.

        Parameters
        ----------
        timestamp_now
            When is now? Defaults to the real now but could be another date if
            you don't have a time machine and need to run some tests.
        """

        if timestamp_now is None:
            timestamp_now = now()

        for extractor_class in self.settings.get_extractors():
            extractor = extractor_class(settings=self.settings, bq=self.bq)
            first_date = extractor.get_latest_extract_date()

            for date in date_range(first_date, timestamp_now):
                yield ExtractStep(
                    extractor=extractor,
                    generator=self._generate_json(
                        extractor=extractor,
                        date=date,
                        generator=extractor.extract(
                            zero_date(date), exclusive_date(date)
                        ),
                    ),
                    date=date,
                )

    def extract_new(
        self,
        timestamp_now: Optional[datetime.datetime] = None,
        use_upsert: bool = False,
    ) -> Iterator[ExtractInfo]:
        """
        The full process of extracting all the data which is new and pouring it
        into the BigQuery tables.

        Parameters
        ----------
        timestamp_now
            When is now? Defaults to the real now but could be another date if
            you don't have a time machine and need to run some tests.
        use_upsert
            Do you want to use upsert method, aka the sql's merge operation. It might be less performant but avoid
            duplicates.
        """
        for step in self._extract_new(timestamp_now):
            if use_upsert:
                self.bq.upsert(
                    table_name=step.extractor.get_table_name(),
                    rows=step.generator,
                )
            else:
                self.bq.insert_rows(
                    table_name=step.extractor.get_table_name(),
                    rows=step.generator,
                )
            yield ExtractInfo(table=step.extractor.get_table_name(), date=step.date)


class BigQueryType(NamedTuple):
    """
    Internal structure to help communicating about BigQuery row types
    """

    name: str
    nullable: bool


def python_to_bq_type(type_: Type) -> BigQueryType:
    """
    Guesses the BigQuery type of a Python type:

    - Numeric, strings, dates are supported (we use TIMESTAMP and not DATETIME
      because DATETIME doesn't store the time zone which is pure evil)
    - Some more advanced things like structs are not (for now?)
    - If the type is Optional it'll be marked as nullable and otherwise not

    Parameters
    ----------
    type_
        A Python type that can be mapped into BigQuery
    """

    nullable = False

    if get_origin(type_) is Union:
        null_types = []
        regular_types = []

        for x in get_args(type_):
            # noinspection PyUnresolvedReferences
            if not isinstance(x, None.__class__) and not issubclass(x, None.__class__):
                regular_types.append(x)
            else:
                null_types.append(x)

        if len(regular_types) != 1:
            raise Exception(f"Ambiguous type: {type_}")

        type_ = regular_types[0]
        nullable = bool(null_types)

    if issubclass(type_, int):
        return BigQueryType("INT64", nullable)
    elif issubclass(type_, float):
        return BigQueryType("FLOAT64", nullable)
    elif issubclass(type_, bool):
        return BigQueryType("BOOL", nullable)
    elif issubclass(type_, str):
        return BigQueryType("STRING", nullable)
    elif issubclass(type_, bytes):
        return BigQueryType("BYTES", nullable)
    elif issubclass(type_, datetime.date):
        return BigQueryType("DATE", nullable)
    elif issubclass(type_, datetime.datetime):
        return BigQueryType("TIMESTAMP", nullable)
    else:
        raise Exception(f"Unmappable BigQuery type: {type_}")


def dataclass_to_schema(type_: Type, date_aggregated: bool, required_fields: Set):
    """
    Transforms a Python dataclass into some meta-information that will help
    building a BigQuery schema.

    Parameters
    ----------
    type_
        Python type to transform into a schema
    date_aggregated
        Is the extractor of this dataclass date-aggregated or not?
    required_fields
        Extra set of fields that we want to see in the schema (typically
        "timestamp" in the case of a individual-rows extractor)
    """

    regular_fields = []
    used_names = set()

    if date_aggregated:
        used_names.add("date")
        regular_fields.append(dict(name="date", type="DATE", nullable=False))

    # noinspection PyDataclass
    for dc_field in fields(type_):
        type_name, nullable = python_to_bq_type(dc_field.type)

        if dc_field.name in used_names:
            raise Exception(f"Field name {dc_field.name} is used twice")

        used_names.add(dc_field.name)

        regular_fields.append(
            dict(
                name=dc_field.name,
                type=type_name,
                nullable=nullable,
            )
        )

    if not required_fields.issubset(used_names):
        raise Exception(
            f'Missing required fields in {type_.__name__}: {", ".join(required_fields - used_names)}'
        )

    delta_fields = set()

    # noinspection PyDataclass
    for dc_field in fields(type_):
        if dc_field.metadata.get("sq", {}).get("differentiate", False):
            if not issubclass(dc_field.type, (int, float)):
                raise Exception("Trying to differentiate a non-numeric field")

            delta_fields.add(dc_field.name)

            if not date_aggregated:
                raise Exception(
                    "Differentiated fields are not allowed on "
                    "non-date-aggregated extracts"
                )

    return dict(regular_fields=regular_fields, delta_fields=delta_fields)
