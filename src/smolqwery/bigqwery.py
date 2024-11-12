from datetime import datetime, timezone
from typing import Dict, Iterator, Optional
from unittest.mock import MagicMock

from google.cloud import bigquery
from google.oauth2 import service_account

from ._utils import ChunkIterator
from .config import BaseConfigProvider, default_settings


class MockClient:
    project = "mock"

    create_dataset = MagicMock()

    create_table = MagicMock()

    query = MagicMock()


class BigQwery:
    """
    Utility wrapper around Google's Big Query
    """

    INSERT_CHUNK_SIZE = 1000

    def __init__(self, settings: BaseConfigProvider = None):
        self.settings: BaseConfigProvider = settings if settings else default_settings
        self._client = None

    @property
    def client(self) -> bigquery.Client:
        """
        Generates the client from the credentials and returns/caches it
        """

        if not self._client:
            if getattr(self.settings, "google_mock", False):
                self._client = MockClient()
            else:
                self._client = bigquery.Client(credentials=self.get_credentials())

        return self._client

    def get_credentials(self):
        """
        Generates the credentials from config
        """

        return service_account.Credentials.from_service_account_info(
            self.settings.get_credentials_data()
        )

    def get_dataset_id(self, name: str = "") -> str:
        """
        Generates the ID of the dataset (the default one from settings by
        default, or the specified one instead).

        Parameters
        ----------
        name
            Optional name of the dataset (if not specified, the one from the
            settings will be used).
        """

        if not name:
            name = self.settings.dataset

        return f"{self.client.project}.{name}"

    def get_table_id(self, name: str, dataset_name: str = "") -> str:
        """
        Returns the ID of a table from its name

        Parameters
        ----------
        name
            Name of the table
        dataset_name
            Name of the dataset containing the table
        """

        return f"{self.get_dataset_id(dataset_name)}.{name}"

    def get_delta_view_id(self, table_name: str, dataset_name: str = "") -> str:
        """
        Computes the ID of a "delta view" based on a specific table

        Parameters
        ----------
        table_name
            Name of the table onto which the view is based
        dataset_name
            Name of the dataset holding the table
        """

        return f"{self.get_table_id(table_name, dataset_name)}_delta"

    def get_table(self, table_id: str, dataset_name: str) -> bigquery.Table:
        return self.client.get_table(
            self.get_table_id(table_id, dataset_name=dataset_name)
        )

    def upsert(
        self,
        table_name: str,
        rows: Iterator[Dict],
        extractor_type,
        dataset_name: str = "",
        insert_chunk_size: Optional[int] = None,
    ) -> None:
        """
        Upsert items that don't yet exist.
        To be considered a duplicate and not merged the row has to be completely identical, see the ON condition in the
        sql query.
        """

        if not rows:
            return

        try:
            temp_table_name = (
                f"{table_name}_temp_{int(datetime.now(timezone.utc).timestamp())}"
            )
            temp_table = bigquery.Table(
                self.get_table_id(temp_table_name, dataset_name)
            )
            temp_table.schema = self.get_table(table_name, dataset_name).schema
            columns_name = [field.name for field in temp_table.schema]
            self.client.create_table(temp_table)

            if insert_chunk_size is None:
                insert_chunk_size = self.INSERT_CHUNK_SIZE

            for chunk in ChunkIterator(rows).chunks(insert_chunk_size):
                errors = self.client.insert_rows_json(
                    self.get_table_id(temp_table_name, dataset_name), chunk
                )

                if errors:
                    errors_str = f"{errors}"[:1000]

                    raise Exception(f"Insertion error: {errors_str}")

            from .extractor import ExtractorType

            if extractor_type == ExtractorType.date_aggregated:
                on_condition = "target.date = source.date"
            elif extractor_type == ExtractorType.individual_rows:
                on_condition = " AND ".join(
                    [f"target.{col} = source.{col}" for col in columns_name]
                )
            else:
                raise Exception(f"Extractor type error")

            merge_sql = f"""
                MERGE `{self.get_table_id(table_name, dataset_name)}` AS target
                USING `{self.get_table_id(temp_table_name, dataset_name)}` AS source
                ON {on_condition}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ROW
            """
            self.client.query(merge_sql).result()
        finally:
            self.client.delete_table(temp_table)
