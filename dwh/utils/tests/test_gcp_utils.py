import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils.gcp_config import CONFIG   

test_config = {
    'dataset' : 'test',
    'table_exists' : 'table_exists',
    'table_new' : 'table_new',
}
class TestBQInit(unittest.TestCase):

    @patch("utils.gcp_utils.bigquery.Client")
    @patch("os.environ", {})
    def test_init_sets_credentials_env(self, mock_client):
        mock_client.return_value.project = CONFIG["project-id"]  
        mock_client.return_value.dataset = MagicMock()

        with patch("os.path.join", return_value=CONFIG["sa-credentials"]  ):  
            from utils.gcp_utils import BQ
            bq = BQ(test_config['dataset'], test_config["table_exists"])  

        import os
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == CONFIG["sa-credentials"]  

    @patch("utils.gcp_utils.bigquery.Client")
    def test_init_creates_dataset_ref(self, mock_client):
        mock_client.return_value.project = CONFIG["project-id"]  

        with patch("os.path.join", return_value=CONFIG["sa-credentials"]  ):  
            from utils.gcp_utils import BQ
            bq = BQ("my_dataset", "prod")  # TODO: replace with your dataset and config name

        assert bq.dataset_ref.dataset_id == "my_dataset"  # TODO: replace with your dataset name
        assert bq.dataset_ref.project == CONFIG["project-id"]  


class TestTableIfNotExist(unittest.TestCase):

    def _make_bq(self, mock_client):
        mock_client.return_value.project = CONFIG["project-id"]  
        with patch("os.path.join", return_value=CONFIG["sa-credentials"]  ): 
            from utils.gcp_utils import BQ
            return BQ("my_dataset", "prod")  

    @patch("utils.gcp_utils.bigquery.Client")
    def test_returns_false_when_table_exists(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_client.return_value.get_table.return_value = MagicMock()

        result = bq.tableIfNotExist("my_table") 

        assert result is False

    @patch("utils.gcp_utils.bigquery.Client")
    def test_returns_true_when_table_not_found(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_client.return_value.get_table.side_effect = NotFound("table not found")

        result = bq.tableIfNotExist("my_table")  # TODO: replace with your table name

        assert result is True


class TestTableCreate(unittest.TestCase):

    def _make_bq(self, mock_client):
        mock_client.return_value.project = CONFIG["project-id"]  # TODO: replace with your GCP project ID
        with patch("os.path.join", return_value=CONFIG["sa-credentials"]  ):  # TODO: replace with your credentials file path
            from utils.gcp_utils import BQ
            return BQ("my_dataset", "prod")  # TODO: replace with your dataset and config name

    @patch("utils.gcp_utils.bigquery.Client")
    def test_creates_table_with_partition(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_table = MagicMock()
        mock_table.table_id = "my_table"  # TODO: replace with your table name
        mock_client.return_value.create_table.return_value = mock_table

        schema = [bigquery.SchemaField("event_date", "DATE")]  # TODO: replace with your actual table schema
        result = bq.tableCreate(schema, "my_table", "event_date")  # TODO: replace table name and partition field

        mock_client.return_value.create_table.assert_called_once()
        assert result is True

    @patch("utils.gcp_utils.bigquery.Client")
    def test_table_created_with_day_partitioning(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_table = MagicMock()
        mock_table.table_id = "my_table"  # TODO: replace with your table name
        mock_client.return_value.create_table.return_value = mock_table

        schema = [bigquery.SchemaField("event_date", "DATE")]  # TODO: replace with your actual table schema
        bq.tableCreate(schema, "my_table", "event_date")  # TODO: replace table name and partition field

        created_table = mock_client.return_value.create_table.call_args[0][0]
        assert created_table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
        assert created_table.time_partitioning.field == "event_date"  # TODO: replace with your partition field


class TestDataIfExist(unittest.TestCase):

    def _make_bq(self, mock_client):
        mock_client.return_value.project = "test-project"  # TODO: replace with your GCP project ID
        with patch("os.path.join", return_value="/fake/path/prod.json"):  # TODO: replace with your credentials file path
            from utils.gcp_utils import BQ
            return BQ("my_dataset", "prod")  # TODO: replace with your dataset and config name

    @patch("utils.gcp_utils.bigquery.Client")
    def test_returns_true_when_data_exists(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=True)
        mock_client.return_value.query.return_value.result.return_value = [mock_row]

        result = bq.dataIfExist("SELECT EXISTS (SELECT 1 FROM t WHERE d = '2025-01-01')")  # TODO: replace with your existence check SQL

        assert result is True

    @patch("utils.gcp_utils.bigquery.Client")
    def test_returns_none_when_data_not_exists(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=False)
        mock_client.return_value.query.return_value.result.return_value = [mock_row]

        result = bq.dataIfExist("SELECT EXISTS (SELECT 1 FROM t WHERE d = '2025-01-01')")  # TODO: replace with your existence check SQL

        assert result is None

    @patch("utils.gcp_utils.bigquery.Client")
    def test_executes_provided_sql(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=False)
        mock_client.return_value.query.return_value.result.return_value = [mock_row]

        sql = "SELECT EXISTS (SELECT 1 FROM t WHERE d = '2025-01-01')"  # TODO: replace with your existence check SQL
        bq.dataIfExist(sql)

        mock_client.return_value.query.assert_called_once_with(sql)


class TestExecute(unittest.TestCase):

    def _make_bq(self, mock_client):
        mock_client.return_value.project = "test-project"  # TODO: replace with your GCP project ID
        with patch("os.path.join", return_value="/fake/path/prod.json"):  # TODO: replace with your credentials file path
            from utils.gcp_utils import BQ
            return BQ("my_dataset", "prod")  # TODO: replace with your dataset and config name

    @patch("utils.gcp_utils.bigquery.Client")
    def test_returns_job(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.return_value.query.return_value = mock_job

        result = bq.execute("DELETE FROM t WHERE d = '2025-01-01'")  # TODO: replace with your SQL statement

        assert result == mock_job

    @patch("utils.gcp_utils.bigquery.Client")
    def test_executes_provided_sql(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.return_value.query.return_value = mock_job

        sql = "DELETE FROM t WHERE d = '2025-01-01'"  # TODO: replace with your SQL statement
        bq.execute(sql)

        mock_client.return_value.query.assert_called_once_with(sql)

    @patch("utils.gcp_utils.bigquery.Client")
    def test_calls_result_on_job(self, mock_client):
        bq = self._make_bq(mock_client)
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.return_value.query.return_value = mock_job

        bq.execute("INSERT INTO t VALUES (1)")  # TODO: replace with your SQL statement

        mock_job.result.assert_called_once()


if __name__ == "__main__":
    unittest.main()
