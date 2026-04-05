import unittest
import csv
import os
import sys
from datetime import datetime

from utils.gcp_utils import BQ
from utils.gcp_config import CONFIG

# Usage: python3 -m pytest bronze/tests/test_etl_event_stream.py -v --date 20250206
# Default date falls back to TEST_DATE env var or hardcoded value below

TEST_DATE = os.environ.get('TEST_DATE', '2025-02-06')  # TODO: set via env or --date arg

RAW_CSV_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'raw', 'event_stream.csv')

ARGS = {
    'tdy': TEST_DATE,
    'project': CONFIG['project-id'],
    'dataset': 'bronze',
    'table': 'events',
    'config': 'ua-dwh-sa',  # TODO: update if config name changes
}


def count_csv_rows_for_date(date_str):
    """Count rows in event_stream.csv matching the given date (YYYY-MM-DD)."""
    count = 0
    with open(RAW_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['event_time'].startswith(date_str):
                count += 1
    return count


def count_bq_rows_for_date(bq, date_str):
    """Count rows in bronze.events in BigQuery for the given date partition."""
    sql = '''
        SELECT COUNT(*) as row_count
        FROM `{project}.{dataset}.{table}`
        WHERE DATE(event_time) = '{tdy}'
    '''.format(**ARGS)

    job = bq.c.query(sql)
    rows = list(job.result())
    return rows[0][0]


class TestEtlEventStreamRowCount(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bq = BQ(ARGS['dataset'], ARGS['config'])
        cls.date_str = TEST_DATE
        cls.csv_count = count_csv_rows_for_date(cls.date_str)
        cls.bq_count = count_bq_rows_for_date(cls.bq, cls.date_str)
        print('\nDate: {}'.format(cls.date_str))
        print('CSV row count : {}'.format(cls.csv_count))
        print('BQ row count  : {}'.format(cls.bq_count))

    def test_row_counts_match(self):
        self.assertEqual(
            self.csv_count,
            self.bq_count,
            msg='Row count mismatch for {}: CSV={}, BQ={}'.format(
                self.date_str, self.csv_count, self.bq_count
            )
        )

    def test_bq_partition_is_not_empty(self):
        self.assertGreater(
            self.bq_count, 0,
            msg='BigQuery partition for {} is empty'.format(self.date_str)
        )

    def test_csv_has_rows_for_date(self):
        self.assertGreater(
            self.csv_count, 0,
            msg='No rows found in CSV for date {}'.format(self.date_str)
        )


if __name__ == '__main__':
    unittest.main()
