import unittest
import csv
import os

from utils.gcp_utils import BQ
from utils.gcp_config import CONFIG

# Usage: python3 -m pytest silver/tests/test_fct_daily_user_login_bitmap.py -v
# Set TEST_DATE env var to test a specific partition (default: 2025-02-06)

TEST_DATE = os.environ.get('TEST_DATE', '2025-02-06')

RAW_CSV_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'raw', 'event_stream.csv')

ARGS = {
    'tdy': TEST_DATE,
    'project': CONFIG['project-id'],
    'dataset': 'silver',
    'table': 'fct_daily_user_login_bitmap',
    'config': 'ua-dwh-sa',
}


def get_csv_devices_for_date(date_str):
    """Unique platform_user_id combos in CSV for a specific date."""
    devices = set()
    with open(RAW_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['event_time'].startswith(date_str):
                devices.add('{}_{}'.format(row['platform'], row['user_id']))
    return devices


def query_bq(bq, sql):
    job = bq.c.query(sql)
    return list(job.result())


class TestFctDailyUserLoginBitmap(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bq = BQ(ARGS['dataset'], ARGS['config'])
        cls.date_str = TEST_DATE
        cls.csv_devices_tdy = get_csv_devices_for_date(cls.date_str)

        rows = query_bq(cls.bq, '''
            SELECT device, login_bitmap, isNew
            FROM `{project}.{dataset}.{table}`
            WHERE event_date = '{tdy}'
        '''.format(**ARGS))
        cls.bq_rows = {r[0]: {'login_bitmap': r[1], 'isNew': r[2]} for r in rows}
        cls.bq_devices_tdy = set(cls.bq_rows.keys())

        print('\nDate           : {}'.format(cls.date_str))
        print('CSV devices    : {}'.format(len(cls.csv_devices_tdy)))
        print('BQ devices     : {}'.format(len(cls.bq_devices_tdy)))

    def test_bq_contains_all_csv_devices_for_tdy(self):
        """Every device active in CSV today must appear in BQ for tdy."""
        missing = self.csv_devices_tdy - self.bq_devices_tdy
        self.assertEqual(len(missing), 0,
            msg='Devices in CSV missing from BQ: {}'.format(missing))

    def test_no_null_devices(self):
        """device column must have no NULLs for tdy partition."""
        rows = query_bq(self.bq, '''
            SELECT COUNT(*)
            FROM `{project}.{dataset}.{table}`
            WHERE event_date = '{tdy}' AND device IS NULL
        '''.format(**ARGS))
        self.assertEqual(rows[0][0], 0,
            msg='Found {} NULL devices for {}'.format(rows[0][0], self.date_str))

    def test_device_format(self):
        """Every device must match platform_userid pattern."""
        rows = query_bq(self.bq, '''
            SELECT COUNT(*)
            FROM `{project}.{dataset}.{table}`
            WHERE event_date = '{tdy}'
              AND NOT REGEXP_CONTAINS(device, r'^.+_.+$')
        '''.format(**ARGS))
        self.assertEqual(rows[0][0], 0,
            msg='Found {} devices not matching platform_userid pattern'.format(rows[0][0]))

    def test_active_users_bitmap_ends_with_1(self):
        """Users active today (in CSV) must have login_bitmap ending in '1'."""
        bad = [
            d for d in self.csv_devices_tdy
            if d in self.bq_rows and not self.bq_rows[d]['login_bitmap'].endswith('1')
        ]
        self.assertEqual(len(bad), 0,
            msg='Active users whose bitmap does not end in 1: {}'.format(bad))

    def test_inactive_users_bitmap_ends_with_0(self):
        """Users carried forward from ytd but not active today must have bitmap ending in '0'."""
        bad = [
            d for d in self.bq_devices_tdy - self.csv_devices_tdy
            if not self.bq_rows[d]['login_bitmap'].endswith('0')
        ]
        self.assertEqual(len(bad), 0,
            msg='Inactive users whose bitmap does not end in 0: {}'.format(bad))

    def test_new_users_bitmap_is_1(self):
        """New users (isNew='1') must have login_bitmap = '1' only."""
        bad = [
            d for d, v in self.bq_rows.items()
            if v['isNew'] == '1' and v['login_bitmap'] != '1'
        ]
        self.assertEqual(len(bad), 0,
            msg='New users with bitmap != 1: {}'.format(bad))

    def test_new_users_are_in_csv_tdy(self):
        """Every user marked isNew='1' must appear in the CSV for tdy."""
        new_devices = {d for d, v in self.bq_rows.items() if v['isNew'] == '1'}
        not_in_csv = new_devices - self.csv_devices_tdy
        self.assertEqual(len(not_in_csv), 0,
            msg='New users not found in CSV for tdy: {}'.format(not_in_csv))


if __name__ == '__main__':
    unittest.main()
