from utils.gcp_utils import BQ
from utils.gcp_config import CONFIG
from google.cloud import bigquery
from datetime import datetime
import csv
import io
import os
import sys

# Usage: python bronze/etl_event_stream.py <YYYYMMDD> <config>
# Example: python bronze/etl_event_stream.py 20250206 prod

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')

ARGS = {
    'tdy': tdy_str,
    'config': sys.argv[2],
    'dataset_dst': 'bronze',
    'table_id_dst': 'events',
    'raw_path': os.path.join(os.path.dirname(__file__), '..', 'raw', 'event_stream.csv'),
}

schema = [
    bigquery.SchemaField("event_time", "TIMESTAMP"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("transaction_category", "STRING"),
    bigquery.SchemaField("miles_amount", "FLOAT64"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("country", "STRING"),
]

if __name__ == '__main__':
    bq = BQ(ARGS['dataset_dst'], ARGS['config'])

    if bq.tableIfNotExist(ARGS['table_id_dst']):
        print('Table not found — creating bronze.events...')
        table_ref_create = bq.dataset_ref.table(ARGS['table_id_dst'])
        table = bigquery.Table(table_ref_create, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='event_time',
        )
        table = bq.c.create_table(table)

        # Patch partition expiration to None to override dataset-level default
        # Required for loading historical data older than the dataset default (60 days)
        table._properties['timePartitioning']['expirationMs'] = None
        bq.c.update_table(table, ['time_partitioning'])
        print('Created table {} with no partition expiration'.format(ARGS['table_id_dst']))

    # Check if data already loaded for today — delete before reload (idempotent)
    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM {dataset_dst}.{table_id_dst}
            WHERE DATE(event_time) = '{tdy}'
        )
    '''.format(**ARGS)

    if bq.dataIfExist(sql):
        print('Data exists for {tdy} — overwriting...'.format(**ARGS))
        sql = '''
            DELETE FROM {dataset_dst}.{table_id_dst}
            WHERE DATE(event_time) = '{tdy}'
        '''.format(**ARGS)
        bq.execute(sql)

    # Filter CSV rows to only the target date
    print('Filtering event_stream.csv for {tdy}...'.format(**ARGS))

    buf = io.StringIO()
    writer = csv.writer(buf)

    with open(ARGS['raw_path'], 'r') as f:
        reader = csv.DictReader(f)
        writer.writerow(reader.fieldnames)
        rows = [
            row for row in reader
            if row['event_time'].startswith(ARGS['tdy'])
        ]
        for row in rows:
            writer.writerow(row.values())

    print('Found {} rows for {tdy} — loading into bronze.events...'.format(len(rows), **ARGS))

    table_ref = '{project}.{dataset}.{table}'.format(
        project=CONFIG['project-id'],
        dataset=ARGS['dataset_dst'],
        table=ARGS['table_id_dst'],
    )

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    buf.seek(0)
    load_job = bq.c.load_table_from_file(
        io.BytesIO(buf.getvalue().encode()),
        table_ref,
        job_config=job_config,
    )
    load_job.result()

    rows_loaded = bq.c.get_table(table_ref).num_rows
    print('Done. Total rows in bronze.events: {}'.format(rows_loaded))
