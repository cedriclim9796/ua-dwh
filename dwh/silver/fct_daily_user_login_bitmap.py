from utils.gcp_utils import BQ
from utils.gcp_config import CONFIG
from google.cloud import bigquery
from datetime import datetime, timedelta
import sys

# Usage: python silver/fct_daily_user_login_bitmap.py <YYYYMMDD> <config>
# Example: python silver/fct_daily_user_login_bitmap.py 20250206 ua-dwh-sa

tdy_dt = datetime.strptime(sys.argv[1], '%Y%m%d')
tdy_str = datetime.strftime(tdy_dt, '%Y-%m-%d')
ytd_str = datetime.strftime(tdy_dt + timedelta(days=-1), '%Y-%m-%d')

ARGS = {
    'tdy': tdy_str,
    'ytd': ytd_str,
    'project': CONFIG['project-id'],
    'dataset_src': 'bronze',
    'table_id_src': 'events',
    'dataset_dst': 'silver',
    'table_id_dst': 'fct_daily_user_login_bitmap',
    'config': sys.argv[2],
    'epoch': '2025-02-06',  # day 0 — earliest date in dataset
}

schema = [
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("utm_source", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("login_bitmap", "STRING"),
    bigquery.SchemaField("isNew", "STRING"),
    bigquery.SchemaField("reg_date", "DATE"),
    bigquery.SchemaField("device", "STRING"),
]

if __name__ == '__main__':
    bq = BQ(ARGS['dataset_dst'], ARGS['config'])

    if bq.tableIfNotExist(ARGS['table_id_dst']):
        print('Table not found — creating silver.fct_daily_user_login_bitmap...')
        bq.tableCreate(schema, ARGS['table_id_dst'], 'event_date')

    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM {project}.{dataset_dst}.{table_id_dst}
            WHERE event_date = '{tdy}'
        )
    '''.format(**ARGS)

    if bq.dataIfExist(sql):
        print('Data exists for {tdy} — overwriting...'.format(**ARGS))
        sql = '''
            DELETE FROM {project}.{dataset_dst}.{table_id_dst}
            WHERE event_date = '{tdy}'
        '''.format(**ARGS)
        bq.execute(sql)

    sql = '''
    INSERT INTO {project}.{dataset_dst}.{table_id_dst} (
        event_date,
        country,
        platform,
        utm_source,
        user_id,
        login_bitmap,
        isNew,
        reg_date,
        device
    )
    WITH _part_ytd AS (
        SELECT 
            event_date,
            country,
            platform,
            utm_source,
            user_id,
            reg_date,
            login_bitmap,
            device
        FROM {project}.{dataset_dst}.{table_id_dst} 
        WHERE event_date = '{ytd}'
    ),
    _part_tdy AS (
        SELECT
            date(event_time) event_date,
            country,
            platform,
            utm_source,
            user_id
        FROM {project}.{dataset_src}.{table_id_src}
        WHERE DATE(event_time) = '{tdy}'
        group by 1,2,3,4,5
    )
    select 
        DATE('{tdy}') event_date,
        coalesce(ytd.country,tdy.country) country,
        coalesce(ytd.platform,tdy.platform) platform,
        coalesce(ytd.utm_source,tdy.utm_source) utm_source,
        coalesce(ytd.user_id,tdy.user_id) user_id,
        case 
            #new user
            when tdy.user_id is not null and ytd.user_id is null then '1'
           
             #existing user login 
            when tdy.user_id is not null and ytd.user_id is not null then CONCAT(ytd.login_bitmap,'1')
            
            #existing user NOT login 
            when tdy.user_id is null and ytd.user_id is not null then CONCAT(ytd.login_bitmap,'0')
        end login_bitmap,
        if(tdy.user_id is not null and ytd.user_id is null,'1','0') isNew,
        coalesce(ytd.reg_date,tdy.event_date) reg_date,
        coalesce(ytd.device,CONCAT(tdy.platform,'_',tdy.user_id)) device
    from 
        _part_tdy tdy
    full outer join 
        _part_ytd ytd
    on tdy.user_id = ytd.user_id

    '''.format(**ARGS)

    bq.execute(sql)
    print('Done. Loaded silver.fct_daily_user_login_bitmap for {tdy}'.format(**ARGS))
