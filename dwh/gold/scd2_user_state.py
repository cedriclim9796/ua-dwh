from utils.gcp_utils import BQ
from utils.gcp_config import CONFIG
from google.cloud import bigquery
import sys

# Usage: python gold/scd2_user_state.py <config>
# Example: python gold/scd2_user_state.py ua-dwh-sa
# Full rebuild — reads all silver partitions and rewrites SCD2 table

ARGS = {
    'project':       CONFIG['project-id'],
    'dataset_src':   'silver',
    'table_id_src':  'fct_daily_user_login_bitmap',
    'dataset_dst':   'gold',
    'table_id_dst':  'scd2_user_state',
    'config':        sys.argv[1],
    'window':        14,    # days in each activity window
}

schema = [
    bigquery.SchemaField("user_id",     "STRING"),
    bigquery.SchemaField("user_state",  "STRING"),
    bigquery.SchemaField("start_date",  "DATE"),
    bigquery.SchemaField("end_date",    "DATE"),
]

if __name__ == '__main__':
    bq = BQ(ARGS['dataset_dst'], ARGS['config'])

    if bq.tableIfNotExist(ARGS['table_id_dst']):
        print('Table not found — creating {dataset_dst}.{table_id_dst}...'.format(**ARGS))
        bq.tableCreate(schema, ARGS['table_id_dst'], 'start_date')

    sql = '''
    CREATE TEMP FUNCTION BitmapToInt(b STRING) AS ((
        SELECT SUM(CAST(c AS INT64) * CAST(POW(2, LENGTH(b) - 1 - off) AS INT64))
        FROM UNNEST(SPLIT(b, '')) c WITH OFFSET off
    ));

    -- Step 1: find each UID's earliest reg_date across all devices
    WITH uid_epoch AS (
        SELECT
            user_id,
            MIN(reg_date) AS uid_reg_date
        FROM `{project}.{dataset_src}.{table_id_src}`
        GROUP BY user_id
    ),

    -- Step 2: align each device bitmap to the UID epoch and aggregate to UID per day
    uid_daily AS (
        SELECT
            s.user_id,
            s.event_date,
            e.uid_reg_date,
            BIT_OR(
                BitmapToInt(
                    LPAD(
                        s.login_bitmap,
                        LENGTH(s.login_bitmap) + DATE_DIFF(s.reg_date, e.uid_reg_date, DAY),
                        '0'
                    )
                )
            ) AS uid_bits
        FROM `{project}.{dataset_src}.{table_id_src}` s
        JOIN uid_epoch e USING (user_id)
        GROUP BY s.user_id, s.event_date, e.uid_reg_date
    ),

    -- Step 3: classify state per (user_id, event_date) using {window}-day windows
    classified AS (
        SELECT
            user_id,
            event_date,
            uid_reg_date,
            -- active days in current window (last {window} bits)
            BIT_COUNT(uid_bits & ((1 << {window}) - 1))                     AS active_now,
            -- active days in prior window (bits {window} to {window}*2-1)
            BIT_COUNT((uid_bits >> {window}) & ((1 << {window}) - 1))        AS active_prior,
            -- days since registration — used to distinguish new vs resurrected
            DATE_DIFF(event_date, uid_reg_date, DAY)                         AS days_since_reg
        FROM uid_daily
    ),

    with_state AS (
        SELECT
            user_id,
            event_date,
            CASE
                -- new: within first window, has activity
                WHEN days_since_reg < {window} AND active_now > 0
                    THEN 'new'
                -- resurrected: active now, inactive in prior window, seen before
                WHEN active_now > 0 AND active_prior = 0 AND days_since_reg >= {window}
                    THEN 'resurrected'
                -- retained: active in both windows
                WHEN active_now > 0 AND active_prior > 0
                    THEN 'retained'
                -- churned: inactive now, was active in prior window
                WHEN active_now = 0 AND active_prior > 0
                    THEN 'churned'
                ELSE NULL
            END AS user_state
        FROM classified
        -- exclude rows with no signal in either window
        WHERE active_now > 0 OR active_prior > 0
    ),

    -- Step 4: detect state changes per user (islands pattern)
    with_change AS (
        SELECT
            user_id,
            event_date,
            user_state,
            IF(
                user_state = LAG(user_state) OVER (PARTITION BY user_id ORDER BY event_date),
                0, 1
            ) AS is_new_segment
        FROM with_state
        WHERE user_state IS NOT NULL
    ),

    -- Step 5: assign segment IDs by cumulative sum of change flags
    with_segment AS (
        SELECT
            user_id,
            event_date,
            user_state,
            SUM(is_new_segment) OVER (PARTITION BY user_id ORDER BY event_date) AS segment_id
        FROM with_change
    ),

    -- Step 6: collapse each segment into one SCD2 row
    segments AS (
        SELECT
            user_id,
            user_state,
            MIN(event_date) AS start_date,
            MAX(event_date) AS end_date_raw
        FROM with_segment
        GROUP BY user_id, user_state, segment_id
    )

    -- Step 7: set end_date to day before next segment starts (NULL = current/open record)
    SELECT
        user_id,
        user_state,
        start_date,
        DATE_SUB(
            LEAD(start_date) OVER (PARTITION BY user_id ORDER BY start_date),
            INTERVAL 1 DAY
        ) AS end_date
    FROM segments
    ORDER BY user_id, start_date
    '''.format(**ARGS)

    print('Building {dataset_dst}.{table_id_dst} from all silver partitions...'.format(**ARGS))

    # Full rebuild — truncate and reload
    truncate_sql = 'DELETE FROM {project}.{dataset_dst}.{table_id_dst} WHERE 1=1'.format(**ARGS)
    bq.execute(truncate_sql)

    insert_sql = '''
        INSERT INTO {project}.{dataset_dst}.{table_id_dst}
        (user_id, user_state, start_date, end_date)
        {select_sql}
    '''.format(select_sql=sql, **ARGS)

    bq.execute(insert_sql)
    print('Done. Rebuilt {dataset_dst}.{table_id_dst}'.format(**ARGS))
