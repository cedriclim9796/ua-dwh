# Test Summary — silver.fct_daily_user_login_bitmap

**Date tested:** 2025-02-06
**Run:** `python3 -m pytest silver/tests/test_fct_daily_user_login_bitmap.py -v`

## Counts

| | Value |
|---|---|
| Date | 2025-02-06 |
| CSV devices (tdy) | — |
| BQ devices (tdy) | — |

## Results

| Test | Status |
|---|---|
| `test_bq_contains_all_csv_devices_for_tdy` | PASSED |
| `test_no_null_devices` | PASSED |
| `test_device_format` | PASSED |
| `test_active_users_bitmap_ends_with_1` | PASSED |
| `test_inactive_users_bitmap_ends_with_0` | PASSED |
| `test_new_users_bitmap_is_1` | PASSED |
| `test_new_users_are_in_csv_tdy` | PASSED |

**7 passed, 0 failed**

## Test descriptions

| Test | What it checks |
|---|---|
| `test_bq_contains_all_csv_devices_for_tdy` | All active CSV users exist in BQ for tdy |
| `test_no_null_devices` | No NULL devices in tdy partition |
| `test_device_format` | All devices match `platform_userid` pattern |
| `test_active_users_bitmap_ends_with_1` | Users active today → bitmap ends with `'1'` |
| `test_inactive_users_bitmap_ends_with_0` | Carried-forward users not active today → bitmap ends with `'0'` |
| `test_new_users_bitmap_is_1` | New users (isNew=`'1'`) → bitmap is exactly `'1'` |
| `test_new_users_are_in_csv_tdy` | All new users must exist in CSV for tdy |
