# Test Summary — gcp_utils.py

**Date:** 2026-04-05  
**File tested:** `utils/gcp_utils.py`  
**Test file:** `utils/tests/test_gcp_utils.py`  
**Result:** 12/12 passed

---

## Results

| Test Class | Test | Status |
|---|---|---|
| `TestBQInit` | `test_init_sets_credentials_env` | PASSED |
| `TestBQInit` | `test_init_creates_dataset_ref` | PASSED |
| `TestTableIfNotExist` | `test_returns_false_when_table_exists` | PASSED |
| `TestTableIfNotExist` | `test_returns_true_when_table_not_found` | PASSED |
| `TestTableCreate` | `test_creates_table_with_partition` | PASSED |
| `TestTableCreate` | `test_table_created_with_day_partitioning` | PASSED |
| `TestDataIfExist` | `test_returns_true_when_data_exists` | PASSED |
| `TestDataIfExist` | `test_returns_none_when_data_not_exists` | PASSED |
| `TestDataIfExist` | `test_executes_provided_sql` | PASSED |
| `TestExecute` | `test_returns_job` | PASSED |
| `TestExecute` | `test_executes_provided_sql` | PASSED |
| `TestExecute` | `test_calls_result_on_job` | PASSED |

---

## Coverage

| Method | Tests |
|---|---|
| `BQ.__init__` | credentials env var set, dataset ref created correctly |
| `BQ.tableIfNotExist` | returns `False` when table exists, `True` on `NotFound` |
| `BQ.tableCreate` | calls `create_table`, returns `True`, DAY partitioning applied |
| `BQ.dataIfExist` | returns `True` when data found, `None` when not, correct SQL passed |
| `BQ.execute` | returns job, correct SQL passed, `.result()` called |
