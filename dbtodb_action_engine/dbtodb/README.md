# Genesis DB-to-DB Action Engine

A FastAPI microservice that reads datasets from multiple heterogeneous
relational databases, executes a declarative, ordered list of actions
against them using Polars, and writes the final materialized dataset into a
target database.

This is a **separate, adapted build** of the original DB-to-DB Federated
Action Engine PRD (`dbtodb_v2`). It keeps the same overall contract
(request envelope, source/target config, action registry, audit/metrics/
security requirements, module layout) but adds one new capability the
original contract didn't have:

## What's different from `dbtodb_v2`: the `output.target_column` directive

In the original contract, every action always replaced its output dataset
wholesale — e.g. `action-filter-dataset` dropped non-matching rows,
`action-reconciliation` always wrote its match/mismatch status into a fixed
column name (`recon_status`), and there was no way to say "run this
operation, but write its result into *this specific column* of the
existing dataset instead."

This build adds an optional `output` block to every action envelope:

```json
{
  "action": "action-filter-dataset",
  "name": "users_with_active_flag",
  "input": ["users_pg"],
  "config": { "condition": { "column": "status", "op": "eq", "value": "ACTIVE" } },
  "output": { "mode": "flag_column", "target_column": "is_active" }
}
```

- `output.mode = "replace"` (default) — unchanged original behavior: the
  action's natural output becomes the new dataset.
- `output.mode = "flag_column"` — the action keeps **every row and every
  column of its input untouched** and adds one new column, named
  `output.target_column`, holding the computed detail for that row (a
  boolean flag, a hash, a reconciliation status, a delta change type, a
  converted value, etc). `target_column` is required in this mode.

Only actions where "write a detail into a column" is a meaningful operation
accept `flag_column` mode (see `FLAG_COLUMN_SUPPORTED_ACTIONS` in
`models.py`); requesting it on an action like `action-sort-dataset` or
`action-merge-join` is rejected with a 400 before anything runs.

| Action | `flag_column` behavior |
|---|---|
| `action-filter-dataset` | Adds a boolean match column instead of dropping rows |
| `action-schema-validate` | Adds a per-row `"OK"` / `"MISSING:col1,col2"` detail column instead of raising |
| `action-format-convert` | Writes the converted value into a new column instead of overwriting the source column (requires exactly one entry in `config.columns`) |
| `action-anti-join` | Adds a boolean "missing in target" column to every left row instead of returning only unmatched rows |
| `action-exception-extract` | Adds a boolean exception-match column instead of filtering |
| `action-reconciliation` | Lets you rename the match/mismatch status column (default `recon_status`) |
| `action-delta-detection` | Lets you rename the `INSERT`/`UPDATE`/`DELETE`/`UNCHANGED` column (default `change_type`) |
| `action-snapshot-diff` | Lets you rename the `ADDED`/`REMOVED`/`CHANGED`/`UNCHANGED` column (default `change_status`) |
| `action-cross-validate` | Lets you rename the `PASS`/`FAIL` column (default `validation_status`) |

`action-row-hash` already targets a specific column via its own
`config.target_column` (unchanged from the original contract); if you also
set `output.target_column`, it overrides `config.target_column` for that
action.

This is an **MVP+ build**: 16 of the 24 actions defined in the contract are
fully implemented (see "Implemented vs. stubbed actions" below), including
all of the ones that meaningfully exercise the new target-column feature.
The other 8 are registered so the API surface and validation behave
correctly, but calling them raises a `400` with a clear "not implemented"
message.

## 1. Folder layout

```
dbtodb/
  app/
    db_to_db_action_service.py     FastAPI app: /health, POST /export/dbtodb
  common/
    audit.py                       Audit event publisher (logs ACTION_*/OPERATION_* events)
    metrics.py                     In-memory latency/success/error recorder
    creds.py                       Resolves ${SECRET:name} placeholders from env vars
    auth.py                        verify_jwt_or_basic dependency
    security_middleware.py         Request size limit + CORS
    correlation_middleware.py      X-Correlation-Id propagation
    rate_limit.py                  slowapi limiter (30/minute on the export endpoint)
    processors/action_engine/
      models.py                   Pydantic request/action/response contracts + OutputSpec
      registry.py                 ACTION_REGISTRY + is_implemented + supports_flag_column
      engine.py                   ActionExecutionEngine (orchestration, audit, target-column validation)
      actions_dataset.py          Single-dataset actions
      actions_multi.py            Multi-dataset actions
      polars_utils.py             Condition parsing, identifiers, type casting, hash + mismatch helpers
      db_loader.py                Async SQLAlchemy source read -> Polars LazyFrame
      db_sink.py                  Writes the final/staging DataFrame to a target table
  tests/
    test_actions_dataset.py        Unit tests for dataset actions (replace + flag_column)
    test_actions_multi.py          Unit tests for multi-dataset actions (replace + flag_column)
    test_target_column_contract.py Contract/engine-level tests for output.target_column
    fixtures/sample_actions.json   Per-action examples + an end-to-end sample request
  requirements.txt
  Dockerfile
  _version.py
```

> **Note on `common/*`:** as with the original build, these are small
> standalone placeholder implementations of the shared Genesis framework
> modules (`common.audit`, `common.creds`, etc.) since this folder doesn't
> contain the rest of the Genesis monorepo — swap them for the real shared
> modules if this code is merged into the actual Genesis codebase.

## 2. Prerequisites

- Python 3.11+ (tested with 3.14)
- pip
- Network access to whatever Postgres/Oracle/SQL Server databases you
  actually want to read from/write to (not required just to run tests)

## 3. Setup

```powershell
cd C:\Users\mysel\OneDrive\Documents\working\genesis\dbtodb_action_engine\dbtodb
python -m pip install -r requirements.txt
```

## 4. Run the unit tests

These don't need a database — they run the dataset/multi-dataset actions
against small in-memory Polars frames, in both `replace` and
`flag_column` output modes.

```powershell
cd C:\Users\mysel\OneDrive\Documents\working\genesis\dbtodb_action_engine\dbtodb
python -m pytest tests/ -q
```

Expected: `27 passed`.

## 5. Run the service locally

```powershell
cd C:\Users\mysel\OneDrive\Documents\working\genesis\dbtodb_action_engine\dbtodb
python -m uvicorn app.db_to_db_action_service:app --reload --port 8000
```

- Health check: `GET http://localhost:8000/health` -> `{"status": "OK"}`
- Interactive API docs: `http://localhost:8000/docs`

### Auth

Every call to `POST /export/dbtodb` requires an `Authorization` header,
either:

- `Authorization: Bearer <jwt>` — verified against `DBTODB_JWT_SECRET`
  (env var, HS256)
- `Authorization: Basic <base64(user:pass)>` — verified against
  `DBTODB_BASIC_USER` / `DBTODB_BASIC_PASSWORD` (env vars)

```powershell
$env:DBTODB_BASIC_USER = "admin"
$env:DBTODB_BASIC_PASSWORD = "changeme"
```

### Credential placeholders in connection URLs

Any `sqlalchemy_url` in a request can contain `${SECRET:NAME}`, resolved
from the environment variable `NAME` at request time:

```json
"sqlalchemy_url": "postgresql+asyncpg://user:${SECRET:PG_PASSWORD}@host:5432/db"
```

```powershell
$env:PG_PASSWORD = "..."
```

## 6. Send a test request

The full end-to-end example is saved at
`tests/fixtures/sample_actions.json` under `end_to_end_sample_request`. It
filters a Postgres `users` table (writing an `is_active` flag column via
`output.target_column`), joins it with an Oracle `accounts` table, hashes
the result, and writes to an `analytics.user_account_view` table.

To try it against real databases, edit the `sqlalchemy_url` values in that
JSON, extract the object into its own file, then POST it:

```powershell
python -c "import json; d=json.load(open('tests/fixtures/sample_actions.json')); json.dump(d['end_to_end_sample_request'], open('request.json','w'))"
curl.exe -X POST http://localhost:8000/export/dbtodb `
  -H "Authorization: Basic $([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('admin:changeme')))" `
  -H "Content-Type: application/json" `
  --data-binary "@request.json"
```

## 7. Implemented vs. stubbed actions

| Status | Actions |
|---|---|
| **Implemented** | `action-filter-dataset`, `action-sort-dataset`, `action-deduplicate`, `action-aggregate`, `action-sample-limit`, `action-format-convert`, `action-schema-validate`, `action-row-hash`, `action-merge-join`, `action-union`, `action-anti-join`, `action-exception-extract`, `action-reconciliation`, `action-delta-detection`, `action-snapshot-diff`, `action-cross-validate`, plus target table write (append/overwrite) |
| **Registered, not implemented (400 on use)** | `action-flatten-json`, `action-explode-array`, `action-pivot-unpivot`, `action-write-staging` (passes data through but doesn't persist to staging yet), `action-error-quarantine`, `action-handle-drift`, `action-aggregate-multi`, `action-rolling-join` |
| **Not implemented** | `write_mode: "merge"` on the target write |

To implement one of the stubbed actions, edit its function in
`actions_dataset.py` or `actions_multi.py`, then add its name to
`MVP_ACTIONS` (and, if it should support `output.mode="flag_column"`, to
`FLAG_COLUMN_SUPPORTED_ACTIONS`) in `models.py`.

## 8. Docker

```powershell
docker build -t genesis-dbtodb-action-engine .
docker run -p 8000:8000 --env-file .env genesis-dbtodb-action-engine
```

## 9. What was verified

- All 27 unit tests pass (`python -m pytest tests/ -q`), covering every
  implemented action in both `output.mode="replace"` and
  `output.mode="flag_column"` where applicable.
- The FastAPI app imports cleanly and exposes `/health` and
  `/export/dbtodb`.
- The end-to-end sample request (including its `output.target_column`
  usage) validates against the `DbToDbRequest` Pydantic model without
  errors.

What has **not** been verified: an actual run against live
Postgres/Oracle/SQL Server databases (no credentials available in this
environment), and the Docker build.

## 10. Roadmap / not yet built

You mentioned more features are coming beyond the target-column directive
covered here — bring those requirements whenever you're ready and this
project can be extended the same way (new fields in `models.py`, new
branches in the relevant action functions, new tests).
