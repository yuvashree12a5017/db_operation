# Genesis DB-to-DB Codex

An independent FastAPI DB-to-DB federated action engine based on
`DB-to-DB_Federated_Action_Engine_PRD_v2.pdf`. It reads one or more relational
sources, runs a strict ordered Polars action plan, explicitly maps the final
result into named target columns, and writes the projected data to a target
database.

This folder does not modify or depend on `dbtodb` or `dbtodb_v2`.

## What is different in this version

The target contract now **requires** `column_mappings`. Nothing is written by
implicit same-name matching. Every output target column must declare exactly
one value provider:

```json
"column_mappings": [
  {"target_column": "customer_id", "source_column": "id", "nullable": false},
  {"target_column": "source_system", "literal": "CRM"},
  {
    "target_column": "full_name",
    "operation": {
      "type": "concat",
      "columns": ["first_name", "last_name"],
      "separator": " "
    }
  }
]
```

Supported mapping operations are `concat`, `coalesce`, `add`, `subtract`,
`multiply`, `divide`, `upper`, `lower`, `trim`, and `cast`. Cast types are
`string`, `integer`, `float`, `boolean`, `date`, and `datetime`. These are typed
operations, not free-form SQL or an expression language.

Before writing, the service:

- verifies every referenced result column exists;
- rejects duplicate target column names;
- checks `nullable: false` mappings for null values;
- emits columns in the exact order declared in `column_mappings`;
- requires merge keys to be present in the declared target columns.

## Layout

```text
db_to_db_codex/
  app/db_to_db_action_service.py
  common/
    audit.py auth.py creds.py metrics.py
    correlation_middleware.py rate_limit.py security_middleware.py
    processors/action_engine/
      models.py registry.py engine.py
      actions_dataset.py actions_multi.py polars_utils.py
      db_loader.py target_mapper.py db_sink.py
  examples/request.json
  tests/
  scripts/run_local.ps1 scripts/run_tests.ps1
  .env.example Dockerfile pyproject.toml
  requirements.txt requirements-dev.txt _version.py
```

## Setup

Python 3.11 or newer is required.

```powershell
cd C:\Users\mysel\OneDrive\Documents\working\genesis\db_to_db_codex
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
```

Copy `.env.example` values into your environment or secret manager. Database
URLs can use `${SECRET:ENVIRONMENT_VARIABLE}` placeholders. Both source and
target URLs are resolved without logging their values.

## Run and test

```powershell
.\scripts\run_tests.ps1
.\scripts\run_local.ps1
```

The API is available at `http://localhost:8000`:

- `GET /health`
- `POST /export/dbtodb`
- Swagger UI at `/docs`

The export endpoint requires either:

- `Authorization: Basic <base64(user:password)>`, using
  `DBTODB_BASIC_USER` and `DBTODB_BASIC_PASSWORD`; or
- `Authorization: Bearer <jwt>`, signed HS256 with `DBTODB_JWT_SECRET`.

See [examples/request.json](examples/request.json) for a full request with
rename, literal, concat, and arithmetic target mappings.

## Target mapping reference

### Direct rename/copy

```json
{"target_column": "warehouse_customer_id", "source_column": "id"}
```

### Literal detail

```json
{"target_column": "load_status", "literal": "READY"}
```

Literal `null` is allowed when the mapping is nullable.

### Safe operation

```json
{
  "target_column": "net_amount",
  "operation": {"type": "subtract", "columns": ["gross_amount", "tax_amount"]}
}
```

Operation constraints are validated: `upper`, `lower`, `trim`, and `cast`
take one column; `subtract` and `divide` take two; `cast` also requires
`data_type`.

## Action support

All 24 PRD action names and strict action-specific configuration contracts are
registered. The MVP implementations are:

- filter, sort, deduplicate, aggregate, sample/limit;
- merge join, union, anti-join;
- row hash and schema validation;
- append/overwrite target write with explicit column mapping.

The remaining advanced PRD actions are contract-validated but return a clear
HTTP 400 "registered but not implemented" response. `write_mode: "merge"` is
validated (including merge keys) but is intentionally still merge-ready rather
than executed, matching the PRD V1 allowance.

## Docker

```powershell
docker build -t genesis-db-to-db-codex .
docker run --rm -p 8000:8000 --env-file .env genesis-db-to-db-codex
```

Source reads use asynchronous drivers (`asyncpg` is included for PostgreSQL).
Target writes use synchronous SQLAlchemy drivers (`psycopg` is included for
PostgreSQL). Add the corresponding async/sync drivers for Oracle, SQL Server,
or MySQL deployments.

## Important behavior

- Pydantic uses `extra="forbid"`; unsupported request and action fields fail
  with HTTP 400 before source loading.
- Dataset dependencies must refer only to sources or earlier actions.
- Schema, table, dataset, and column identifiers are validated.
- Raw SQL transformations are not accepted.
- Independent source loads can run concurrently and can be capped with
  `max_rows_per_source`.
- Action and operation audit events include correlation and node-run IDs.
