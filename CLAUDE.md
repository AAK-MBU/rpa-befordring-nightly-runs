# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

RPA bot for Aarhus Kommune that runs nightly and catches date-based changes affecting transport reimbursement (*befordring*). It is based on [odense-rpa/process-template](https://github.com/odense-rpa/process-template).

The work itself is SQL-driven: the bot calls stored procedures in the `befordring` schema of the *Befordringssystemet* database and imports address data from LOIS. There is no GUI automation, despite the `application_handler` lifecycle inherited from the template.

## Commands

```bash
# Install dependencies
uv sync

# Lint (CI runs `ruff check` on every push and PR)
uv run ruff check .
uv run ruff format .

# Run phases individually (requires .env — see Environment variables)
uv run python main.py --queue      # populate workqueue
uv run python main.py --process    # process workqueue items
uv run python main.py --finalize   # finalize process
```

Flags are independent and can be combined; they run in the order queue → process → finalize regardless of argument order.

CI (`.github/workflows/check_version_number.yml`) fails any PR to `main` that does not raise `version` in `pyproject.toml`.

## Architecture

The process runs in three phases, each triggered by a CLI flag. `main.py` calls `prod_workqueue.clear_workqueue()` unconditionally at startup, **before** dispatching on any flag — so a `--process` run in a separate invocation from `--queue` wipes the queue it was meant to process. Today all phases must run in a single invocation.

1. **`--queue`** (`populate_queue`): Calls `retrieve_items_for_queue()` to build a list of `{reference, data}` dicts, deduplicates against existing ATS workqueue items by reference, then bulk-adds new items via `concurrent_add` (asyncio semaphore + exponential backoff). Items are wrapped as `{"item": {...}}` when added, which is what `get_item_info()` unpacks.

2. **`--process`** (`process_workqueue`): Iterates the workqueue up to `MAX_RETRY=10` times on `ProcessError`. `BusinessError` marks the item `pending_user` without counting as a retry; `ProcessError` fails the item, sends an error email with screenshot, and increments the retry counter. Calls `startup()`/`close()` around the loop with `reset()` after each recoverable failure.

3. **`--finalize`** (`finalize()`): Runs post-processing cleanup; any failure here sends an email and re-raises.

### Queue items and actions

`retrieve_items_for_queue()` emits a single item, `{today}_nightly_run`, because the steps below form a dependency chain and separate workqueue items give no ordering guarantee. `process_item()` dispatches on `data["action"]`; an unknown action raises `BusinessError`. Each step is still individually dispatchable by hand for debugging — queue an item with that step's action name.

| Step | What | Where |
|---|---|---|
| 1 | Addresses: `LOIS.DAR.AdresseDkGeoView` → `Adresse_STG` → `Adresse` | `_fetch_and_upsert_addresses` + `usp_upsert_adresser_from_stg` |
| 2 | `Elev_STG` → `Elev` (not matrikel_id / ungdomsuddannelse_id / adresse_id / skoleafstand / kraever_genberegning) | `usp_upsert_elev_from_stg` |
| 3 | `Foraelder_STG` → `Foraelder` (not adresse_id / maa_vide_barns_adresse) | `usp_upsert_foraelder_from_stg` |
| 4 | `adresse_id` for elever and forældre: `LOIS.CPR.PersonGeoView` → `Elev_Adresse_STG` → both tables | `_fetch_and_upsert_person_adresser` + `usp_upsert_adresse_ids_from_stg` |
| 5 | Recalculate every bevilling's status; raise revurdering / genbehandling | `usp_recalculate_bevilling_status` |
| 6 | Derive the student's school from their bevilling | `usp_sync_elev_matrikel_from_bevilling` |
| 7 | Walking distance for everyone flagged; clears the flag | `_calculate_gaaafstand` |

Step 5 runs **before** step 6 deliberately: it is what turns Kommende into Aktiv, and step 6's first question is which bevilling is active. Reverse them and a newly-activated bevilling's school does not reach the student until the following night.

`kraever_genberegning` is raised by whichever step owns the column that changed — skolekode in step 2, adresse_id in step 4, school in step 6 — and cleared only by step 7.

### Key modules

| Module | Role |
|---|---|
| `processes/queue_handler.py` | `retrieve_items_for_queue()` (builds the day's actions) and `concurrent_add()` |
| `processes/process_item.py` | `process_item()` action dispatch plus the `_exec_sp` / `_fetch_and_upsert_addresses` implementations |
| `processes/finalize_process.py` | `finalize_process()` (stub — returns immediately) |
| `processes/application_handler.py` | `startup/close/reset` lifecycle for GUI automation; exposes a global `APP` variable. All bodies are empty — kept from the template, nothing to start or close. |
| `processes/error_handling.py` | `ErrorContext` dataclass + `handle_error()` + email/screenshot dispatch |
| `helpers/ats_functions.py` | Thin wrappers around the ATS REST API (paginated item listing, item unpacking, logger init) |
| `helpers/db.py` | Lazy SQLAlchemy engine/session factory over `DBCONNECTIONSTRINGBEFORDRING`; `get_db()` context manager |
| `helpers/config.py` | Global tunables: `MAX_RETRY`, `MAX_CONCURRENCY`, `MAX_RETRIES`, `RETRY_BASE_DELAY`, `DRY_RUN` |

Two database access styles coexist: `_exec_sp` goes through SQLAlchemy (`helpers.db.get_db`), while `_fetch_and_upsert_addresses` opens raw `pyodbc` connections from its own env vars.

### Elev is not written from here

`_exec_sp` used to post-process Kommende→Aktiv transitions by copying the newly-active bevilling's school onto `Elev` and re-running the SP, so the transition would not raise a flag. That is removed. It never worked (it wrote `Elev.matrikel_id`, but the SP compares `Elev.skolekode`), and it suppressed something that is now meant to be seen: since revurdering and genbehandling were split, a skolekode mismatch raises **genbehandling**, which holds until a caseworker marks it handled.

A Kommende bevilling activating at a school that differs from the child's registered school therefore lands on the Genbehandling page. That is expected when a child changes school, and clearing it is one click.

The underlying rule, already stated in `_calculate_gaaafstand`: the data worker owns `Elev` and writes the authoritative values there. Nothing in this repo writes `Elev` from a bevilling.

### `DRY_RUN`

`config.DRY_RUN` (currently `False`) makes `_exec_sp` pass `@dry_run = 1` to the SP and skip every commit, logging what would change instead. `_fetch_and_upsert_addresses` does **not** honour it — it always writes.

### Environment variables (`.env`)

| Variable | Used by | Purpose |
|---|---|---|
| `ATS_URL` | `automation_server_client`, `ats_functions` | Base URL for the Automation Server API |
| `ATS_TOKEN` | `automation_server_client`, `ats_functions` | Bearer token for ATS authentication |
| `ATS_WORKQUEUE_OVERRIDE` | `automation_server_client` | Override the workqueue ID (dev/test use) |
| `DBCONNECTIONSTRINGBEFORDRING` | `helpers/db.py` | pyodbc connection string for the SQLAlchemy engine |
| `DBCONNECTIONSTRINGSERVER29` | `_fetch_and_upsert_addresses` | LOIS source server (read; currently validated but not actually connected to — see TODOs) |
| `DBCONNECTIONSTRINGDEV` | `_fetch_and_upsert_addresses` | Used for *both* the source and target connections today |
| `DBCONNECTIONSTRINGPROD` | `_fetch_and_upsert_addresses` | Validated but otherwise unused |
| `API_ENDPOINT` / `API_KEY` | `_calculate_gaaafstand` (commented out) | Walking-distance backend |

`mbu-dev-shared-components` reads a separate `PROD` database connection (`RPAConnection`) for SMTP constants (`Error Email`, `Email Friend`, `smtp_server`, `smtp_port`).

### Outstanding TODOs

- The SSL verification bypass block at the top of `main.py` is marked **REMOVE BEFORE DEPLOYMENT** — it disables certificate checks for every `requests` call in the process.
- `clear_workqueue()` in `main.py` runs before flag dispatch, so it clears the queue even on a `--process`-only run.
- `_fetch_and_upsert_addresses` opens both connections against `DBCONNECTIONSTRINGDEV`, so the LOIS fetch never hits Server 29; its `fetch_sql` is also still capped at `SELECT top (10)`.
- `retrieve_items_for_queue()` only queues `_fetch_and_upsert_addresses`; the `exec_sp` item is commented out and `_calculate_gaaafstand` has no dispatch branch in `process_item()`, so it cannot be reached even if queued.
- `finalize_process()` is an empty stub.
- `uv run ruff check .` and `ruff format --check .` currently fail (E402/F401 in `main.py` and `process_item.py`, three files unformatted).
