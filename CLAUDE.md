# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

RPA bot for Aarhus Kommune that runs nightly and catches date-based changes affecting transport reimbursement (*befordring*). It is based on [odense-rpa/process-template](https://github.com/odense-rpa/process-template).

## Commands

```bash
# Install dependencies
uv sync

# Lint
uv run ruff check .
uv run ruff format .

# Run phases individually (requires .env with ATS credentials)
uv run python main.py --queue      # populate workqueue
uv run python main.py --process    # process workqueue items
uv run python main.py --finalize   # finalize process
```

## Architecture

The process runs in three sequential phases, each triggered by a CLI flag:

1. **`--queue`** (`populate_queue`): Calls `retrieve_items_for_queue()` to build a list of `{reference, data}` dicts, deduplicates against existing ATS workqueue items by reference, then bulk-adds new items via `concurrent_add` (asyncio semaphore + exponential backoff).

2. **`--process`** (`process_workqueue`): Iterates the workqueue up to `MAX_RETRY=10` times on `ProcessError`. `BusinessError` marks the item `pending_user` without counting as a retry; `ProcessError` fails the item, sends an error email with screenshot, and increments the retry counter. Calls `startup()`/`close()` around the loop with `reset()` after each recoverable failure.

3. **`--finalize`** (`finalize()`): Runs post-processing cleanup; any failure here sends an email and re-raises.

### Key modules

| Module | Role |
|---|---|
| `processes/queue_handler.py` | `retrieve_items_for_queue()` (stub — needs implementation) and `concurrent_add()` |
| `processes/process_item.py` | `process_item()` (stub — needs implementation) |
| `processes/finalize_process.py` | `finalize_process()` (stub — needs implementation) |
| `processes/application_handler.py` | `startup/close/reset` lifecycle for GUI automation; exposes a global `APP` variable |
| `processes/error_handling.py` | `ErrorContext` dataclass + `handle_error()` + email/screenshot dispatch |
| `helpers/ats_functions.py` | Thin wrappers around the ATS REST API (paginated item listing, item unpacking) |
| `helpers/config.py` | Global tunables: `MAX_RETRY`, `MAX_CONCURRENCY`, `MAX_RETRIES`, `RETRY_BASE_DELAY` |

### Environment variables (`.env`)

| Variable | Purpose |
|---|---|
| `ATS_URL` | Base URL for the Automation Server API |
| `ATS_TOKEN` | Bearer token for ATS authentication |
| `ATS_WORKQUEUE_OVERRIDE` | Override the workqueue ID (dev/test use) |
| `API_ENDPOINT` / `API_KEY` | Local service endpoint used by the process |

`mbu-dev-shared-components` reads a separate `PROD` database connection (`RPAConnection`) for SMTP constants (`Error Email`, `Email Friend`, `smtp_server`, `smtp_port`).

### Outstanding TODOs

- `retrieve_items_for_queue()` in `queue_handler.py` needs to fetch today's date and build two queue items: `{todays_date}_exec_sp` and `{todays_date}_register_data_differences`.
- `process_item()` in `process_item.py` needs the actual business logic.
- The SSL verification bypass block at the top of `main.py` is marked **REMOVE BEFORE DEPLOYMENT**.
