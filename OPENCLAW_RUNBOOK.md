# OpenClaw Runbook

This document describes how to run and maintain this project with OpenClaw.

## Goal

Run the active market report job and update the target Google Sheet.

Primary production entrypoint:

```bash
python daily_market_report_to_gsheet.py
```

Set `MARKET_REPORT_TASK` to choose what the job updates:

```bash
MARKET_REPORT_TASK=quotes python daily_market_report_to_gsheet.py   # daily stock prices only
MARKET_REPORT_TASK=revenue python daily_market_report_to_gsheet.py  # monthly revenue only
MARKET_REPORT_TASK=all python daily_market_report_to_gsheet.py      # original combined behavior
```

## Workspace

Project root:

```bash
/Users/lushuyan/Documents/Playground/market-reportV2
```

Important files:

- `daily_market_report_to_gsheet.py`: active market report job
- `market_report/`: shared modules used by the active job
- `.env`: local runtime configuration
- `requirements.txt`: Python dependencies
- `tests/`: regression tests
- `app.py`: HTTP wrapper, not needed for normal local scheduled runs

## Required runtime config

The project expects a local `.env` file.

Current required Google Sheets settings:

```env
GSHEET_ID=1euE2xPchT6c8BvEnAXTbcxjd0Ye9bN9YPserNj9hIfw
GSHEET_TAB=IR_updated (PC HOME)
GSHEET_TAB_REVENUE=營收
GOOGLE_APPLICATION_CREDENTIALS=/Users/lushuyan/Desktop/openClaw金鑰.json
```

Optional mail settings:

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_USER=qwe19930408@gmail.com
SMTP_APP_PASSWORD=<app-password>
MAIL_TO=qwe19930408@gmail.com
MAIL_NOTIFY_ON_SKIP=0
```

## Google auth requirement

The service account in `GOOGLE_APPLICATION_CREDENTIALS` must have Google Sheet edit access.

Expected service account:

```text
openclaw@stellar-fx-488510-b0.iam.gserviceaccount.com
```

## Environment rule

Always use a project-local virtual environment.

Do not use the system Python environment on this machine.

Reason:

- system packages are mixed across `arm64` and `x86_64`
- `numpy/pandas` imports can fail in the system environment
- the project has been verified to run correctly inside `.venv`

## Setup

From the project root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install --prefer-binary -r requirements.txt
```

## Test commands

Regression tests:

```bash
python3 -m unittest discover -s tests
```

Syntax check with the local venv:

```bash
.venv/bin/python -m py_compile daily_market_report_to_gsheet.py app.py notify_server.py status_uploader.py market_report/*.py
```

## Full local run

Always load `.env` through `python-dotenv` instead of shell `source`.

Correct full run command:

```bash
.venv/bin/python - <<'PY'
from dotenv import load_dotenv
import runpy
load_dotenv('.env')
runpy.run_path('daily_market_report_to_gsheet.py', run_name='__main__')
PY
```

## Forced run

The script skips repeated same-day runs by checking the sheet timestamp.

To force a full rerun:

```bash
.venv/bin/python - <<'PY'
from dotenv import load_dotenv
import os
import runpy
load_dotenv('.env')
os.environ['FORCE_RUN'] = '1'
runpy.run_path('daily_market_report_to_gsheet.py', run_name='__main__')
PY
```

## Current sheet timestamp behavior

Main sheet timestamp layout:

- `L3`: date, format `YYYY-MM-DD`
- `M3`: time, format `HH:MM:SS`

The dedup logic is backward-compatible with the old format where `L3` stored both date and time.

## Quote date guard

Daily quote updates are strict about source dates.

- TWSE FMTQIK turnover dates are treated as the canonical Taiwan market dates.
- TWSE MI_INDEX quote dates must match the FMTQIK today / previous-trading-day dates.
- yfinance fallback is only allowed when its K-line date exactly matches the expected date.
- If a source returns an older date, the quote job fails or skips that row instead of overwriting the sheet.

For example, on 2026-05-11 17:30 Taipei, the job expects today `2026-05-11` and previous trading day `2026-05-08`. It must not write `2026-05-07` values as the previous trading day.

## Split scheduler behavior

The stock quote and revenue updates can now be scheduled separately.

Recommended production schedules:

- Daily stock prices: call Cloud Run `/run?task=quotes` at 17:30 Taipei on trading weekdays.
- Monthly revenue: call Cloud Run `/run?task=revenue` at 17:30 Taipei on the 10th of each month.

If using the GitHub workflow dispatch bridge, send workflow input `target=quotes` or `target=revenue`.

`MARKET_REPORT_TASK=all` keeps the previous combined run available for manual recovery.

## Revenue tab behavior

The revenue tab can be updated independently with `MARKET_REPORT_TASK=revenue`.

Behavior:

- Reads stock codes from the `營收` sheet
- Pulls monthly revenue from listed, OTC, and emerging market sources
- Writes:
  - `C2`: current dataset month
  - `D2`: same month last year
  - `F2`: previous month
- Writes revenue values into columns `C`, `D`, and `F`
- If a tracked company has not announced yet or the API source is stale, columns `C` and `D` are written as `N/A` so the row is visible for manual MOPS review.

In `MARKET_REPORT_TASK=all`, the original same-day dedup behavior is preserved: if the daily quote job is skipped, revenue is skipped too.

In `MARKET_REPORT_TASK=revenue`, the daily quote dedup check is not used.

## Known runtime notes

- Some TW symbols may return Yahoo Finance 404 or "possibly delisted" messages.
- Those warnings do not necessarily fail the overall run.
- The last verified full local run completed successfully and updated Google Sheets.

## Success criteria

Treat the run as successful if output contains:

```text
DONE: updated Google Sheet
```

Expected additional log lines include:

- `Revenue tab updated: 營收 | month=...`
- `TW rows: ... | HK rows: ...`

If dedup skips the run, expected output includes:

```text
[DEDUP] skipped.txt written. Exit 0.
```

## Guardrails for OpenClaw

- Use `daily_market_report_to_gsheet.py` as the only production job entrypoint
- Do not rely on GitHub Actions for scheduled execution
- Use `.venv` for all Python commands
- Load `.env` via `python-dotenv`
- Run tests after code changes

## 防呆回歸規則（2026-04-21 起）

- 前一交易日判定以 TWSE `MI_INDEX` 報價可用性為主，不可以 `FMTQIK` 成交值作主判定。
- 台股成交量（K欄）只採官方來源（TWSE/TPEx）；官方缺失時寫 `N/A`，不得用 yfinance 補成交量。
- 通知內容必須包含繁體中文缺失清單：`台股成交量缺失清單：...`。
- 每次調整後需 `FORCE_RUN=1` 實跑並確認：
  - `TW dates` 沒有跳過有效交易日。
  - 代表性股票（如 2903）成交量與官方一致。
  - 缺失清單輸出與表內 `N/A` 一致。
