# market-report

Current entrypoints:
- `daily_market_report_to_gsheet.py`: the active market report job used by GitHub Actions and Docker.
- `app.py`: HTTP wrapper that triggers the active market report job.
- `notify_server.py`: notification endpoint for uploaded status summaries.
- `status_uploader.py`: uploads run summaries to Cloud Storage.

Update modes:
- `MARKET_REPORT_TASK=quotes`: update only the daily stock quote sheet.
- `MARKET_REPORT_TASK=revenue`: update only the monthly revenue tab.
- `MARKET_REPORT_TASK=all`: update both, preserving the original behavior.

Cloud Run `/run` also accepts `?task=quotes` / `?task=revenue`, or JSON body `{"task":"quotes"}`.

Project structure:
- `market_report/`: shared modules extracted from the original monolithic script.
- `tests/`: regression tests for shared logic and parsing helpers.
