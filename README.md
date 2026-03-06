# market-report

Current entrypoints:
- `daily_market_report_to_gsheet.py`: the active market report job used by GitHub Actions and Docker.
- `app.py`: HTTP wrapper that triggers the active market report job.
- `notify_server.py`: notification endpoint for uploaded status summaries.
- `status_uploader.py`: uploads run summaries to Cloud Storage.

Project structure:
- `market_report/`: shared modules extracted from the original monolithic script.
- `tests/`: regression tests for shared logic and parsing helpers.
- `legacy/`: historical script variants kept for reference during refactoring. They are not used by current deployment paths.
