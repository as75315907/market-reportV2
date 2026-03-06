# Legacy Scripts

This directory contains historical one-off variants of the market report job.

They were moved out of the repository root because:
- current deployment paths only execute `daily_market_report_to_gsheet.py`
- keeping many similarly named variants in the root made it unclear which file was authoritative
- the active logic is being consolidated into the shared `market_report/` package

These files are kept as reference only while refactoring continues. They should not be used as new entrypoints unless a specific missing behavior is intentionally restored.
