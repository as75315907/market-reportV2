import unittest
from datetime import datetime
from unittest.mock import patch

import daily_market_report_to_gsheet as job


class TradeDateGuardTests(unittest.TestCase):
    def test_resolve_tw_trade_dates_requires_quote_prev_to_match_turnover_prev(self):
        base = datetime(2026, 5, 11, 17, 30)
        with patch.object(
            job,
            "_scan_tw_turnover_trade_dates",
            return_value=[datetime(2026, 5, 11), datetime(2026, 5, 8)],
        ), patch.object(
            job,
            "_scan_tw_quote_trade_dates",
            return_value=[datetime(2026, 5, 11), datetime(2026, 5, 7)],
        ):
            with self.assertRaisesRegex(RuntimeError, "quote prev date 2026-05-07 != turnover prev date 2026-05-08"):
                job._resolve_tw_trade_dates(object(), base)

    def test_resolve_tw_trade_dates_uses_turnover_dates_when_sources_match(self):
        base = datetime(2026, 5, 11, 17, 30)
        with patch.object(
            job,
            "_scan_tw_turnover_trade_dates",
            return_value=[datetime(2026, 5, 11), datetime(2026, 5, 8)],
        ), patch.object(
            job,
            "_scan_tw_quote_trade_dates",
            return_value=[datetime(2026, 5, 11), datetime(2026, 5, 8)],
        ):
            today, prev = job._resolve_tw_trade_dates(object(), base)

        self.assertEqual(today.date().isoformat(), "2026-05-11")
        self.assertEqual(prev.date().isoformat(), "2026-05-08")


if __name__ == "__main__":
    unittest.main()
