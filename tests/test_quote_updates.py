import unittest
from datetime import datetime

import pandas as pd

from market_report.quote_updates import build_hk_stock_updates, build_tw_stock_updates, fetch_hk_stock_map


class QuoteUpdateTests(unittest.TestCase):
    def test_build_tw_stock_updates(self):
        updates, missing = build_tw_stock_updates(
            "'tab'",
            [(3, "2330")],
            {"2330": {"close": 10.126, "open": 9.1, "low": 8.2, "high": 10.9, "volume": 12345}},
            {"2330": {"close": 9.876}},
            round_price=lambda value: None if value is None else round(value, 2),
        )
        self.assertEqual(updates[0], ("'tab'!D3", [[10.13]]))
        self.assertEqual(updates[1], ("'tab'!E3", [[9.88]]))
        self.assertEqual(updates[-1], ("'tab'!K3", [[12]]))
        self.assertEqual(missing, [])

    def test_build_hk_stock_updates_prefers_aastocks_volume(self):
        calls = []

        def fake_hands(code):
            calls.append(code)
            return 77

        updates = build_hk_stock_updates(
            "'tab'",
            [(8, "00825")],
            ["0825.HK"],
            {"0825.HK": {"close": 1.2345, "prev_close": 1.2, "open": 1.1, "low": 1.0, "high": 1.3, "volume": 9999}},
            round_price=lambda value: None if value is None else round(value, 3),
            hk_hands_from_aastocks=fake_hands,
        )

        self.assertEqual(calls, ["00825"])
        self.assertEqual(updates[0], ("'tab'!D8", [[1.234]]))
        self.assertEqual(updates[-1], ("'tab'!K8", [[77]]))

    def test_fetch_hk_stock_map_requires_expected_today_date(self):
        history = pd.DataFrame(
            {
                "Open": [9.0, 10.0],
                "High": [9.5, 10.5],
                "Low": [8.5, 9.5],
                "Close": [9.2, 10.2],
                "Volume": [1000, 2000],
            },
            index=pd.to_datetime(["2026-03-05", "2026-03-06"]),
        )

        result = fetch_hk_stock_map(
            ["0825.HK"],
            hist_one=lambda ticker: history,
            expected_today=datetime(2026, 3, 7),
            expected_prev=datetime(2026, 3, 6),
        )

        self.assertEqual(result["0825.HK"], {})


if __name__ == "__main__":
    unittest.main()
