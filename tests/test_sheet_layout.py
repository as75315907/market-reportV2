import unittest

from market_report.sheet_layout import find_revenue_rows_from_sheet, find_stock_rows_from_sheet


class SheetLayoutTests(unittest.TestCase):
    def test_find_stock_rows_from_sheet(self):
        col_a = [
            "標題",
            "台股",
            "2330",
            "2317",
            "",
            "港股",
            "700",
            "00825.HK",
            "",
        ]
        col_b = [
            "",
            "台幣",
            "",
            "",
            "",
            "港幣",
            "",
            "",
            "",
        ]

        tw_rows, hk_rows = find_stock_rows_from_sheet(col_a, col_b)
        self.assertEqual(tw_rows, [(3, "2330"), (4, "2317")])
        self.assertEqual(hk_rows, [(7, "00700"), (8, "00825")])

    def test_find_revenue_rows_from_sheet(self):
        col_a = ["標題", "代碼", "2330", "2317.TW", "", "2603"]
        self.assertEqual(find_revenue_rows_from_sheet(col_a), [(3, "2330"), (4, "2317")])

    def test_missing_headers_raises(self):
        with self.assertRaises(RuntimeError):
            find_stock_rows_from_sheet(["2330"], [""])


if __name__ == "__main__":
    unittest.main()
