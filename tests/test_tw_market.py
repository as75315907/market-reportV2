import unittest
from datetime import datetime

from market_report.tw_market import ad_to_twse_date_str, extract_turnover_from_fmtqik, parse_mi_index_map, parse_tpex_st43


class TwMarketTests(unittest.TestCase):
    def test_ad_to_twse_date_str(self):
        self.assertEqual(ad_to_twse_date_str(datetime(2026, 3, 6)), "115/03/06")

    def test_extract_turnover_from_fmtqik(self):
        payload = {"fields": ["日期", "成交金額(元)"], "data": [["115/03/06", "123,456,789"]]}
        self.assertEqual(extract_turnover_from_fmtqik(payload, datetime(2026, 3, 6)), 123456789)

    def test_parse_mi_index_map(self):
        payload = {
            "tables": [
                {
                    "fields": ["證券代號", "開盤", "最高", "最低", "收盤", "成交股數"],
                    "data": [["2330", "1", "2", "0.5", "1.5", "1,000"]],
                }
            ]
        }
        result = parse_mi_index_map(payload, to_float=lambda v: float(str(v).replace(",", "")))
        self.assertEqual(result["2330"]["close"], 1.5)
        self.assertEqual(result["2330"]["volume"], 1000.0)

    def test_parse_tpex_st43(self):
        payload = {"aaData": [["2330", "", "10.5", "", "10.0", "11.0", "9.5", "5,000"]]}
        result = parse_tpex_st43(payload, to_float=lambda v: float(str(v).replace(",", "")))
        self.assertEqual(result["close"], 10.5)
        self.assertEqual(result["volume"], 5000.0)


if __name__ == "__main__":
    unittest.main()
