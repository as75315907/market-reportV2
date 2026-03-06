import unittest

from market_report.hk_market import normalize_hk_turnover_to_yi, parse_aastocks_turnover_yi, parse_hkex_turnover_hkd


class HkMarketTests(unittest.TestCase):
    def test_parse_hkex_turnover_hkd(self):
        html = "Total Market Turnover ( HK$ Million ) 12,345.5"
        result = parse_hkex_turnover_hkd(html, to_float=lambda v: float(str(v).replace(",", "")))
        self.assertEqual(result, 12345500000.0)

    def test_parse_aastocks_turnover_yi(self):
        self.assertEqual(parse_aastocks_turnover_yi("成交額 12.5 億"), 12.5)
        self.assertEqual(parse_aastocks_turnover_yi("Turnover 3.2 B"), 32.0)

    def test_normalize_hk_turnover_to_yi(self):
        self.assertEqual(normalize_hk_turnover_to_yi(1230000000), 12.3)


if __name__ == "__main__":
    unittest.main()
