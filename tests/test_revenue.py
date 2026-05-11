import unittest
from datetime import datetime
from unittest.mock import patch

from market_report.revenue import update_revenue_tab


class RevenueUpdateTests(unittest.TestCase):
    def test_missing_current_month_marks_current_and_yoy_base_na(self):
        captured_updates = []

        def fake_get_values(_svc, _sheet_id, rng):
            if rng.endswith("!A1:B260"):
                return [[""], [""], ["2330"]]
            return []

        def fake_batch_update(_svc, _sheet_id, updates, value_input="USER_ENTERED"):
            captured_updates.extend(updates)

        with patch("market_report.revenue.fetch_monthly_revenue_maps_all", return_value=((2026, 4), {})):
            update_revenue_tab(
                object(),
                "sheet-id",
                get_values=fake_get_values,
                batch_update_values=fake_batch_update,
                today_taipei=lambda: datetime(2026, 5, 10, 17, 30),
                session=object(),
                user_agent="ua",
                to_float=lambda value: value,
            )

        update_map = {rng: values for rng, values in captured_updates}
        self.assertEqual(update_map["'營收'!C3"], [["N/A"]])
        self.assertEqual(update_map["'營收'!D3"], [["N/A"]])


if __name__ == "__main__":
    unittest.main()
