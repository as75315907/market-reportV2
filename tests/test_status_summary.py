import unittest

from market_report.status_summary import build_notification_text, email_subject_for_result, parse_run_output


class ParseRunOutputTests(unittest.TestCase):
    def test_skip_log_is_classified(self):
        summary = parse_run_output("[DEDUP] Already updated today at 2026-03-06 09:00:00 -> skip")
        self.assertEqual(summary.result, "skip")
        self.assertTrue(summary.skipped)
        self.assertEqual(summary.last_updated, "2026-03-06 09:00:00")

    def test_traceback_is_failure(self):
        summary = parse_run_output("Traceback (most recent call last):\nboom")
        self.assertEqual(summary.result, "fail")
        self.assertFalse(summary.skipped)
        self.assertIsNone(summary.last_updated)

    def test_timeout_is_failure(self):
        summary = parse_run_output("[TIMEOUT] daily_market_report_to_gsheet.py timeout")
        self.assertEqual(summary.result, "fail")

    def test_plain_output_is_success(self):
        summary = parse_run_output("job finished")
        self.assertEqual(summary.result, "success")
        self.assertFalse(summary.skipped)


class EmailSubjectTests(unittest.TestCase):
    def test_subject_mapping(self):
        self.assertEqual(email_subject_for_result("success"), "✅Daily Market Report 更新完成")
        self.assertEqual(email_subject_for_result("skip"), "🟨Daily Market Report（略過：今日已更新）")
        self.assertEqual(email_subject_for_result("fail"), "❌Daily Market Report 更新失敗")


class NotificationTextTests(unittest.TestCase):
    def test_quote_success_message_is_readable_traditional_chinese(self):
        text = "\n".join(
            [
                "[TASK] MARKET_REPORT_TASK=quotes",
                "TW rows: 12 | HK rows: 8",
                "TW dates: 2026-05-11 / 2026-05-08",
                "TWII turnover (today/prev, 億元): 3210.55 / 2988.12",
                "HK turnover (today/prev, 億港幣): 1450.33 / 1322.45",
                "港股因日期或資料缺失略過清單：無",
                "台股因日期或資料缺失略過清單：2903",
                "台股成交量缺失清單：5903",
                "DONE: updated quote sheet",
            ]
        )

        message = build_notification_text(text, time_taipei="2026-05-11 17:32:10 CST")

        self.assertIn("✅ 每日股價更新完成", message)
        self.assertIn("台股交易日：今日 2026-05-11，前一交易日 2026-05-08", message)
        self.assertIn("台股略過清單：2903", message)
        self.assertIn("台股成交量缺失：5903", message)

    def test_revenue_success_message_mentions_na_manual_review(self):
        text = "\n".join(
            [
                "[TASK] MARKET_REPORT_TASK=revenue",
                "Revenue fallback summary | month=2026-04 | needed=3 ok=1 fail=2",
                "Revenue fallback failed codes: 2330, 2317",
                "Revenue tab updated: 營收 | month=2026-04 | rows=12",
                "Revenue missing codes: 2330",
                "DONE: updated revenue sheet",
            ]
        )

        message = build_notification_text(text, time_taipei="2026-05-10 17:32:10 CST")

        self.assertIn("✅ 每月營收更新完成", message)
        self.assertIn("營收月份：2026-04", message)
        self.assertIn("營收缺值公司：2330", message)
        self.assertIn("公開觀測資訊站", message)

    def test_failure_message_explains_no_overwrite_for_date_guard(self):
        text = "\n".join(
            [
                "[TASK] MARKET_REPORT_TASK=quotes",
                "Traceback (most recent call last):",
                "RuntimeError: TW date guard failed: quote prev date 2026-05-07 != turnover prev date 2026-05-08; skip writing quotes",
            ]
        )

        message = build_notification_text(text, time_taipei="2026-05-11 17:32:10 CST")

        self.assertIn("❌ 每日股價更新失敗", message)
        self.assertIn("錯誤原因：TW date guard failed", message)
        self.assertIn("本次沒有覆蓋表格", message)


if __name__ == "__main__":
    unittest.main()
