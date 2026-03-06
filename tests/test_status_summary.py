import unittest

from market_report.status_summary import email_subject_for_result, parse_run_output


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


if __name__ == "__main__":
    unittest.main()
