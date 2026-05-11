import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RunSummary:
    result: str
    skipped: bool
    last_updated: str | None


_SKIP_RE = re.compile(r"\[DEDUP\] Already updated today at ([0-9:\-\s]+) -> skip")
_TASK_RE = re.compile(r"\[TASK\]\s+MARKET_REPORT_TASK=([a-z]+)")
_TW_DATES_RE = re.compile(r"TW dates:\s*([0-9\-]+)\s*/\s*([0-9\-]+)")
_TW_TURNOVER_RE = re.compile(r"TWII turnover \(today/prev, 億元\):\s*([^\n]+)")
_HK_TURNOVER_RE = re.compile(r"HK turnover \(today/prev, 億港幣\):\s*([^\n]+)")
_ROWS_RE = re.compile(r"TW rows:\s*(\d+)\s*\|\s*HK rows:\s*(\d+)")
_REVENUE_UPDATED_RE = re.compile(r"Revenue tab updated:\s*(.+?)\s*\|\s*month=([0-9]{4}-[0-9]{2})\s*\|\s*rows=(\d+)")
_REVENUE_FALLBACK_RE = re.compile(r"Revenue fallback summary \| month=([0-9]{4}-[0-9]{2}) \| needed=(\d+) ok=(\d+) fail=(\d+)")


def _match_group(pattern: re.Pattern, text: str, group: int = 1) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return match.group(group).strip()


def _line_value(text: str, prefix: str) -> str | None:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line.split("：", 1)[1].strip() if "：" in line else line.split(":", 1)[1].strip()
    return None


def _extract_error_reason(text: str) -> str | None:
    for pattern in (
        r"RuntimeError:\s*(.+)",
        r"\[TIMEOUT\]\s*(.+)",
        r"ValueError:\s*(.+)",
        r"Exception:\s*(.+)",
    ):
        matches = re.findall(pattern, text)
        if matches:
            return matches[-1].strip()
    return None


def task_label(task: str | None) -> str:
    return {
        "quotes": "每日股價",
        "revenue": "每月營收",
        "all": "股價與營收",
    }.get(task or "", "市場報表")


def parse_run_output(text: str) -> RunSummary:
    match = _SKIP_RE.search(text)
    if match:
        return RunSummary(result="skip", skipped=True, last_updated=match.group(1).strip())

    if "[TIMEOUT]" in text or "Traceback (most recent call last):" in text:
        return RunSummary(result="fail", skipped=False, last_updated=None)

    return RunSummary(result="success", skipped=False, last_updated=None)


def email_subject_for_result(result: str) -> str:
    if result == "success":
        return "✅Daily Market Report 更新完成"
    if result == "skip":
        return "🟨Daily Market Report（略過：今日已更新）"
    return "❌Daily Market Report 更新失敗"


def notification_title_for_result(result: str, task: str | None = None) -> str:
    name = task_label(task)
    if result == "success":
        return f"✅ {name}更新完成"
    if result == "skip":
        return f"🟨 {name}略過：今日已更新"
    return f"❌ {name}更新失敗"


def build_notification_text(text: str, *, time_taipei: str | None = None, include_log_tail: bool = False) -> str:
    summary = parse_run_output(text)
    task = _match_group(_TASK_RE, text) or "all"
    lines: list[str] = [notification_title_for_result(summary.result, task)]

    if time_taipei:
        lines.append(f"執行時間：{time_taipei}")

    if summary.result == "skip":
        if summary.last_updated:
            lines.append(f"原因：今天已在 {summary.last_updated} 更新過。")
        else:
            lines.append("原因：今天已更新過。")
        return "\n".join(lines)

    if summary.result == "fail":
        reason = _extract_error_reason(text)
        if reason:
            lines.append(f"錯誤原因：{reason}")
        else:
            lines.append("錯誤原因：請查看原始執行紀錄。")

        if "date guard failed" in text:
            lines.append("處理方式：本次沒有覆蓋表格，避免寫入錯誤日期的資料。")
        elif "[TIMEOUT]" in text:
            lines.append("處理方式：任務逾時，建議稍後重新執行。")

        if include_log_tail:
            lines.append("")
            lines.append("原始紀錄：")
            lines.append(text[-1200:])
        return "\n".join(lines)

    if task in ("quotes", "all") and ("TW dates:" in text or "TWII turnover" in text):
        rows = _ROWS_RE.search(text)
        if rows:
            lines.append(f"更新檔數：台股 {rows.group(1)} 檔、港股 {rows.group(2)} 檔")

        tw_dates = _TW_DATES_RE.search(text)
        if tw_dates:
            lines.append(f"台股交易日：今日 {tw_dates.group(1)}，前一交易日 {tw_dates.group(2)}")

        tw_turnover = _match_group(_TW_TURNOVER_RE, text)
        if tw_turnover:
            lines.append(f"台股大盤成交值：{tw_turnover}")

        hk_turnover = _match_group(_HK_TURNOVER_RE, text)
        if hk_turnover:
            lines.append(f"港股大盤成交值：{hk_turnover}")

        hk_skipped = _line_value(text, "港股因日期或資料缺失略過清單")
        tw_skipped = _line_value(text, "台股因日期或資料缺失略過清單")
        tw_missing_volume = _line_value(text, "台股成交量缺失清單")
        lines.append(f"港股略過清單：{hk_skipped or '無'}")
        lines.append(f"台股略過清單：{tw_skipped or '無'}")
        lines.append(f"台股成交量缺失：{tw_missing_volume or '無'}")

    if task in ("revenue", "all") and "Revenue tab updated:" in text:
        revenue = _REVENUE_UPDATED_RE.search(text)
        if revenue:
            lines.append(f"營收分頁：{revenue.group(1)}")
            lines.append(f"營收月份：{revenue.group(2)}")
            lines.append(f"追蹤公司數：{revenue.group(3)}")

        fallback = _REVENUE_FALLBACK_RE.search(text)
        if fallback:
            lines.append(
                f"MOPS單公司補抓：需要 {fallback.group(2)} 檔，成功 {fallback.group(3)} 檔，失敗 {fallback.group(4)} 檔"
            )

        missing = _line_value(text, "Revenue missing codes")
        stale = _line_value(text, "Revenue stale codes")
        failed = _line_value(text, "Revenue fallback failed codes")
        lines.append(f"營收缺值公司：{missing or failed or '無'}")
        lines.append(f"營收月份不符公司：{stale or '無'}")
        if missing or stale or failed:
            lines.append("提醒：表格中的 N/A 代表 API 尚未取得，請到公開觀測資訊站人工確認。")

    return "\n".join(lines)
