import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RunSummary:
    result: str
    skipped: bool
    last_updated: str | None


_SKIP_RE = re.compile(r"\[DEDUP\] Already updated today at ([0-9:\-\s]+) -> skip")


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
