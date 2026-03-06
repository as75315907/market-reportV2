import json
import os
import sys
from google.cloud import storage

from market_report.status_summary import parse_run_output
from market_report.time_utils import timestamp_taipei, today_taipei

def main():
    if len(sys.argv) < 2:
        print("usage: python status_uploader.py /tmp/run.log", file=sys.stderr)
        sys.exit(2)

    bucket = os.getenv("STATUS_BUCKET", "").strip()
    if not bucket:
        print("[STATUS] STATUS_BUCKET empty -> skip upload")
        return

    log_path = sys.argv[1]
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    summary = parse_run_output(text)
    payload = {
        "time_taipei": timestamp_taipei(),
        "result": summary.result,
        "skipped": summary.skipped,
        "last_updated_today": summary.last_updated,
        "log_tail": text[-4000:],
    }

    object_name = f"status/{today_taipei()}.json"

    client = storage.Client()
    client.bucket(bucket).blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    print(f"[STATUS] uploaded gs://{bucket}/{object_name}")

if __name__ == "__main__":
    main()
