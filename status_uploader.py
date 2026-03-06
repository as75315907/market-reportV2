import json, os, re, sys
from datetime import datetime
from google.cloud import storage

def parse_log(text: str):
    result = "success"
    skipped = False
    last_updated = None
    m = re.search(r"\[DEDUP\] Already updated today at ([0-9:\-\s]+) -> skip", text)
    if m:
        result = "skip"
        skipped = True
        last_updated = m.group(1).strip()
    if "Traceback (most recent call last):" in text and result != "skip":
        result = "fail"
    return result, skipped, last_updated

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

    result, skipped, last_updated = parse_log(text)
    payload = {
        "time_taipei": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "result": result,
        "skipped": skipped,
        "last_updated_today": last_updated,
        "log_tail": text[-4000:],
    }

    today = datetime.now().strftime("%Y-%m-%d")
    object_name = f"status/{today}.json"

    client = storage.Client()
    client.bucket(bucket).blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    print(f"[STATUS] uploaded gs://{bucket}/{object_name}")

if __name__ == "__main__":
    main()
