import os
import smtplib
from email.mime.text import MIMEText


def send_mail(subject: str, body: str, *, strict: bool = False) -> bool:
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465").strip() or "465")
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_APP_PASSWORD", "").strip()
    recipients_raw = os.getenv("MAIL_TO", "").strip()

    if not all([host, user, password, recipients_raw]):
        if strict:
            raise RuntimeError("Missing SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_APP_PASSWORD/MAIL_TO")
        print("[MAIL] Missing SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_APP_PASSWORD/MAIL_TO -> skip")
        return False

    recipients = [item.strip() for item in recipients_raw.split(",") if item.strip()]
    if not recipients:
        if strict:
            raise RuntimeError("MAIL_TO is empty")
        print("[MAIL] MAIL_TO is empty -> skip")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP_SSL(host, port, timeout=25) as smtp:
        smtp.login(user, password)
        smtp.sendmail(user, recipients, msg.as_string())
    return True
