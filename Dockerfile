FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Taipei

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV ENTRYPOINT_PY=daily_market_report_to_gsheet.py
CMD ["bash", "-lc", "python ${ENTRYPOINT_PY}"]
