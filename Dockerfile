FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Cloud Run 會打 HTTP 進來，所以需要一個簡單的 web server
RUN pip install --no-cache-dir flask gunicorn

ENV PORT=8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
