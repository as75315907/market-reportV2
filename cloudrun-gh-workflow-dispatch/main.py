# -*- coding: utf-8 -*-
"""Tiny Cloud Run service: Cloud Scheduler -> Cloud Run -> GitHub workflow_dispatch

Endpoints:
  GET  /healthz          -> "ok"
  POST /dispatch         -> triggers a GitHub Actions workflow via workflow_dispatch

Security:
  - Optional shared secret header: X-CRON-SECRET (recommended)
  - You can also protect the service by restricting invokers (Cloud Run IAM + Scheduler OIDC)

Env vars (Cloud Run):
  GITHUB_TOKEN           (required) GitHub PAT with repo + workflow permissions
  GITHUB_OWNER           (required) e.g. "your-org"
  GITHUB_REPO            (required) e.g. "market-report"
  GITHUB_WORKFLOW_ID     (required) workflow file name or ID, e.g. "market-report.yml"
  GITHUB_REF             (optional) branch/tag, default "main"
  CRON_SECRET            (optional) if set, requests must include header X-CRON-SECRET == this value

Optional JSON payload (POST /dispatch):
  {
    "inputs": { ... }  # workflow_dispatch inputs, if your workflow defines them
  }
"""
import os
import json
import logging
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

GITHUB_API = "https://api.github.com"

def _env(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k)
    if v is None:
        return default
    v = v.strip()
    return v if v else default

def _require_env(k: str) -> str:
    v = _env(k)
    if not v:
        raise RuntimeError(f"Missing env: {k}")
    return v

def _check_secret() -> bool:
    expected = _env("CRON_SECRET")
    if not expected:
        return True
    got = request.headers.get("X-CRON-SECRET", "")
    return got == expected

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.post("/dispatch")
def dispatch():
    if not _check_secret():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    token = _require_env("GITHUB_TOKEN")
    owner = _require_env("GITHUB_OWNER")
    repo = _require_env("GITHUB_REPO")
    workflow_id = _require_env("GITHUB_WORKFLOW_ID")
    ref = _env("GITHUB_REF", "main")

    payload: Dict[str, Any] = {"ref": ref}

    try:
        body = request.get_json(silent=True) or {}
        if isinstance(body, dict):
            inputs = body.get("inputs")
            if isinstance(inputs, dict) and inputs:
                payload["inputs"] = inputs
    except Exception:
        pass

    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "cloudrun-workflow-dispatch",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    if r.status_code == 204:
        return jsonify({"ok": True, "status": 204}), 200

    out = {"ok": False, "status": r.status_code, "url": url}
    try:
        out["github_response"] = r.json()
    except Exception:
        out["github_response_text"] = (r.text or "")[:1000]
    return jsonify(out), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
