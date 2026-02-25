# Cloud Run → GitHub workflow_dispatch（超小服務）

目的：讓 **Cloud Scheduler** 打到 **Cloud Run** 的 HTTP endpoint，Cloud Run 再呼叫 GitHub API 觸發 `workflow_dispatch`，達到「不搬家、照跑 GitHub Actions」的排程。

## 部署到 Cloud Run（來源部署）
在此資料夾執行：

```bash
gcloud run deploy gh-workflow-dispatch \
  --source . \
  --region asia-east1 \
  --allow-unauthenticated \
  --set-env-vars GITHUB_OWNER=你的owner,GITHUB_REPO=你的repo,GITHUB_WORKFLOW_ID=market-report.yml,GITHUB_REF=main \
  --set-secrets GITHUB_TOKEN=GITHUB_TOKEN:latest \
  --set-env-vars CRON_SECRET=你自訂的一段隨機字串
```

> 若你不想 `--allow-unauthenticated`，可改成 Cloud Run 只允許 Scheduler 的 service account 呼叫（更安全）。

## 測試
```bash
curl -s https://<YOUR_CLOUD_RUN_URL>/healthz
# ok

curl -s -X POST https://<YOUR_CLOUD_RUN_URL>/dispatch \
  -H "X-CRON-SECRET: 你設定的CRON_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"inputs": {}}'
```

成功回：
```json
{"ok": true, "status": 204}
```

## 建 Cloud Scheduler Job（台北 17:00）
- Target: HTTP
- URL: `https://<YOUR_CLOUD_RUN_URL>/dispatch`
- Method: POST
- Time zone: `Asia/Taipei`
- Schedule: `0 17 * * 1-5`
- Headers:
  - `X-CRON-SECRET: 你設定的CRON_SECRET`
  - `Content-Type: application/json`
- Body: `{"inputs":{}}`（若 workflow 有 inputs 才需要）

## GitHub Token（PAT）建議權限
- repo（private repo 需要）
- workflow（觸發 actions）

## 更嚴格安全（推薦）
1. Cloud Run 不要 allow unauthenticated  
2. Scheduler 用 OIDC 身分（service account）呼叫  
3. Cloud Run IAM：只允許該 service account invoker
