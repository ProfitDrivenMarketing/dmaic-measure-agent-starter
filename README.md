# DMAIC Measure Agent (Starter)

This is a ready-to-deploy starter for the Measure Agent using FastAPI + Pydantic,
with adapters for BigQuery (actuals) and Postgres (targets).

## What you need before deploying
1) A BigQuery project & dataset that contain per-client tables, e.g.:
   - `marketing_data.google_ads_<client_id>` with columns: `day`, `cost`, `conversions_value`
   - `marketing_data.shopify_<client_id>` with columns: `order_date`, `revenue`, `order_status` ('completed')
2) A Postgres DB with a `targets` table that has:
   - `client_id, metric_name, target_type, target_value, lower_bound, upper_bound, currency, period_start, period_end, status`

> If your column names differ, edit `app/adapters/bigquery.py` SQL once.

## Local run (optional)
```bash
pip install -r requirements.txt
export BQ_PROJECT_ID=your_project
export BQ_DATASET=marketing_data
export PG_URI=postgresql+psycopg2://USER:PASS@HOST:PORT/DB
export TARGETS_TABLE=targets
uvicorn app.main:app --reload --port 8080
```

Check health:
```
GET http://localhost:8080/health
```

Test evaluate (use REST client or curl):
```
POST http://localhost:8080/measure/evaluate
{
  "client_id": "recommerceit",
  "period_start": "2025-07-01",
  "period_end": "2025-07-31",
  "metrics": ["roas","revenue","cost"]
}
```

## Deploy to Railway (easiest path)
1. Create a new project on Railway → Deploy from GitHub (or upload this zip).
2. Set Environment Variables in Railway:
   - `BQ_PROJECT_ID`
   - `BQ_DATASET` (e.g., `marketing_data`)
   - `PG_URI` (full connection string)
   - `TARGETS_TABLE` (e.g., `targets`)
3. Railway will build using the Dockerfile and expose a public URL.
4. Hit `/health` on the public URL to verify.

## Wire up n8n (simple)
- HTTP Request (POST) to `<railway-url>/measure/evaluate`
- Body:
```json
{
  "client_id": "recommerceit",
  "period_start": "2025-07-01",
  "period_end": "2025-07-31",
  "metrics": ["roas","revenue","cost"]
}
```
- Route on `overall_status`. Post to Slack or create a task if `AT_RISK`.

## Customising for your schema
- Edit `app/adapters/bigquery.py` to match your table and column names.
- Edit `app/adapters/postgres.py` if your targets table name differs.

## Notes
- ROAS is computed as `revenue / cost` when cost > 0, else 0.
- If no active target exists for a metric, status returns `NO_TARGET`.
- Adjust the tolerance rules in `app/core/evaluator.py` if desired.
