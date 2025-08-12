from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date
from typing import Dict, List
import os, json

def fetch_actuals(cfg: Dict[str, str], start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    bq_project = cfg["bq_project"]
    bq_dataset = cfg["bq_dataset"]
    prefix     = cfg["table_prefix"]

    ads_table  = f"google_ads_{prefix}"
    shop_table = f"shopify_{prefix}"

    sa = os.getenv("GCP_SA_JSON")
    creds = service_account.Credentials.from_service_account_info(json.loads(sa)) if sa else None
    client = bigquery.Client(project=bq_project, credentials=creds)

    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;

    WITH ads AS (
      SELECT DATE(day) AS d, SUM(cost) AS cost
      FROM `{bq_project}.{bq_dataset}.{ads_table}`
      WHERE DATE(day) BETWEEN start_date AND end_date
      GROUP BY d
    ),
    shop AS (
      SELECT DATE(order_date) AS d, SUM(revenue) AS revenue
      FROM `{bq_project}.{bq_dataset}.{shop_table}`
      WHERE DATE(order_date) BETWEEN start_date AND end_date
        AND order_status = 'completed'
      GROUP BY d
    ),
    totals AS (
      SELECT SUM(ads.cost) AS total_cost, SUM(shop.revenue) AS total_revenue
      FROM ads FULL OUTER JOIN shop USING (d)
    )
    SELECT * FROM totals
    """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ]),
    )
    rows = list(job.result())
    total_cost = float((rows[0]["total_cost"] if rows else 0) or 0)
    total_revenue = float((rows[0]["total_revenue"] if rows else 0) or 0)

    out: Dict[str, float] = {}
    if "cost" in metrics: out["cost"] = total_cost
    if "revenue" in metrics: out["revenue"] = total_revenue
    if "roas" in metrics: out["roas"] = (total_revenue / total_cost) if total_cost > 0 else 0.0
    return out
