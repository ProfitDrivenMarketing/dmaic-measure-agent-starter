from google.cloud import bigquery
from datetime import date
from typing import Dict, List

def get_client_tables(client_id: str) -> Dict[str, str]:
    return {
        "google_ads": f"google_ads_{client_id}",
        "shopify": f"shopify_{client_id}",
    }

def fetch_actuals(bq_project: str, bq_dataset: str, client_id: str,
                  start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    client = bigquery.Client(project=bq_project)
    t = get_client_tables(client_id)
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;

    WITH ads AS (
      SELECT
        DATE(day) AS d,
        SUM(cost) AS cost,
        SUM(conversions_value) AS conv_value
      FROM `{bq_project}.{bq_dataset}.{t['google_ads']}`
      WHERE DATE(day) BETWEEN start_date AND end_date
      GROUP BY d
    ),
    shop AS (
      SELECT
        DATE(order_date) AS d,
        SUM(revenue) AS revenue
      FROM `{bq_project}.{bq_dataset}.{t['shopify']}`
      WHERE DATE(order_date) BETWEEN start_date AND end_date
        AND order_status = 'completed'
      GROUP BY d
    ),
    totals AS (
      SELECT
        SUM(ads.cost) AS total_cost,
        SUM(shop.revenue) AS total_revenue
      FROM ads
      FULL OUTER JOIN shop USING (d)
    )
    SELECT * FROM totals
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start),
                bigquery.ScalarQueryParameter("end_date", "DATE", end),
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        total_cost = 0.0
        total_revenue = 0.0
    else:
        row = rows[0]
        total_cost = float(row.get("total_cost") or 0.0)
        total_revenue = float(row.get("total_revenue") or 0.0)

    out: Dict[str, float] = {}
    if "cost" in metrics: out["cost"] = total_cost
    if "revenue" in metrics: out["revenue"] = total_revenue
    if "roas" in metrics: out["roas"] = (total_revenue / total_cost) if total_cost > 0 else 0.0
    return out
