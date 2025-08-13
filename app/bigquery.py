from typing import Dict, List
from datetime import date
import os, json


def fetch_actuals(cfg: Dict[str, str], start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    """
    Fetch totals for cost/revenue and compute roas.

    cfg keys required:
      - bq_project
      - bq_dataset
      - table_prefix

    BigQuery tables (standardized):
      - {dataset}.google_ads_{prefix}  with columns: `Date` (YYYY-MM-DD string), `Cost`
      - {dataset}.shopify_{prefix}     with columns: `Date` (YYYY-MM-DD string), `NetSales`
    """
    bq_project = cfg["bq_project"]
    bq_dataset = cfg["bq_dataset"]
    prefix     = cfg["table_prefix"]

    ads_table  = f"{bq_project}.{bq_dataset}.google_ads_{prefix}"
    shop_table = f"{bq_project}.{bq_dataset}.shopify_{prefix}"

    # Import BigQuery libs INSIDE the function so module import never crashes
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except Exception as e:
        raise RuntimeError(f"BigQuery libraries not available: {e}")

    # Service account creds from env (Railway variable GCP_SA_JSON)
    sa_json = os.getenv("GCP_SA_JSON")
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json)) if sa_json else None
    client = bigquery.Client(project=bq_project, credentials=creds)

    # Build query (use SAFE.PARSE_DATE and backticks for case-sensitive cols)
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date   DATE DEFAULT @end_date;

    WITH ads_raw AS (
      SELECT SAFE.PARSE_DATE('%Y-%m-%d', `Date`) AS d, `Cost` AS cost
      FROM `{ads_table}`
    ),
    ads AS (
      SELECT d, SUM(cost) AS total_cost
      FROM ads_raw
      WHERE d IS NOT NULL AND d BETWEEN start_date AND end_date
      GROUP BY d
    ),
    shop_raw AS (
      SELECT SAFE.PARSE_DATE('%Y-%m-%d', `Date`) AS d, `NetSales` AS revenue
      FROM `{shop_table}`
    ),
    shop AS (
      SELECT d, SUM(revenue) AS total_revenue
      FROM shop_raw
      WHERE d IS NOT NULL AND d BETWEEN start_date AND end_date
      GROUP BY d
    ),
    totals AS (
      SELECT
        SUM(ads.total_cost)     AS total_cost,
        SUM(shop.total_revenue) AS total_revenue
      FROM ads
      FULL OUTER JOIN shop USING (d)
    )
    SELECT total_cost, total_revenue
    FROM totals
    """

    try:
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start),
                bigquery.ScalarQueryParameter("end_date",   "DATE", end),
            ])
        )
        rows = list(job.result())
    except Exception as e:
        raise RuntimeError(f"BigQuery query failed for prefix='{prefix}' "
                           f"({bq_project}.{bq_dataset}): {e}")

    total_cost = float((rows[0]["total_cost"] if rows else 0) or 0)
    total_revenue = float((rows[0]["total_revenue"] if rows else 0) or 0)

    out: Dict[str, float] = {}
    if "cost" in metrics:
        out["cost"] = total_cost
    if "revenue" in metrics:
        out["revenue"] = total_revenue
    if "roas" in metrics:
        out["roas"] = (total_revenue / total_cost) if total_cost > 0 else 0.0
    return out
