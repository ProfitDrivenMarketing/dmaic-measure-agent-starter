from __future__ import annotations
from typing import Dict, List
from datetime import date
from google.cloud import bigquery
from google.oauth2 import service_account
import os, json


def _bq_client(project_id: str) -> bigquery.Client:
    """
    Build a BigQuery client using the GCP_SA_JSON env var (service-account JSON).
    If GCP_SA_JSON is missing, credentials=None will fall back to default ADC.
    """
    sa_json = os.getenv("GCP_SA_JSON")
    creds = None
    if sa_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
    return bigquery.Client(project=project_id, credentials=creds)


def fetch_actuals(cfg: Dict[str, str], start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    """
    Fetch actuals for the requested metrics for a given client and date window.

    cfg must contain:
      - bq_project
      - bq_dataset
      - table_prefix

    Assumed schemas (standardized across clients):
      Google Ads table: google_ads_{prefix}
        - Date       (STRING, 'YYYY-MM-DD')
        - Cost       (FLOAT)
      Shopify table:   shopify_{prefix}
        - Date       (STRING, 'YYYY-MM-DD')
        - NetSales   (FLOAT)

    Returns a dict like: { "cost": 123.0, "revenue": 456.0, "roas": 3.7 }
    """
    bq_project = cfg["bq_project"]
    bq_dataset = cfg["bq_dataset"]
    prefix     = cfg["table_prefix"]

    ads_table  = f"{bq_project}.{bq_dataset}.google_ads_{prefix}"
    shop_table = f"{bq_project}.{bq_dataset}.shopify_{prefix}"

    client = _bq_client(bq_project)

    # We PARSE (safely) string dates once, then filter/group.
    # Using SAFE.PARSE_DATE avoids query failure if a row has a bad date; we filter d IS NOT NULL.
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date   DATE DEFAULT @end_date;

    WITH ads_raw AS (
      SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', `Date`) AS d,
        `Cost` AS cost
      FROM `{ads_table}`
    ),
    ads AS (
      SELECT d, SUM(cost) AS total_cost
      FROM ads_raw
      WHERE d IS NOT NULL AND d BETWEEN start_date AND end_date
      GROUP BY d
    ),
    shop_raw AS (
      SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', `Date`) AS d,
        `NetSales` AS revenue
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
    SELECT total_cost, total_revenue FROM totals
    """

    try:
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start),
                bigquery.ScalarQueryParameter("end_date",   "DATE", end),
            ]),
        )
        rows = list(job.result())
    except Exception as e:
        # Bubble up a concise message (helps n8n validation step)
        raise RuntimeError(f"BigQuery query failed for {prefix} ({bq_project}.{bq_dataset}): {e}")

    total_cost     = float((rows[0]["total_cost"] if rows else 0) or 0)
    total_revenue  = float((rows[0]["total_revenue"] if rows else 0) or 0)

    out: Dict[str, float] = {}
    if "cost" in metrics:
        out["cost"] = total_cost
    if "revenue" in metrics:
        out["revenue"] = total_revenue
    if "roas" in metrics:
        out["roas"] = (total_revenue / total_cost) if total_cost > 0 else 0.0

    return out
