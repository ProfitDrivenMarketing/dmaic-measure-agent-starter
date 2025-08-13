from typing import Dict, List
from datetime import date
import os, json, re


def _safe_ident(s: str) -> str:
    """
    Very small guard so a column name/table prefix can't inject SQL.
    Allows letters, digits, underscore only.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
        raise ValueError(f"Invalid identifier: {s}")
    return s


def fetch_actuals(cfg: Dict[str, str], start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    """
    Fetch totals for cost/revenue and compute roas using dynamic column mappings.

    cfg required:
      - bq_project
      - bq_dataset
      - table_prefix

    cfg optional overrides:
      - ads_date_col (default 'Date')
      - ads_cost_col (default 'Cost')
      - ads_cost_divisor (default 1)  # set 1e6 if values are in micros
      - shop_date_col (default 'Date')
      - shop_revenue_col (default 'TotalSales')  # was NetSales before
    """
    bq_project = cfg["bq_project"]
    bq_dataset = cfg["bq_dataset"]
    prefix     = cfg["table_prefix"]

    # Column mappings with safe defaults
    ads_date_col       = _safe_ident(cfg.get("ads_date_col", "Date"))
    ads_cost_col       = _safe_ident(cfg.get("ads_cost_col", "Cost"))
    shop_date_col      = _safe_ident(cfg.get("shop_date_col", "Date"))
    shop_revenue_col   = _safe_ident(cfg.get("shop_revenue_col", "TotalSales"))
    ads_cost_divisor   = float(cfg.get("ads_cost_divisor", 1))

    # Build fully qualified tables (dataset.table)
    ads_table  = f"{bq_project}.{bq_dataset}." + _safe_ident(f"google_ads_{prefix}")
    shop_table = f"{bq_project}.{bq_dataset}." + _safe_ident(f"shopify_{prefix}")

    # Import BigQuery libs INSIDE the function so app import never crashes
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except Exception as e:
        raise RuntimeError(f"BigQuery libraries not available: {e}")

    # Service account creds from env (Railway variable GCP_SA_JSON)
    sa_json = os.getenv("GCP_SA_JSON")
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json)) if sa_json else None
    client = bigquery.Client(project=bq_project, credentials=creds)

    # Use backticks for case-sensitive column names. Do math with a parameterized divisor.
    sql = f"""
    WITH ads_raw AS (
      SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', `{ads_date_col}`) AS d,
        CAST(`{ads_cost_col}` AS FLOAT64) / @cost_divisor AS cost
      FROM `{ads_table}`
    ),
    ads AS (
      SELECT d, SUM(cost) AS total_cost
      FROM ads_raw
      WHERE d IS NOT NULL AND d BETWEEN @start_date AND @end_date
      GROUP BY d
    ),
    shop_raw AS (
      SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', `{shop_date_col}`) AS d,
        CAST(`{shop_revenue_col}` AS FLOAT64) AS revenue
      FROM `{shop_table}`
    ),
    shop AS (
      SELECT d, SUM(revenue) AS total_revenue
      FROM shop_raw
      WHERE d IS NOT NULL AND d BETWEEN @start_date AND @end_date
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
                bigquery.ScalarQueryParameter("cost_divisor", "FLOAT64", ads_cost_divisor),
            ]),
        )
        rows = list(job.result())
    except Exception as e:
        raise RuntimeError(
            f"BigQuery query failed for prefix='{prefix}' "
            f"({bq_project}.{bq_dataset}): {e}"
        )

    total_cost    = float((rows[0]["total_cost"] if rows else 0) or 0)
    total_revenue = float((rows[0]["total_revenue"] if rows else 0) or 0)

    out: Dict[str, float] = {}
    if "cost" in metrics:
        out["cost"] = total_cost
    if "revenue" in metrics:
        out["revenue"] = total_revenue
    if "roas" in metrics:
        out["roas"] = (total_revenue / total_cost) if total_cost > 0 else 0.0
    return out
