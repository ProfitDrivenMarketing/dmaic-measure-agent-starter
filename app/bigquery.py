from typing import Dict, List
from google.cloud import bigquery
from sqlalchemy import create_engine, text
import os

# Global cache for Postgres engine
_pg_engine = None

def pg_engine():
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(os.environ["PG_URI"])
    return _pg_engine

def get_client_config(client_id: str):
    """Fetch table_prefix and dataset for the given client from Postgres."""
    sql = text("""
        SELECT dataset, dataslayer_config->>'table_prefix' AS table_prefix
        FROM clients
        WHERE client_id = :client_id
    """)
    with pg_engine().connect() as conn:
        result = conn.execute(sql, {"client_id": client_id}).mappings().first()
        if not result:
            raise ValueError(f"Client '{client_id}' not found in Postgres.")
        return result["dataset"], result["table_prefix"]

def fetch_actuals(client_id: str, metrics: List[str], start: str, end: str) -> Dict[str, float]:
    """Fetch metrics dynamically for any client using their prefix and dataset."""
    
    dataset, table_prefix = get_client_config(client_id)

    # Build table names dynamically
    google_ads_table = f"{dataset}.google_ads_{table_prefix}"
    shopify_table = f"{dataset}.shopify_{table_prefix}"

    client = bigquery.Client()

    # Example query combining Google Ads + Shopify
    query = f"""
    WITH google_ads AS (
        SELECT
            DATE(Date) AS date,
            SUM(ReturnonadspendROAS) AS roas,
            SUM(ConversionValue) AS revenue,
            SUM(Cost) AS cost
        FROM `{google_ads_table}`
        WHERE DATE(Date) BETWEEN @start_date AND @end_date
        GROUP BY date
    ),
    shopify AS (
        SELECT
            DATE(Date) AS date,
            SUM(TotalSales) AS shopify_sales
        FROM `{shopify_table}`
        WHERE DATE(Date) BETWEEN @start_date AND @end_date
        GROUP BY date
    )
    SELECT
        COALESCE(ga.date, sh.date) AS date,
        roas,
        revenue,
        cost,
        shopify_sales
    FROM google_ads ga
    FULL OUTER JOIN shopify sh ON ga.date = sh.date
    ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end)
        ]
    )

    results = client.query(query, job_config=job_config).result()

    out = {}
    for row in results:
        for metric in metrics:
            if metric in row and row[metric] is not None:
                out[metric] = float(row[metric])
    return out
