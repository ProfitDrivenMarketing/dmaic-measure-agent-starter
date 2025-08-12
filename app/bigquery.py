import psycopg2
from google.cloud import bigquery
from datetime import date
from typing import Dict, List

def get_client_tables_from_db(client_id: str) -> Dict[str, str]:
    conn = psycopg2.connect(
        host="YOUR_PG_HOST",
        port="YOUR_PG_PORT",
        database="railway",
        user="postgres",
        password="YOUR_PG_PASSWORD"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT google_ads_table, shopify_table, bq_project, bq_dataset
        FROM clients
        WHERE client_id = %s
    """, (client_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise ValueError(f"No client found with ID: {client_id}")
    
    return {
        "google_ads": row[0],
        "shopify": row[1],
        "bq_project": row[2],
        "bq_dataset": row[3]
    }

def fetch_actuals(client_id: str, start: date, end: date, metrics: List[str]) -> Dict[str, float]:
    tables = get_client_tables_from_db(client_id)
    client = bigquery.Client(project=tables["bq_project"])
    
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;

    WITH ads AS (
        SELECT DATE(day) AS d, SUM(cost) AS cost, SUM(conversions_value) AS conv_value
        FROM `{tables["bq_project"]}.{tables["bq_dataset"]}.{tables["google_ads"]}`
        WHERE DATE(day) BETWEEN start_date AND end_date
        GROUP BY d
    ),
    shop AS (
        SELECT DATE(day) AS d, SUM(total_sales) AS revenue
        FROM `{tables["bq_project"]}.{tables["bq_dataset"]}.{tables["shopify"]}`
        WHERE DATE(day) BETWEEN start_date AND end_date
        GROUP BY d
    )
    SELECT * FROM ads
    JOIN shop USING(d);
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
    results = list(job.result())
    return results
