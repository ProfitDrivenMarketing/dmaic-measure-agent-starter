from typing import Dict, List, Tuple, Optional
from datetime import date
from sqlalchemy import create_engine, text
import os

_engine = None

def engine():
    global _engine
    if _engine is None:
        _engine = create_engine(os.environ["PG_URI"])
    return _engine


# --- USED BY Measure Agent: read active targets for a client/period ---
def fetch_targets(
    client_id: str,
    metrics: List[str],
    start: date,
    end: date,
) -> Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    """
    Returns a dict keyed by metric_name with:
      (target_type, target_value, lower_bound, upper_bound, currency)
    """
    sql = text(f"""
        SELECT metric_name, target_type, target_value, lower_bound, upper_bound, currency
        FROM {os.environ['TARGETS_TABLE']}
        WHERE client_id = :client_id
          AND metric_name = ANY(:metrics)
          AND period_start <= :end
          AND period_end   >= :start
          AND status = 'ACTIVE'
    """)
    with engine().connect() as conn:
        rows = conn.execute(sql, {
            "client_id": client_id,
            "metrics": metrics,
            "start": start,
            "end": end
        }).mappings().all()

    out: Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]] = {}
    for r in rows:
        out[r["metric_name"]] = (
            r["target_type"],
            r["target_value"],
            r["lower_bound"],
            r["upper_bound"],
            r.get("currency"),
        )
    return out


# --- NEW: read BigQuery config for this client from clients.dataslayer_config ---
def fetch_client_bq_config(client_id: str) -> Dict[str, str]:
    """
    Reads project/dataset/table_prefix from the clients table’s dataslayer_config JSON.
    Falls back to env vars for project/dataset if not present.
    """
    sql = text("""
        SELECT
          COALESCE(dataslayer_config->>'project_id',  :env_proj)   AS bq_project,
          COALESCE(dataslayer_config->>'dataset',     :env_ds)     AS bq_dataset,
          dataslayer_config->>'table_prefix'                         AS table_prefix
        FROM clients
        WHERE client_id = :client_id
        LIMIT 1
    """)
    with engine().connect() as conn:
        row = conn.execute(sql, {
            "client_id": client_id,
            "env_proj": os.environ.get("BQ_PROJECT_ID"),
            "env_ds":   os.environ.get("BQ_DATASET"),
        }).mappings().first()

    if not row:
        raise ValueError(f"No client row found for client_id={client_id}")

    if not row["table_prefix"]:
        raise ValueError(f"clients.dataslayer_config.table_prefix missing for client_id={client_id}")

    return {
        "bq_project": row["bq_project"],
        "bq_dataset": row["bq_dataset"],
        "table_prefix": row["table_prefix"],
    }
