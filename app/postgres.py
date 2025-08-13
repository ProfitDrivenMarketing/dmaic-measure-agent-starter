"""
Postgres helpers for the Measure Agent.

- engine(): lazy global SQLAlchemy engine using PG_URI
- fetch_targets(): load active targets for a client & period
- fetch_client_bq_config(): read BQ project/dataset/table_prefix from clients.dataslayer_config
- client_exists(), upsert_client_config(), upsert_target(): small utilities for n8n onboarding
"""

from __future__ import annotations

from typing import Dict, List, Tuple, Optional
from datetime import date
from sqlalchemy import create_engine, text
import os
import re

# --------------------
# Engine (lazy, global)
# --------------------
_engine = None

def engine():
    global _engine
    if _engine is None:
        pg_uri = os.environ["PG_URI"]
        _engine = create_engine(pg_uri, pool_pre_ping=True, future=True)
    return _engine


# --------------------
# Internal helpers
# --------------------
def _targets_table_name() -> str:
    """
    Returns the targets table name (defaults to 'targets') and validates it to avoid SQL injection.
    """
    name = os.environ.get("TARGETS_TABLE", "targets")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Invalid TARGETS_TABLE name.")
    return name


# --------------------------------------------
# Measure Agent: read active targets for period
# --------------------------------------------
def fetch_targets(
    client_id: str,
    metrics: List[str],
    start: date,
    end: date,
) -> Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    """
    Returns a dict keyed by metric_name with tuple:
      (target_type, target_value, lower_bound, upper_bound, currency)
    """
    targets_tbl = _targets_table_name()
    sql = text(f"""
        SELECT metric_name, target_type, target_value, lower_bound, upper_bound, currency
        FROM {targets_tbl}
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


# ------------------------------------------------------------
# Read BigQuery config from clients.dataslayer_config (JSONB)
# ------------------------------------------------------------
def fetch_client_bq_config(client_id: str) -> Dict[str, str]:
    """
    Reads project/dataset/table_prefix from clients.dataslayer_config JSON.
    Falls back to env vars BQ_PROJECT_ID / BQ_DATASET when missing.
    """
    sql = text("""
        SELECT
          COALESCE(dataslayer_config->>'project_id', :env_proj) AS bq_project,
          COALESCE(dataslayer_config->>'dataset',    :env_ds)   AS bq_dataset,
          dataslayer_config->>'table_prefix'                     AS table_prefix
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


# -----------------------------
# Optional: onboarding utilities
# -----------------------------
def client_exists(client_id: str) -> bool:
    sql = text("SELECT 1 FROM clients WHERE client_id = :client_id LIMIT 1")
    with engine().connect() as conn:
        return conn.execute(sql, {"client_id": client_id}).first() is not None


def upsert_client_config(
    client_id: str,
    client_name: Optional[str],
    bq_project: str,
    bq_dataset: str,
    table_prefix: str,
) -> None:
    """
    Idempotent upsert of the clients row with dataslayer_config JSON.
    Use this from n8n if you want the API to handle onboarding over HTTP later.
    """
    sql = text("""
        INSERT INTO clients (client_id, client_name, dataslayer_config, updated_at)
        VALUES (:client_id, :client_name,
                jsonb_build_object(
                  'project_id', :bq_project,
                  'dataset',    :bq_dataset,
                  'table_prefix', :table_prefix
                ),
                NOW())
        ON CONFLICT (client_id) DO UPDATE
          SET client_name = EXCLUDED.client_name,
              dataslayer_config = EXCLUDED.dataslayer_config,
              updated_at = NOW();
    """)
    with engine().connect() as conn:
        conn.execute(sql, {
            "client_id": client_id,
            "client_name": client_name,
            "bq_project": bq_project,
            "bq_dataset": bq_dataset,
            "table_prefix": table_prefix,
        })
        conn.commit()


def upsert_target(
    client_id: str,
    metric_name: str,            # 'roas' | 'revenue' | 'cost' | ...
    target_type: str,            # 'MIN' | 'MAX' | 'RANGE'
    period_start: date,
    period_end: date,
    target_value: Optional[float] = None,
    lower_bound: Optional[float] = None,
    upper_bound: Optional[float] = None,
    currency: Optional[str] = None,
    status: str = "ACTIVE",
) -> None:
    """
    Idempotent insert of a target row. You can call this 3x for roas/revenue/cost during onboarding.
    """
    targets_tbl = _targets_table_name()
    sql = text(f"""
        INSERT INTO {targets_tbl}
          (client_id, metric_name, target_type, target_value, lower_bound, upper_bound,
           currency, period_start, period_end, status, created_at)
        VALUES
          (:client_id, :metric_name, :target_type, :target_value, :lower_bound, :upper_bound,
           :currency, :period_start, :period_end, :status, NOW())
        ON CONFLICT DO NOTHING
    """)
    with engine().connect() as conn:
        conn.execute(sql, {
            "client_id": client_id,
            "metric_name": metric_name,
            "target_type": target_type,
            "target_value": target_value,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "currency": currency,
            "period_start": period_start,
            "period_end": period_end,
            "status": status,
        })
        conn.commit()
