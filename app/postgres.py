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

def fetch_targets(client_id: str, metrics: List[str],
                  start: date, end: date) -> Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    sql = text(f"""
        SELECT metric_name, target_type, target_value, lower_bound, upper_bound, currency
        FROM {os.environ['TARGETS_TABLE']}
        WHERE client_id = :client_id
          AND metric_name = ANY(:metrics)
          AND period_start <= :end
          AND period_end >= :start
          AND status = 'ACTIVE'
    """)
    with engine().connect() as conn:
        rows = conn.execute(sql, {
            "client_id": client_id,
            "metrics": metrics,
            "start": start,
            "end": end
        }).mappings().all()
    out = {}
    for r in rows:
        out[r["metric_name"]] = (
            r["target_type"], r["target_value"], r["lower_bound"], r["upper_bound"], r.get("currency")
        )
    return out
