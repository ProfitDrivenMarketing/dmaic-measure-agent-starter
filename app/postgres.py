def fetch_targets(
    client_id: str,
    metrics: List[str],
    start: date,
    end: date,
) -> Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    """
    Returns a dict keyed by metric_name with tuple:
      (target_type, target_value, lower_bound, upper_bound, currency)

    Backward compatible: if lower/upper/currency don't exist, they return None.
    """
    targets_tbl = _targets_table_name()

    def _map_rows(rows, with_bounds: bool):
        out: Dict[str, Tuple[str, Optional[float], Optional[float], Optional[float], Optional[str]]] = {}
        for r in rows:
            if with_bounds:
                out[r["metric_name"]] = (
                    r["target_type"],
                    r["target_value"],
                    r["lower_bound"],
                    r["upper_bound"],
                    r.get("currency"),
                )
            else:
                out[r["metric_name"]] = (
                    r["target_type"],
                    r["target_value"],
                    None,  # lower_bound
                    None,  # upper_bound
                    None,  # currency
                )
        return out

    params = {
        "client_id": client_id,
        "metrics": metrics,
        "start": start,
        "end": end,
    }

    with engine().connect() as conn:
        # Try new schema (with bounds)
        try:
            sql_new = text(f"""
                SELECT metric_name, target_type, target_value, lower_bound, upper_bound, currency
                FROM {targets_tbl}
                WHERE client_id = :client_id
                  AND metric_name = ANY(:metrics)
                  AND period_start <= :end
                  AND period_end   >= :start
                  AND status = 'ACTIVE'
            """)
            rows = conn.execute(sql_new, params).mappings().all()
            return _map_rows(rows, with_bounds=True)

        except Exception as e:
            # Fall back when columns don't exist
            msg = str(e).lower()
            if "undefinedcolumn" in msg or "does not exist" in msg:
                sql_old = text(f"""
                    SELECT metric_name, target_type, target_value
                    FROM {targets_tbl}
                    WHERE client_id = :client_id
                      AND metric_name = ANY(:metrics)
                      AND period_start <= :end
                      AND period_end   >= :start
                      AND status = 'ACTIVE'
                """)
                rows = conn.execute(sql_old, params).mappings().all()
                return _map_rows(rows, with_bounds=False)
            # Re-raise unexpected errors
            raise
