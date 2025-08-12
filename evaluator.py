from app.models.schemas import MetricEvaluation

def evaluate_metric(name: str, actual: float, target_def) -> MetricEvaluation:
    if not target_def:
        return MetricEvaluation(
            name=name, actual=actual, status="NO_TARGET", notes="No active target in period"
        )

    target_type, target_value, lower, upper, _currency = target_def
    if target_type == "MIN":
        variance_abs = actual - float(target_value)
        variance_pct = (variance_abs / float(target_value)) if target_value else None
        status = "MEETS_TARGET" if actual >= target_value else "BELOW_TARGET"
        if variance_pct is not None and variance_pct > 0.03:
            status = "ABOVE_TARGET"
        return MetricEvaluation(
            name=name, actual=actual, target=target_value,
            variance_abs=variance_abs, variance_pct=variance_pct, status=status
        )

    if target_type == "MAX":
        variance_abs = actual - float(target_value)
        variance_pct = (variance_abs / float(target_value)) if target_value else None
        status = "MEETS_TARGET" if actual <= target_value else "BELOW_TARGET"
        if variance_pct is not None and variance_pct < -0.03:
            status = "ABOVE_TARGET"
        return MetricEvaluation(
            name=name, actual=actual, target=target_value,
            variance_abs=variance_abs, variance_pct=variance_pct, status=status
        )

    if target_type == "RANGE":
        in_range = (lower is not None and upper is not None and lower <= actual <= upper)
        status = "MEETS_TARGET" if in_range else "BELOW_TARGET"
        if not in_range:
            nearest = lower if actual < lower else upper
            variance_abs = actual - float(nearest)
            variance_pct = variance_abs / float(nearest) if nearest else None
        else:
            variance_abs = variance_pct = 0.0
        return MetricEvaluation(
            name=name, actual=actual, target=None,
            variance_abs=variance_abs, variance_pct=variance_pct, status=status
        )

    return MetricEvaluation(name=name, actual=actual, status="NO_TARGET")
