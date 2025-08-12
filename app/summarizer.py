from app.schemas import MeasureResponse, MetricEvaluation, KeyInsight

def compute_score(evals):
    score = 100
    for e in evals:
        if e.status == "BELOW_TARGET":
            score -= 20
        elif e.status == "NO_TARGET":
            score -= 5
    return max(0, min(100, score))

def top_insights(evals):
    insights = []
    for e in sorted([x for x in evals if x.target is not None],
                    key=lambda x: abs(x.variance_pct or 0), reverse=True)[:3]:
        msg = f"{e.name.upper()} variance {round((e.variance_pct or 0)*100,2)}%"
        level = "HIGH" if abs(e.variance_pct or 0) > 0.1 else "MEDIUM"
        insights.append(KeyInsight(message=msg, importance=level))
    return insights or [KeyInsight(message="Stable performance across tracked metrics", importance="LOW")]

def overall(evals):
    if any(e.status == "BELOW_TARGET" for e in evals):
        return "AT_RISK"
    if all(e.status in ("MEETS_TARGET", "ABOVE_TARGET", "NO_TARGET") for e in evals):
        return "MEETING_TARGETS"
    return "MEETING_TARGETS"

def summarize(evals):
    score = compute_score(evals)
    ovr = overall(evals)
    insights = top_insights(evals)
    headline = "Performance on track" if ovr == "MEETING_TARGETS" else "Performance at risk"
    exec_sum = f"{headline}. Score {score}. " + "; ".join(i.message for i in insights)
    slack = f"DMAIC • {headline} • Score {score}."
    return score, ovr, insights, exec_sum, slack
