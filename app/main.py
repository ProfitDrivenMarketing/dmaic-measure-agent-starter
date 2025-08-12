import os
from fastapi import FastAPI, HTTPException

from app.schemas import MeasureRequest, MeasureResponse
from app.bigquery import fetch_actuals
from app.postgres import fetch_targets, fetch_client_bq_config   # <- add this
from app.evaluator import evaluate_metric
from app.summarizer import summarize

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/measure/evaluate", response_model=MeasureResponse)
def measure(req: MeasureRequest):
    try:
        # Look up this client's BigQuery config (project, dataset, table_prefix)
        cfg = fetch_client_bq_config(req.client_id)

        # Pull actuals from BigQuery using the dynamic tables for this client
        actuals = fetch_actuals(
            cfg=cfg,
            start=req.period_start,
            end=req.period_end,
            metrics=req.metrics
        )

        # Load targets from Postgres
        targets = fetch_targets(req.client_id, req.metrics, req.period_start, req.period_end)

        # Evaluate metrics against targets
        evaluations = []
        for m in req.metrics:
            actual = float(actuals.get(m, 0.0))
            evaln = evaluate_metric(m, actual, targets.get(m))
            evaluations.append(evaln)

        # Summarize
        score, ovr, insights, exec_sum, slack = summarize(evaluations)
        return MeasureResponse(
            overall_status=ovr,
            performance_score=score,
            evaluations=evaluations,
            key_insights=insights,
            executive_summary=exec_sum,
            slack_message=slack
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
