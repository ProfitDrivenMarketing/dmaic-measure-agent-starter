from fastapi import FastAPI, HTTPException
from app.models.schemas import MeasureRequest, MeasureResponse
from app.bigquery import fetch_actuals
from app.postgres import fetch_targets, fetch_client_bq_config
from app.evaluator import evaluate_metric
from app.summarizer import summarize

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/measure/evaluate", response_model=MeasureResponse)
def measure(req: MeasureRequest):
    try:
        # 1) get client’s BQ config from Postgres
        cfg = fetch_client_bq_config(req.client_id)

        # 2) fetch actuals from BigQuery (uses cfg, not client_id)
        actuals = fetch_actuals(
            cfg=cfg,
            start=req.period_start,
            end=req.period_end,
            metrics=req.metrics
        )

        # 3) fetch targets from Postgres
        targets = fetch_targets(req.client_id, req.metrics, req.period_start, req.period_end)

        # 4) evaluate + summarize
        evaluations = []
        for m in req.metrics:
            actual = float(actuals.get(m, 0.0))
            evaluations.append(evaluate_metric(m, actual, targets.get(m)))

        score, overall, insights, exec_sum, slack = summarize(evaluations)

        return MeasureResponse(
            overall_status=overall,
            performance_score=score,
            evaluations=evaluations,
            key_insights=insights,
            executive_summary=exec_sum,
            slack_message=slack
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
