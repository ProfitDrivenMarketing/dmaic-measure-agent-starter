from fastapi import FastAPI, HTTPException
from app.schemas import MeasureRequest, MeasureResponse
from app.bigquery import fetch_actuals
from app.evaluator import evaluate_metric

app = FastAPI()

@app.post("/measure/evaluate", response_model=MeasureResponse)
def measure_evaluate(req: MeasureRequest):
    """
    Dynamic Measure Evaluation
    Pulls metrics from BigQuery based on client_id, date range, and metric list.
    """
    try:
        # Get actuals dynamically from BigQuery
        actuals = fetch_actuals(
            client_id=req.client_id,
            metrics=req.metrics,
            start=req.period_start,
            end=req.period_end
        )

        # Evaluate each metric against targets (if provided)
        results = {}
        for metric in req.metrics:
            target = req.targets.get(metric) if req.targets else None
            results[metric] = evaluate_metric(metric, actuals.get(metric), target)

        return MeasureResponse(
            client_id=req.client_id,
            period_start=req.period_start,
            period_end=req.period_end,
            results=results
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
