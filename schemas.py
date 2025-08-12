from pydantic import BaseModel, Field
from typing import Literal, List, Optional
from datetime import date

class MetricTarget(BaseModel):
    name: str
    target_type: Literal["MIN", "MAX", "RANGE"] = "MIN"
    target_value: float | None = None
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None
    currency: Optional[str] = None

class MetricActual(BaseModel):
    name: str
    value: float
    currency: Optional[str] = None
    as_of: date

class MeasureRequest(BaseModel):
    client_id: str
    period_start: date
    period_end: date
    metrics: List[str] = Field(..., description="['roas','revenue','cost', ...]")

class MetricEvaluation(BaseModel):
    name: str
    actual: float
    target: Optional[float] = None
    variance_abs: Optional[float] = None
    variance_pct: Optional[float] = None
    status: Literal["ABOVE_TARGET","MEETS_TARGET","BELOW_TARGET","NO_TARGET"]
    notes: Optional[str] = None

class KeyInsight(BaseModel):
    message: str
    importance: Literal["HIGH","MEDIUM","LOW"]

class MeasureResponse(BaseModel):
    overall_status: Literal["MEETING_TARGETS","AT_RISK","FAILING","NO_TARGETS"]
    performance_score: float
    evaluations: List[MetricEvaluation]
    key_insights: List[KeyInsight]
    executive_summary: str
    slack_message: str
