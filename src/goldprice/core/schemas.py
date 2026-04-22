from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import List

class GoldItem(BaseModel):
    curr: str
    xauPrice: float
    xagPrice: float
    chgXau: float
    chgXag: float
    pcXau: float
    pcXag: float
    xauClose: float
    xagClose: float


class GoldPriceRecod(BaseModel):
    fetched_at: str
    ts: int
    tsj: int
    date: str
    items: List[GoldItem]