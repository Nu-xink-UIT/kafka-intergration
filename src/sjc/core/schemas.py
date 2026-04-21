from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class SJCRecord(BaseModel):
    fetched_at: str
    latestDate: str
    Id: int
    TypeName: str
    BranchName: str
    BuyValue: float
    SellValue: float
    BuyDifferValue: float = 0
    SellDifferValue: float = 0

    @field_validator('BuyValue', 'SellValue', mode='before')
    @classmethod
    def clean_currency(cls, v):
        if isinstance(v, str):
            return float(v.replace(",", ""))
        return v