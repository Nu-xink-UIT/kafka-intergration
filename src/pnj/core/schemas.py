from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class PNJRecord(BaseModel):
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
            if not v.strip():
                return 0.0
            return float(v.replace(".", ""))
            # return float(v.replace(",", ""))

        return v



# fetched_at
# Id
# latestDate: "updated_at" of SJC
# TypeName: Vàng SJC 1 lượng / lấy name bên trong gold_type
# BranchName: name bên trong location/ "Hồ Chí Minh"
