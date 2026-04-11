import json
import os
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

# 1. PYDANTIC MODEL 
class BinanceRecord(BaseModel):
    fetched_at: str = Field(..., description="Thời gian lấy dữ liệu (ISO 8601)")
    e: Optional[str] = Field(default=None, description="Event type")
    E: Optional[int] = Field(default=None, description="Event time")
    s: str = Field(..., description="Symbol (Bắt buộc, vd: PAXGUSDT)")
    p: Optional[str] = Field(default=None)
    P: Optional[str] = Field(default=None)
    w: Optional[str] = Field(default=None)
    x: Optional[str] = Field(default=None)
    c: Optional[str] = Field(default=None)
    Q: Optional[str] = Field(default=None)
    b: Optional[str] = Field(default=None)
    B: Optional[str] = Field(default=None)
    a: Optional[str] = Field(default=None)
    A: Optional[str] = Field(default=None)
    o: Optional[str] = Field(default=None)
    h: Optional[str] = Field(default=None)
    l: Optional[str] = Field(default=None)
    v: Optional[str] = Field(default=None)
    q: Optional[str] = Field(default=None)
    O: Optional[int] = Field(default=None)
    C: Optional[int] = Field(default=None)
    F: Optional[int] = Field(default=None)
    L: Optional[int] = Field(default=None)
    n: Optional[int] = Field(default=None)

# 2. ĐỌC KAFKA CONNECT SCHEMA TỪ FILE JSON
current_dir = os.path.dirname(os.path.abspath(__file__))
schema_file_path = os.path.join(current_dir, "binance_kafka_schema.json")

with open(schema_file_path, "r", encoding="utf-8") as f:
    KAFKA_BINANCE_SCHEMA = json.load(f)

# 3. HÀM KIỂM DUYỆT
def validate_binance_data(raw_dict: dict) -> dict | None:
    try:
        valid_model = BinanceRecord.model_validate(raw_dict)
        return valid_model.model_dump()
    except ValidationError as err:
        print(f"Dữ liệu Binance bị loại bỏ do sai Schema. Chi tiết: {err.errors()}")
        return None