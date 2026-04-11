import json
import os
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

# 1. PYDANTIC MODEL
class VCBExchangeRateRecord(BaseModel):
    fetched_at: str = Field(..., description="Thời gian lấy dữ liệu (ISO 8601)")
    DateTime: Optional[str] = Field(default=None, description="Thời gian ngân hàng công bố")
    Source: Optional[str] = Field(default=None, description="Nguồn dữ liệu (Vietcombank)")
    CurrencyCode: str = Field(..., description="Mã tiền tệ (Bắt buộc, vd: USD, EUR)")
    CurrencyName: Optional[str] = Field(default=None, description="Tên tiền tệ")
    Buy: Optional[str] = Field(default=None, description="Giá mua")
    Transfer: Optional[str] = Field(default=None, description="Giá chuyển khoản")
    Sell: Optional[str] = Field(default=None, description="Giá bán")

# 2. ĐỌC KAFKA CONNECT SCHEMA TỪ FILE JSON
current_dir = os.path.dirname(os.path.abspath(__file__))
schema_file_path = os.path.join(current_dir, "vcb_kafka_schema.json")

with open(schema_file_path, "r", encoding="utf-8") as f:
    KAFKA_VCB_SCHEMA = json.load(f)

# 3. HÀM KIỂM DUYỆT
def validate_vcb_data(raw_dict: dict) -> dict | None:
    """
    Hàm kiểm duyệt dữ liệu VCB thông qua Pydantic.
    Trả về dict sạch nếu hợp lệ, ngược lại log lỗi và trả về None (Drop).
    """
    try:
        valid_model = VCBExchangeRateRecord.model_validate(raw_dict)
        return valid_model.model_dump()
    except ValidationError as e:
        print(f"Dữ liệu VCB bị loại bỏ do sai Schema. Chi tiết: {e.errors()}")
        return None