from pydantic import BaseModel
class GoldPriceRecord(BaseModel):
    fetched_at: str
    ts: int
    tsj: int
    date: str
    curr: str
    xauPrice: float
    xagPrice: float
    chgXau: float
    chgXag: float
    pcXau: float
    pcXag: float
    xauClose: float
    xagClose: float