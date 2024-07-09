from pydantic import BaseModel
from datetime import datetime

class Payload(BaseModel):
    marketplace: str # OZON / WB
    market: str
    performance_key: str
    performance_secret: str
    client_id: str
    client_key: str
    startDate: str
    endDate: str