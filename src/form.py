from pydantic import BaseModel
from datetime import datetime

class Payload(BaseModel):
    marketplace: str # OZON / WB
    client_id: str
    client_key: str
    startDate: str
    endDate: str