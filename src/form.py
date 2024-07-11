from pydantic import BaseModel

class Payload(BaseModel):
    marketplace: str # OZON / WB
    market: str
    startDate: str
    endDate: str

class Market(BaseModel):
    name: str
    performance_key: str
    performance_secret: str
    client_id: str
    client_key: str

class Markets(BaseModel):
    markets: list[Market]