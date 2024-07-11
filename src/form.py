from pydantic import BaseModel

class Payload(BaseModel):
    shopName: str
    startDate: str
    endDate: str

class Market(BaseModel):
    name: str
    marketplace: str
    performance_key: str
    performance_secret: str
    client_id: str
    client_key: str
    spreadsheet_url: str

class Markets(BaseModel):
    markets: list[Market]