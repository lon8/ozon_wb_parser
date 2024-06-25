from pydantic import BaseModel

class Payload(BaseModel):
    marketplace: str # OZON / WB
    client_id: str
    client_key: str