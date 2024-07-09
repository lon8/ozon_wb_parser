from fastapi import APIRouter
from src.form import Payload
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple
from src.program import run
from fastapi import Request
import sys

sys.tracebacklimit = 3

router = APIRouter()

@router.options('/api/req')
def f(data: Request):
    return 200

@router.post('/api/req')
async  def start_programm(data: Payload):
    result = run(data.marketplace, data.market, data.performance_key, data.performance_secret, data.client_id, data.client_key, 
        data.startDate, data.endDate)
    
    response = {
        "client_id": data.client_id,
        "client_key": data.client_key,
        "result": result
    }
    
    return response