from fastapi import APIRouter
from src.form import Payload
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple
from src.program import run

router = APIRouter()


@router.post('/req')
def start_programm(data: Payload):
    
    task = (data.marketplace, data.client_id, data.client_key)
    
    with ProcessPoolExecutor() as executor:
        future = executor.submit(run, *task)
        result = future.result()
    
    response = {
        "client_id": data.client_id,
        "client_key": data.client_key,
        "result": result
    }
    
    return response