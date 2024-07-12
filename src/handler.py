from fastapi import APIRouter, BackgroundTasks
from src.form import *
from src.program import run
from fastapi import Request
import sys
import json
import os

sys.tracebacklimit = 3

router = APIRouter()

@router.options('/api/req')
def f(data: Request):
    return 200

@router.post('/api/req')
async  def start_programm(data: Payload, background_tasks: BackgroundTasks):
    if not os.path.exists('markets.json'):
        return {'ok': False, 'status': 400, 'error': 'No markets data'}

    market = None

    with open('markets.json') as f:
        markets = json.load(f)['markets']

        for m in markets:
            if m['name'] == data.shopName:
                market = m

    if not market:
        return {'ok': False, 'status': 401, 'error': 'Market is not found'}

    result = run(
        market['marketplace'], data.shopName, market['performance_key'], 
        market['performance_secret'], market['client_id'], 
        market['client_key'], data.startDate, data.endDate,
        background_tasks
    )
    
    response = {
        "ok": True,
        "status": 200,
        "sheet_url": result
    }
    
    return response


@router.post('/api/markets')
async  def save_markets(data: Markets):
    with open('markets.json', 'w') as f:
        f.write(data.model_dump_json())

    return 200

@router.get('/api/markets')
async  def get_markets():
    if not os.path.exists('markets.json'):
        return {'markets': []}

    with open('markets.json') as f:
        data = json.load(f)

    for d in data:
        data[d] = {
            'name': data[d]['name'],
        }

    return data
