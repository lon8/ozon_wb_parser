import requests
import csv
from loguru import logger
from tqdm import tqdm
from time import sleep

def get_beaver_token(perf_client_id: str, perf_client_key: str) -> str:

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    payload = {
        "client_id": perf_client_id, 
        "client_secret": perf_client_key, 
        "grant_type": "client_credentials"
    }
    
    response = requests.post("https://performance.ozon.ru/api/client/token", json=payload, headers=headers)
    
    return response.json()['access_token']
    

def download_and_get_csv(url :str) -> list[list]:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Проверка на успешное выполнение запроса 8192
        
        if response.status_code == 200:
            # Calculate the total file size in bytes
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1024  # 1 KB
            
            # Initialize tqdm progress bar
            progress_bar = tqdm(total=total_size_in_bytes, unit='B', unit_scale=True)
            
            # Open a local file with 'wb' (write binary) mode
            with open("file.csv", 'wb') as f:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    f.write(data)
            
            progress_bar.close()
            logger.info(f"File downloaded successfully!")
        
        with open('file.csv', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=";")
            data = list(reader)
        
        return data  # return the rows of csv-file
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        return None


def get_report_url(client_id: str, client_key: str, code: str) -> str: # return url
    
    payload = {
        "code": code
    }
    
    headers = {
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post('https://api-seller.ozon.ru/v1/report/info', json=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        logger.debug(data)
        url = data['result']['file']
        status = data['result']['status']
        if status == 'processing' or status == 'waiting':
            sleep(3)
            return get_report_url(client_id, client_key, code)
        logger.debug(url)
        logger.info('Get url successfully!')
        return url
        
    
def create_products_report(client_id, client_key) -> list[list]:
    payload = {
        "language": "DEFAULT",
        "offer_id": [ ],
        "search": "",
        "sku": [ ],
        "visibility": "ALL"
    }
    
    headers = {
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post("https://api-seller.ozon.ru/v1/report/products/create", json=payload, headers=headers)

    if response.status_code == 200:
        data = response.json()
        code = data['result']['code']
        url = get_report_url(client_id, client_key, code)
        logger.info("Product succeesfully created!")
        return download_and_get_csv(url)
    else:
        logger.warning('Product report creating was failed')
        return -1

def prepare_return_report(data: list[dict], headers: dict, scope_type: str) -> list[list]:
    
    first_row = [
        'Возврат на FBO или FBS',
        'Артикул продавца',
        'OZON SKU ID',
        "Наименование товара",
        "Номер отправления",
        "Статус возврата",
        "Дата оформления заказа",
        "Дата возврата",
        "Причина возврата"
    ]
    
    result = []
    result.append(first_row)
    
    skus = [item['sku'] for item in data]
    
    payload = {
        "offer_id": [],
        "product_id": [],
        "sku": skus
    }
    
    
    product_response = requests.post('https://api-seller.ozon.ru/v2/product/info/list', json=payload, headers=headers)
    
    product_list = product_response.json()['result']['items']
    
    def get_name_by_sku(sku):
        for elem in product_list:
            if elem['sku'] == item['sku']:
                return elem['name']
            return "Имя отсутствует, товар удален"
    
    for item in data:
        
        row = [scope_type,
               item['company_id'],
               item['sku'],
               get_name_by_sku(item['sku']),
               item['posting_number'],
               item['status_name'],
               item['accepted_from_customer_moment'],
               item['returned_to_ozon_moment'],
               item['return_reason_name']]
        result.append(row)
        
    return result

# scope_type - fbo | fbs
def create_returns_report(client_id: str, client_key: str, scope_type: str) -> list[list]:
    payload = {
        "filter": {},
        "last_id": 0,
        "limit": 1000
    }
    
    headers = {
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post(f"https://api-seller.ozon.ru/v3/returns/company/{scope_type}", json=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()['returns']
    
        return prepare_return_report(data, headers, scope_type)
    

def create_postings_report(client_id: str, client_key: str, scope_type: str) -> list[list]:
    payload = {
        "filter": {
            "processed_at_from": "2023-12-02T17:10:54.861Z",
            "processed_at_to": "2024-02-02T17:09:54.861Z",
            "delivery_schema": [
                f"{scope_type}"
            ]
        },
        "language": "DEFAULT"
    }
    
    headers = {
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post('https://api-seller.ozon.ru/v1/report/postings/create', json=payload, headers=headers)
    
    code = response.json()['result']['code']
    
    url = get_report_url(client_id, client_key, code)
    return download_and_get_csv(url)

def prepare_orders_report(data: list[dict]) -> list[list]:
    
    result = []
    
    first_row = ["Номер заявки","Статус","Дата поставки и таймслот","Склад размещения","Поставки","Кол-во товаров","Дата создания"]

    result.append(first_row)
    
    for item in data:
        row = [
            item['supply_order_number'],
            item['state'],
            f"От: {item['local_timeslot']['from']}\n"
            f"До: {item['local_timeslot']['to']}",
            item['supply_warehouse']['name'],
            1,
            item['total_items_count'],
            item['created_at'] 
        ]
        result.append(row)
        
    return result


def create_supply_orders_report(client_id: str, client_key: str) -> list[list]:
    payload = {
        "page": 1,
        "page_size": 100
    }
    
    headers = {
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post('https://api-seller.ozon.ru/v1/supply-order/list', json=payload, headers=headers)
    
    data = response.json()['supply_orders']
    
    return prepare_orders_report(data)


def prepare_ads_report(payload: dict, headers: dict, beaver_token: str) -> list[list]:

    headers = {
        'Authorization': f'Bearer {beaver_token}',
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    response = requests.post('https://performance.ozon.ru/api/client/statistic/products/generate/json', json=payload, headers=headers)
    
    uuid = response.json()['UUID']
    
    sleep(10)
    
    report_response = requests.get(f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}', headers=headers)
    
    data = report_response.json()['report']['rows']
    
    first_row = ["OZON id","Артикул","Заказы шт","Заказы руб", "Продвижение в поиске, руб", "Трафареты, руб","Реклама, руб","ДРР, %"]
    result = []
    
    result.append(first_row)
    
    for item in data:
        pass # Continue
    
    sleep(5)


def create_ads_report(client_id: str, client_key: str, date_to: str, date_from: str) -> list[list]:
    
    payload = {
        "from": f"{date_from}",
        "to": f"{date_to}"
    }
    
    beaver_token = get_beaver_token(client_id, client_key)
    
    headers = {
        'Authorization': f'Bearer {beaver_token}',
        "Client-Id": client_id,
        "Api-Key": client_key
    }
    
    data = prepare_ads_report(payload, headers, beaver_token)
    
    
    sleep(5)
    
if __name__ == '__main__':
    date_to = '2024-02-24T14:15:22Z'
    date_from = '2024-01-24T14:15:22Z'
    performance_client_id = '30757988-1719325107981@advertising.performance.ozon.ru'
    performance_client_key = "-PPvEshjvyZA60idYi6xs6VBaUgA31RlerMcb7z4cJzXGyvhm33kEUFw9Z7qrhHT6IqYuISbuooeIowViw"
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    data = create_ads_report(performance_client_id, performance_client_key, date_to, date_from)
    logger.info(data)