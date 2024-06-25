import requests
import csv
from loguru import logger
from tqdm import tqdm
from time import sleep

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


def get_report_url(client_id: str, client_key: str, code: str, counter = 5) -> str: # return url
    
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
        if url == '' and counter != 5:
            sleep(3)
            get_report_url(client_id, client_key, code, counter=counter+1)
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
    
    
if __name__ == '__main__':
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    create_products_report(client_id, client_key)