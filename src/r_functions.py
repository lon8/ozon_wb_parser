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
    
def download_accessibility_report(url: str, payload: dict, headers: dict) -> list:
    try:
        
        response = requests.get(url, json=payload, headers=headers)
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
    
def create_accessibility_report(client_id: str, client_key: str) -> list[list]:
    payload = {
        "limit": "50",
        "offset": "0",
        "filter": {}
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ru',
        'content-type': 'application/json',
        # 'cookie': 'cf_clearance=sDquQwZYr21PBw4Zgt9p4HQC_uJPBVqlv3uEBCNcCJc-1717325082-1.0.1.1-tFMgwtnhGcFEY2LW.2pQTXJ.ALvdFqGCaXZH8DO1g20menJXSzrbaeia1tV9kj3kvZZtqOe_KRQTrhwZ9NGbiw; __Secure-ab-group=1; __Secure-ext_xcid=227ab7ea19de7c86aad6b2bc0858188b; rfuid=NjkyNDcyNDUyLDEyNC4wNDM0NzUyNzUxNjA3NCwxMzQ3MDc1MTIzLC0xLC0yMTE3MjQ0MzU4LFczc2libUZ0WlNJNklsQkVSaUJXYVdWM1pYSWlMQ0prWlhOamNtbHdkR2x2YmlJNklsQnZjblJoWW14bElFUnZZM1Z0Wlc1MElFWnZjbTFoZENJc0ltMXBiV1ZVZVhCbGN5STZXM3NpZEhsd1pTSTZJbUZ3Y0d4cFkyRjBhVzl1TDNCa1ppSXNJbk4xWm1acGVHVnpJam9pY0dSbUluMHNleUowZVhCbElqb2lkR1Y0ZEM5d1pHWWlMQ0p6ZFdabWFYaGxjeUk2SW5Ca1ppSjlYWDBzZXlKdVlXMWxJam9pUTJoeWIyMWxJRkJFUmlCV2FXVjNaWElpTENKa1pYTmpjbWx3ZEdsdmJpSTZJbEJ2Y25SaFlteGxJRVJ2WTNWdFpXNTBJRVp2Y20xaGRDSXNJbTFwYldWVWVYQmxjeUk2VzNzaWRIbHdaU0k2SW1Gd2NHeHBZMkYwYVc5dUwzQmtaaUlzSW5OMVptWnBlR1Z6SWpvaWNHUm1JbjBzZXlKMGVYQmxJam9pZEdWNGRDOXdaR1lpTENKemRXWm1hWGhsY3lJNkluQmtaaUo5WFgwc2V5SnVZVzFsSWpvaVEyaHliMjFwZFcwZ1VFUkdJRlpwWlhkbGNpSXNJbVJsYzJOeWFYQjBhVzl1SWpvaVVHOXlkR0ZpYkdVZ1JHOWpkVzFsYm5RZ1JtOXliV0YwSWl3aWJXbHRaVlI1Y0dWeklqcGJleUowZVhCbElqb2lZWEJ3YkdsallYUnBiMjR2Y0dSbUlpd2ljM1ZtWm1sNFpYTWlPaUp3WkdZaWZTeDdJblI1Y0dVaU9pSjBaWGgwTDNCa1ppSXNJbk4xWm1acGVHVnpJam9pY0dSbUluMWRmU3g3SW01aGJXVWlPaUpOYVdOeWIzTnZablFnUldSblpTQlFSRVlnVm1sbGQyVnlJaXdpWkdWelkzSnBjSFJwYjI0aU9pSlFiM0owWVdKc1pTQkViMk4xYldWdWRDQkdiM0p0WVhRaUxDSnRhVzFsVkhsd1pYTWlPbHQ3SW5SNWNHVWlPaUpoY0hCc2FXTmhkR2x2Ymk5d1pHWWlMQ0p6ZFdabWFYaGxjeUk2SW5Ca1ppSjlMSHNpZEhsd1pTSTZJblJsZUhRdmNHUm1JaXdpYzNWbVptbDRaWE1pT2lKd1pHWWlmVjE5TEhzaWJtRnRaU0k2SWxkbFlrdHBkQ0JpZFdsc2RDMXBiaUJRUkVZaUxDSmtaWE5qY21sd2RHbHZiaUk2SWxCdmNuUmhZbXhsSUVSdlkzVnRaVzUwSUVadmNtMWhkQ0lzSW0xcGJXVlVlWEJsY3lJNlczc2lkSGx3WlNJNkltRndjR3hwWTJGMGFXOXVMM0JrWmlJc0luTjFabVpwZUdWeklqb2ljR1JtSW4wc2V5SjBlWEJsSWpvaWRHVjRkQzl3WkdZaUxDSnpkV1ptYVhobGN5STZJbkJrWmlKOVhYMWQsV3lKeWRTMVNWU0pkLDAsMSwwLDI0LDIzNzQxNTkzMCw4LDIyNzEyNjUyMCwwLDEsMCwtNDkxMjc1NTIzLFIyOXZaMnhsSUVsdVl5NGdUbVYwYzJOaGNHVWdSMlZqYTI4Z1RHbHVkWGdnZURnMlh6WTBJRFV1TUNBb1dERXhPeUJNYVc1MWVDQjRPRFpmTmpRcElFRndjR3hsVjJWaVMybDBMelV6Tnk0ek5pQW9TMGhVVFV3c0lHeHBhMlVnUjJWamEyOHBJRU5vY205dFpTOHhNall1TUM0d0xqQWdVMkZtWVhKcEx6VXpOeTR6TmlBeU1EQXpNREV3TnlCTmIzcHBiR3hoLGV5SmphSEp2YldVaU9uc2lZWEJ3SWpwN0ltbHpTVzV6ZEdGc2JHVmtJanBtWVd4elpTd2lTVzV6ZEdGc2JGTjBZWFJsSWpwN0lrUkpVMEZDVEVWRUlqb2laR2x6WVdKc1pXUWlMQ0pKVGxOVVFVeE1SVVFpT2lKcGJuTjBZV3hzWldRaUxDSk9UMVJmU1U1VFZFRk1URVZFSWpvaWJtOTBYMmx1YzNSaGJHeGxaQ0o5TENKU2RXNXVhVzVuVTNSaGRHVWlPbnNpUTBGT1RrOVVYMUpWVGlJNkltTmhibTV2ZEY5eWRXNGlMQ0pTUlVGRVdWOVVUMTlTVlU0aU9pSnlaV0ZrZVY5MGIxOXlkVzRpTENKU1ZVNU9TVTVISWpvaWNuVnVibWx1WnlKOWZYMTksNjUsLTEyODU1NTEzLDEsMSwtMSwxNjk5OTU0ODg3LDE2OTk5NTQ4ODcsMzM2MDA3OTMzLDg=; __Secure-access-token=4.48185617.YzfchhnKTrOOqG00vhYN5Q.1.AajcEko_4lDU6Yg4s7M4apHgbMuRcynOdeaqgfC68nUXJtVxshk7cO6QsN8obZsZDZH092QmWN0b6RyhlBUT6SU.20200704191410.20240626160859.LUk-5_ea4Rro80zUfB1x700SgT4BfOeGMaVMAVRwx6E; __Secure-refresh-token=4.48185617.YzfchhnKTrOOqG00vhYN5Q.1.AajcEko_4lDU6Yg4s7M4apHgbMuRcynOdeaqgfC68nUXJtVxshk7cO6QsN8obZsZDZH092QmWN0b6RyhlBUT6SU.20200704191410.20240626160859.3UdlfpSXnFxaHNQn65q3I8x6jPbxNITqG8-iZ1NpXjs; __Secure-user-id=48185617; bacntid=4457252; contentId=550209; x-o3-language=ru; xcid=dbca5de9df86414385d8320d69f4c098; __Secure-ETC=b49644b7a3656aeb3636427a5ce10f27; abt_data=9922ed12525c2f2cc0cb45e56f8999ef:4c0b7b4c736814735d30095b577c1cd51eadb2bee44b818d824d26a165dcb4ed4255e1985558de87628fee999c032eceb846cbd8cf4f233837f2f5b64ac1edb4e6f155427c1e9d47c5b7149b95c1e644a8896239c3dfc5f02d408d2648af9e79336c091fb1109d8bab4906bc61b5f5de3e9f9baed44244862d74899640869b6c08c9172570a71f21b1837fde79908c234bfe3002017403f03884caa383b717548a99ec79f0fa591c0a77f34926b523f71eff0b674691d584656955d167c0a7085bc0bd451824afc318d31de4f5a2ec1f0e4f12bd3211d335002ef105e40b7aedd1a29a9efc88c830d15d8139a2dedd23668c88ce67cef6259e4143ae984f612605923ff98bb27d9493c6a2df8a7eaa7a9548c67f3559f6aa12a45a5e1ffffffe3f9bfad61780fc57ba6e35eff5c2831fcea7438501a28158995910d84f1cfcca6299482179026c0713f8fc45d09cd51a48d5c6c9bbcfd2f0bd85d3f7d3fbd41f72a604e673b263efc669fc29ca82cc52; ADDRESSBOOKBAR_WEB_CLARIFICATION=1719414738',
        'origin': 'https://seller.ozon.ru',
        'priority': 'u=1, i',
        'referer': 'https://seller.ozon.ru/app/analytics/goods-availability/index',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'x-o3-app-name': 'seller-ui',
        'x-o3-company-id': '550209',
        'x-o3-language': 'ru',
        'x-o3-page-type': 'analytics-other',
    }
    response = requests.post('https://seller.ozon.ru/api/accessibility-index/v1/items/list/prepare', json=payload, headers=headers)
    print(response.text)
    sleep(7)
    data = download_accessibility_report('https://seller.ozon.ru/api/accessibility-index/v1/items/list/download', payload, headers)
    print(data)
    
if __name__ == '__main__':
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    # create_products_report(client_id, client_key)
    create_accessibility_report(client_id, client_key)