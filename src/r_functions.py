import requests
import csv
from loguru import logger
from tqdm import tqdm
from time import sleep

class ParserSession:

    def __init__(
        self, 
        client_id: str, 
        client_key: str, 
        date_from: str, date_to: str, 
        perf_client_id: str = '30757988-1719325107981@advertising.performance.ozon.ru', 
        perf_client_key: str = '-PPvEshjvyZA60idYi6xs6VBaUgA31RlerMcb7z4cJzXGyvhm33kEUFw9Z7qrhHT6IqYuISbuooeIowViw'
    ):
        self.__client_id = client_id
        self.__client_key = client_key
        self.__beaver_token = None
        self.__date_from = date_from
        self.__date_to = date_to
        self.__perf_client_id = perf_client_id
        self.__perf_client_key = perf_client_key

    @property
    def beaver_token(self) -> str:
        if self.__beaver_token is None:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            payload = {
                "client_id": self.__perf_client_id, 
                "client_secret": self.__perf_client_key, 
                "grant_type": "client_credentials"
            }
            
            response = requests.post("https://performance.ozon.ru/api/client/token", json=payload, headers=headers)
            
            self.__beaver_token = response.json()['access_token']
        
        return self.__beaver_token

    def __download_and_get_csv(self, url :str) -> list[list]:
        try:
            response = self.__g(url)
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
        except requests.exceptions.RequestException:
            logger.exception(f"Error downloading file {url}")
            return

    def __p(self, url: str, headers: dict = None, json: dict = None) -> requests.Response:
        return self.__r(requests.post, url, headers, json)
    
    def __g(self, url: str, headers: dict = None, json: dict = None) -> requests.Response:
        return self.__r(requests.get, url, headers, json)

    def __r(self, method, url: str, headers: dict = None, json: dict = None) -> requests.Response:
        response = method(url, headers=headers, json=json)
        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(response.json())
            raise e
        return response

    def __get_report_url(self, code: str) -> str: # returns url
        
        payload = {
            "code": code
        }
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p('https://api-seller.ozon.ru/v1/report/info', json=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(data)
            url = data['result']['file']
            status = data['result']['status']
            if status == 'processing' or status == 'waiting':
                sleep(3)
                return self.__get_report_url(code)
            logger.debug(url)
            logger.info('Get url successfully!')
            return url
            
        
    def create_products_report(self) -> list[list]:
        payload = {
            "language": "DEFAULT",
            "offer_id": [ ],
            "search": "",
            "sku": [ ],
            "visibility": "ALL"
        }
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p("https://api-seller.ozon.ru/v1/report/products/create", json=payload, headers=headers)

        if response.status_code == 200:
            data = response.json()
            code = data['result']['code']
            url = self.__get_report_url(code)
            logger.info("Product succeesfully created!")
            return self.__download_and_get_csv(url)
        else:
            logger.warning('Product report creating was failed')
            return -1

    def __prepare_return_report(self, data: list[dict], headers: dict, scope_type: str) -> list[list]:
        
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
        if not skus:
            return result
        
        payload = {
            "offer_id": [],
            "product_id": [],
            "sku": skus
        }
        
        product_response = self.__p(
            'https://api-seller.ozon.ru/v2/product/info/list', 
            json=payload, 
            headers=headers
        )
        product_response = product_response.json()
        if not product_response:
            product_list = []
        else:
            product_list = product_response['result']['items']
        
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
    def create_returns_report(self, scope_type: str) -> list[list]:
        payload = {
            "filter": {} if scope_type != 'fbs' else {
                "last_free_waiting_day": {
                    "time_from": self.__date_from,
                    "time_to": self.__date_to
                }
            },
            "last_id": 0,
            "limit": 1000
        }
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p(f"https://api-seller.ozon.ru/v3/returns/company/{scope_type}", json=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()['returns']
            if not data:
                return []
        
            return self.__prepare_return_report(data, headers, scope_type)
        

    def create_postings_report(self, scope_type: str) -> list[list]:
        payload = {
            "filter": {
                "processed_at_from": self.__date_from,
                "processed_at_to": self.__date_from,
                "delivery_schema": [
                    f"{scope_type}"
                ]
            },
            "language": "DEFAULT"
        }
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p('https://api-seller.ozon.ru/v1/report/postings/create', json=payload, headers=headers)
        response = response.json()
        if not response:
            return []
        
        code = response['result']['code']
        
        url = self.__get_report_url(code)
        return self.__download_and_get_csv(url)

    def create_supply_orders_report(self) -> list[list]:
        payload = {
            "page": 1,
            "page_size": 100
        }
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p(
            f'https://api-seller.ozon.ru/v1/supply-order/list', 
            json=payload, 
            headers=headers
        )
        
        data = response.json()['supply_orders']
        
        return self.__prepare_orders_report(data)

    def __prepare_orders_report(self, data: list[dict]) -> list[list]:
        
        result = []
        
        first_row = ["Номер заявки","Статус","Дата поставки и таймслот","Склад размещения","Поставки","Кол-во товаров","Дата создания"]

        result.append(first_row)
        
        for item in data:
            row = [
                item['supply_order_number'],
                item['state'],
                ('-' if not item['local_timeslot'] else \
                    f"От: {item['local_timeslot']['from']}\n"
                    f"До: {item['local_timeslot']['to']}"
                ),
                item['supply_warehouse']['name'],
                1,
                item['total_items_count'],
                item['created_at'] 
            ]
            result.append(row)
            
        return result


    def create_daily_ads_report(self):
        
        headers = {
            'Authorization': f'Bearer {self.beaver_token}',
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }

        date_from = self.__date_from.split('T')[0]
        date_to = self.__date_to.split('T')[0]

        response = self.__g(f"https://performance.ozon.ru/api/client/statistics/daily/json?dateFrom={date_from}&dateTo={date_to}", 
            headers=headers)
        data = response.json()
        
        sleep(1)


    def create_ads_report(self, client_id: str, client_key: str) -> list[list]:
        
        payload = {
            "from": f"{self.__date_from}",
            "to": f"{self.__date_to}"
        }
        
        headers = {
            'Authorization': f'Bearer {self.beaver_token}',
            "Client-Id": client_id,
            "Api-Key": client_key
        }
        
        data = self.__prepare_ads_report(payload, headers)
        
        sleep(5)

    def __prepare_ads_report(self, payload: dict, headers: dict) -> list[list]:

        headers = {
            'Authorization': f'Bearer {self.beaver_token}',
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p( 
            f'https://performance.ozon.ru/api/client/statistic/products/generate/json', 
            json=payload, 
            headers=headers
        )
        
        uuid = response.json()['UUID']
        
        sleep(10)
        
        report_response = self.__g(f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}', headers=headers)
        
        campaign_response = self.__g('https://performance.ozon.ru/api/client/campaign?advObjectType=SKU&state=CAMPAIGN_STATE_UNKNOWN', headers=headers)
        
        campaign_data = campaign_response.json()
        
        data = report_response.json()['report']['rows']
        
        first_row = ["OZON id","Артикул","Заказы шт","Заказы руб", "Продвижение в поиске, руб","ДРР, %"]
        result = []
        
        result.append(first_row)
        
        for item in data:
            row = [
                item['sku'],
                item['offerId'],
                item['orders'],
                item['ordersMoney'],
                item['moneySpent'],
                item['drr']
            ]
            result.append(row)
        
        sleep(5)

    def __get_warehouses(self):
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        
        response = self.__p('https://api-seller.ozon.ru/v1/warehouse/list', headers=headers)
        response = response.json()
        if not response:
            return []
        
        return response['result']

    def __get_stock_on_warehouses(self):

        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        json = {
            "limit": 100,
            "offset": 0,
            "warehouse_type": "ALL"
        }

        response = self.__p('https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses', headers=headers, json=json)
        response = response.json()
        if not response:
            return []
        
        return response['result']

    def get_order_incomes(self):
        
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        json = {
            "limit": 100,
            "offset": 0,
            "transaction_type": "orders",
            'filter': {
                'date': {
                    'from': self.__date_from,
                    'to': self.__date_to
                }
            }
        }

        response = self.__p('https://api-seller.ozon.ru/v3/finance/transaction/list', headers=headers, json=json)
        response = response.json()
        if not response or 'result' not in response or 'operations' not in response['result']:
            return []
        
        return self.__prepare_order_incomes(response['result']['operations'])
    
    def __prepare_order_incomes(self, data: list[dict]) -> list:
        result = []
        
        first_row = ['Дата начисления', 'Тип начисления', 'Номер отправления или идентификатор услуги',	
                     'Дата принятия заказа в обработку или оказания услуги', 'Склад отгрузки',	
                     'SKU',	'Артикул', 'Название товара или услуги', 'Количество',	
                     'За продажу или возврат до вычета комиссий и услуг', 'Ставка комиссии',
                     'Комиссия за продажу',	
                     'Сборка заказа', 'Обработка отправления (Drop-off/Pick-up) (разбивается по товарам пропорционально количеству в отправлении)',
                     'Магистраль',	'Последняя миля (разбивается по товарам пропорционально доле цены товара в сумме отправления)',	
                     'Обратная магистраль',	'Обработка возврата',
                     'Обработка отмененного или невостребованного товара (разбивается по товарам в отправлении в одинаковой пропорции)', 
                     'Обработка невыкупленного товара',	
                     'Логистика', 'Индекс локализации',	'Обратная логистика', 'Итого']

        result.append(first_row)
        
        for item in data:
            row = [
                item['supply_order_number'],
                item['state'],
                ('-' if not item['local_timeslot'] else \
                    f"От: {item['local_timeslot']['from']}\n"
                    f"До: {item['local_timeslot']['to']}"
                ),
                item['supply_warehouse']['name'],
                1,
                item['total_items_count'],
                item['created_at'] 
            ]
            result.append(row)
            
        return result

if __name__ == '__main__':
    date_to = '2024-02-24T14:15:22Z'
    date_from = '2024-01-24T14:15:22Z'
    performance_client_id = '30757988-1719325107981@advertising.performance.ozon.ru'
    performance_client_key = "-PPvEshjvyZA60idYi6xs6VBaUgA31RlerMcb7z4cJzXGyvhm33kEUFw9Z7qrhHT6IqYuISbuooeIowViw"
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    # data = create_ads_report(performance_client_id, performance_client_key, date_to, date_from)
    parser = ParserSession(client_id, client_key, date_from, date_to)
    d = parser.create_sotck_report()
    logger.info(d)