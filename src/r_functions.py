import requests
import csv
from loguru import logger
from tqdm import tqdm
from time import sleep

current_fbo = current_fbs = None

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
            
            response = self.__p("https://performance.ozon.ru/api/client/token", json=payload, headers=headers)
            
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
            
            with open('file.csv', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=";")
                data = list(reader)[1:]
            
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
            return self.__download_and_get_csv(url)
        else:
            logger.warning('Product report creating was failed')
            return -1

    def __prepare_return_report(self, data: list[dict], headers: dict, scope_type: str) -> list[list]:
        
        result = []
        
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
        global current_fbo
        global current_fbs

        if scope_type == 'fbs' and current_fbs is not None:
            return current_fbs
        if scope_type == 'fbo' and current_fbo is not None:
            return current_fbo


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
        res =  self.__download_and_get_csv(url)
        if scope_type == 'fbo':
            current_fbo = res
        if scope_type == 'fbs':
            current_fbs = res

        return res

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
            'https://api-seller.ozon.ru/v1/supply-order/list', 
            json=payload, 
            headers=headers
        )
        
        data = response.json()['supply_orders']
        
        return self.__prepare_orders_report(data)

    def __prepare_orders_report(self, data: list[dict]) -> list[list]:

        result = []

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


    def create_ads_report(self) -> list[list]:
        right = self.__get_daily_ads_report()
        left = self.__get_ads_report()
        
        while len(left) < len(right):
            left.append([])
        
        for i in range(min(len(left), len(right))):
            left[i] += [''] + right[i]

        return left

    def __get_ads_report(self) -> list[list]:
        
        payload = {
            "from": f"{self.__date_from}",
            "to": f"{self.__date_to}"
        }
        
        headers = {
            'Authorization': f'Bearer {self.beaver_token}',
            "Client-Id": self.__perf_client_id,
            "Api-Key": self.__perf_client_key
        }

        response = self.__p( 
            'https://performance.ozon.ru:443/api/client/statistic/products/generate/json', 
            json=payload, 
            headers=headers
        )
        data = self.__prepare_ads_report(response.json(), headers)
        
        result = []
        for item in data:
            row = [
                item['sku'],
                item['offerId'],
                item['orders'],
                item['ordersMoney'],
                'Нет данных',
                item['bid'],
                item['moneySpent'],
                item['drr']
            ]
            result.append(row)
        
        return result

    def __prepare_ads_report(self, data: dict, headers: dict) -> list[list]:
        
        uuid = data['UUID']
        
        sleep(5)
        
        report_response = self.__g(f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}', headers=headers)
        return report_response.json()['report']['rows']
        
    def __get_daily_ads_report(self):
        
        headers = {
            'Authorization': f'Bearer {self.beaver_token}',
            "Client-Id": self.__perf_client_id,
            "Api-Key": self.__perf_client_key
        }

        date_from = self.__date_from.split('T')[0]
        date_to = self.__date_to.split('T')[0]

        response = self.__g(f"https://performance.ozon.ru/api/client/statistics/daily/json?dateFrom={date_from}&dateTo={date_to}", 
            headers=headers)
            
        data = response.json()
        if not data['rows']:
            return []
        
        data = data['rows']
        
        result = []
        for item in data:
            row = [
                item['date'],
                item['sku'],
                item['offerId'],
                item['orders'],
                item['ordersMoney'],
                'Нет данных',
                item['bid'],
                item['moneySpent'],
                item['drr']
            ]
            result.append(row)
        
        return result

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
            'filter': {
                "transaction_type": "all",
                'date': {
                    'from': self.__date_from,
                    'to': self.__date_to
                },
                "operation_type": [],
                "posting_number": "all",
            },
            "page": 1,
            "page_size": 1000
        }

        response = self.__p('https://api-seller.ozon.ru/v3/finance/transaction/list', headers=headers, json=json)
        response = response.json()
        if not response or 'result' not in response or 'operations' not in response['result']:
            return []
        
        return self.__prepare_order_incomes(response['result']['operations'])

    
    def __prepare_order_incomes(self, data: list[dict]) -> list:
        result = []

        warehouse_names = self.__getWareHouseNames([i['posting']['warehouse_id'] for i in data])
        goods = set()
        for i in data:
            for g in i['items']:
                goods.add(g['sku'])
        good_info = self.__getGoodsInfo(list(goods))

        def getServicePrice(name: str, item: dict) -> float:
            for i in item['services']:
                if name in i:
                    return i[name]['price']
            return 0

        for item in data:
            item['services_str'] = str(item['services'])
            goods_num = len(item['items'])
            for good in item['items']:
                row = [
                    item['operation_date'],
                    item['operation_type_name'],
                    item['operation_id'],
                    item['posting']['order_date'],
                    warehouse_names[item['posting']['warehouse_id']],
                    item['operation_date'],
                    good['sku'],
                    good_info[good['sku']]['offer_id'],
                    good_info[good['sku']]['name'],
                    1,
                    self.__cnvBool('MarketplaceNotDeliveredCostItem' in item['services_str'] or 'MarketplaceReturnAfterDeliveryCostItem' in item['services_str']),
                    item['sale_commission'],
                    item['posting']['posting_number'],
                    'Pick-Up' if 'MarketplaceServiceItemPickup' in item['services_str'] else (
                        'Drop-off' if 'MarketplaceServiceItemDropoffPPZ' in item['services_str'] else 'Нет данных'),
                    self.__cnvBool(getServicePrice('MarketplaceServiceItemDirectFlowTrans', item) / goods_num, True),
                    self.__cnvBool(getServicePrice('MarketplaceServiceItemDelivToCustomer', item) / goods_num, True),
                    self.__cnvBool(getServicePrice('MarketplaceServiceItemReturnFlowTrans', item) / goods_num, True),
                    self.__cnvBool('MarketplaceServiceItemReturnAfterDelivToCustomer' in item['services_str']),
                    self.__cnvBool('MarketplaceServiceItemReturnNotDelivToCustomer' in item['services_str']),
                    self.__cnvBool('MarketplaceServiceItemReturnPartGoodsCustomer' in item['services_str']),
                    self.__cnvBool('MarketplaceServiceItemDirectFlowLogistic' in item['services_str']),
                    'Нет данных',
                    self.__cnvBool('MarketplaceServiceItemReturnFlowLogistic' in item['services_str']),
                    item['amount'] / goods_num
                ]
                result.append(row)
            
        return result

    def __getWareHouseNames(self, warehouse_ids: list[str]) -> dict:
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
    
        return {i: self.__get_wh_name(i, headers) for i in warehouse_ids}       
    
    def __get_wh_name(self, wh_id: int, headers: dict) -> str:
        json = {
            "filter": {
                "provider_id": 0,
                "status": "",
                "warehouse_id": wh_id
            },
            "limit": 100,
            "offset": 0
        }
        res = self.__p('https://api-seller.ozon.ru/v1/delivery-method/list', headers=headers, json=json)
        res = res.json()
        if not res or not res['result']:
            return 'Нет данных'

        return res['result'][0]['name']

    def __getGoodsInfo(self, good_skus: list[str]) -> dict:
        if not good_skus:
            return {}

        payload = {
            "offer_id": [],
            "product_id": [],
            "sku": good_skus
        }
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        products = self.__p(
            'https://api-seller.ozon.ru/v2/product/info/list', 
            json=payload, 
            headers=headers
        )
        products = products.json()['result']
        if not products or 'items' not  in products or not products['items']:
            return {}
        
        commissions = self.__getGoodCommissions([i['offer_id'] for i in products['items']], headers)
        result = {}
        for p in products['items']:
            result[p['sku']] = {
                'is_kgt': p['is_kgt'],
                'currency_code': p['currency_code'],
                'offer_id': p['offer_id'],         
                'name': p['name'],
                "price_indexes": p['price_indexes'],
                'commissions': {'value': 'Нет данных'} if not commissions else {'value': commissions[p['offer_id']]},
                'stocks': p['stocks']
            }
        
        for s in good_skus:
            if s not in result:
                result[p['sku']] = {
                    'is_kgt': 'Нет данных',
                    'currency_code': 'Нет данных',
                    'offer_id': 'Нет данных',         
                    'name': 'Нет данных',
                    'price_indexes': {
                        'price_index': 'Нет данных'
                    },
                    'commissions' : {'value': 'Нет данных'},
                    'stocks': {
                        "coming": 'Нет данных',
                        "present": 'Нет данных',
                        "reserved": 'Нет данных'
                    } 
                }

        return result

    def __getGoodCommissions(self, offer_id: list[str], headers: str) -> dict:
        try:
            info = self.__p('https://api-seller.ozon.ru/v4/product/info/prices', headers, {
                "last_id": "",
                "limit": 100,
                "filter": {
                    "offer_id": offer_id,
                    "product_id": [],
                    "visibility": "ALL"
                }
            })
        except:
            return {}
        
        res = info.json()['result']
        if not res or 'items' not in res:
            return {}
        
        res = res['items']
        result = {}
        for r in res:
            result[r['offer_id']] = sum(list(r['commissions'].values()))

        return result

    def __cnvBool(self, s: bool|object, value_if_true: bool = False) -> str|object:
        if s:
            if not value_if_true:
                return '+'
            return s
        return '-'
    
    def create_supply_report(self):
        goods = self.create_postings_report('fbo') + self.create_postings_report('fbs')
        if not goods:
            return []
        whs_stocks = self.__get_stock_on_warehouses()

        def get_stock(good):
            for i in whs_stocks:
                if i['sku'] == good[1]:
                    return i['free_to_sell_amount']
            return 'Нет данных'

        res = []
    
        info = self.__getGoodsInfo([i[1] for i in goods])
        for g in goods:
            stock = get_stock(g)
            row = [
                g[5],
                '-',
                g[1],
                '-',
                info[g[1]]['commissions']['value'],
                '-' if not isinstance(g, int) else stock * g[15],
                stock,
                '-',
                '-',
                '-', # прсомотры карточки
                '-',
                '-',
                info[g[1]]['price_indexes']['price_index'],
                '-',
                '-',
                '-',
                '-',
                '-',
                '-',
                '-'
            ]
            res.append(row)
        
        return res

    def get_products_awailability(self):
        payload = {
            "page": 1,
            "page_size": 100,
            "states": ['READY_TO_SUPPLY', 'ACCEPTED_AT_SUPPLY_WAREHOUSE', 'IN_TRANSIT']
        }
        headers = {
            "Client-Id": self.__client_id,
            "Api-Key": self.__client_key
        }
        supplies = self.__p(
            'https://api-seller.ozon.ru/v1/supply-order/list', 
            json=payload, 
            headers=headers
        )
        supplies = supplies.json()
        if not supplies or 'supply_orders' not in supplies:
            return []
        
        supplies = supplies['supply_orders']

        payload["supply_order_id"] = None

        goods_supply_num = {}

        for s in supplies:
            items = self.__get_supply_orders(s['supply_order_id'], headers, payload)
            if not items:
                continue
            for i in items:
                if i['sku'] not in goods_supply_num:
                    goods_supply_num[i['sku']] = 0
                goods_supply_num[i['sku']] += i['quantity']

        result = []

        if not goods_supply_num:
            return result

        info = self.__getGoodsInfo(list(goods_supply_num.keys()))
        for g in goods_supply_num:
            if g not in info:
                continue
            row = [
                info[g]['offer_id'],
                info[g]['name'],
                '-',
                g,
                '-',
                '-',
                '-',
                '-',
                '-',
                info[g]['stocks']['present'],
                goods_supply_num[g],
                '-',
                '-',
                '-',
                '-'
            ]
            result.append(row)

        return result


    def __get_supply_orders(self, supply_order_id: int, headers: dict, json: dict) -> list:
        json['supply_order_id'] = int(supply_order_id)
        try:
            products = self.__p(
                'https://api-seller.ozon.ru/v1/supply-order/items', 
                json=json, 
                headers=headers
            )
        except:
            return []
        
        products = products.json()
        if not products or 'items' not  in products:
            return []
        
        return products['items']


    def end(self):
        global current_fbs
        global current_fbo

        current_fbo = current_fbs = None

if __name__ == '__main__':
    date_to = '2024-03-24T14:15:22Z'
    date_from = '2024-07-8T14:15:22Z'
    performance_client_id = '30757988-1719325107981@advertising.performance.ozon.ru'
    performance_client_key = "-PPvEshjvyZA60idYi6xs6VBaUgA31RlerMcb7z4cJzXGyvhm33kEUFw9Z7qrhHT6IqYuISbuooeIowViw"
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    # data = create_ads_report(performance_client_id, performance_client_key, date_to, date_from)
    parser = ParserSession(client_id, client_key, date_from, date_to)
    d = parser.__get_daily_ads_report()
    logger.info(d)