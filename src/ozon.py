import requests
import csv
from loguru import logger
import dateutil.parser
from time import sleep

class Parser:

    def __init__(
        self, 
        client_id: str, 
        client_key: str, 
        date_from: str, date_to: str, 
        perf_client_id: str, 
        perf_client_key: str
    ):
        self.__client_id = client_id
        self.__client_key = client_key
        self.__beaver_token = None
        self.__date_from = date_from
        self.__date_to = date_to
        self.__perf_client_id = perf_client_id
        self.__perf_client_key = perf_client_key

        # cache
        self.__current_fbo = self.__current_fbs = None
        self.__goods_info = {}

        # datetime format dates and parsing period length
        self.__date_from_dt = dateutil.parser.isoparse(self.__date_from) 
        self.__date_to_dt = dateutil.parser.isoparse(self.__date_to)
        self.__period_days = self.__date_to_dt - self.__date_from_dt
        self.__period_days = self.__period_days.days
        
        # dates without time
        self.__date_from_truncated = self.__date_from[:10] 
        self.__date_to_truncated = self.__date_to[:10]


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
            
            response = self.__p("https://performance.ozon.ru/api/client/token", payload, headers)
            
            self.__beaver_token = response.json()['access_token']
        
        return self.__beaver_token


    def __download_and_get_csv(self, url :str) -> list[list]:
        try:
            response = self.__g(url)
            if response.status_code == 200:
                block_size = 1024  # 1 KB
                
                # Open a local file with 'wb' (write binary) mode
                with open("file.csv", 'wb') as f:
                    for data in response.iter_content(block_size):
                        f.write(data)
            
            with open('file.csv', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=";")
                data = list(reader)[1:]
                
            return data  # return the rows of csv-file
        except requests.exceptions.RequestException:
            logger.exception(f"Error downloading file {url}")
            return

    def __p(self, url: str, json: dict = None, headers: dict = None) -> requests.Response:
        '''POST'''
        return self.__r(requests.post, url, json, headers)
    
    def __g(self, url: str, json: dict = None, headers: dict = None) -> requests.Response:
        '''GET'''
        return self.__r(requests.get, url, json, headers)

    def __r(self, method, url: str, json: dict|None = None, headers: dict|None = None) -> requests.Response:
        '''requests.<method>'''
        if headers is None:
            if 'performance' in url:
                headers = {
                    'Authorization': f'Bearer {self.beaver_token}',
                    "Client-Id": self.__perf_client_id,
                    "Api-Key": self.__perf_client_key
                }
            elif 'api-seller' in url:
                headers = {
                    "Client-Id": self.__client_id,
                    "Api-Key": self.__client_key
                }

        response = method(url, headers=headers, json=json)
        try:
            response.raise_for_status()
        except Exception as e:
            logger.warning(response.json())
            raise e
        return response

    def __find_keys(self, response_json: dict, *keys: list[str]) -> dict:
        """
        Searches for nested keys in json and return the final value or an empty dict
        """
        for k in keys:
            if k not in response_json:
                return {}
            
            response_json = response_json[k]

        return response_json

    def __get_report_url(self, code: str) -> str:
        
        payload = {
            "code": code
        }
        
        response = self.__p('https://api-seller.ozon.ru/v1/report/info', payload)
        
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

        response = self.__p("https://api-seller.ozon.ru/v1/report/products/create", payload)

        if response.status_code == 200:
            data = response.json()
            code = data['result']['code']
            url = self.__get_report_url(code)
            result = self.__download_and_get_csv(url)
            return [i[:16] for i in result]
        else:
            logger.warning('Product report creating was failed')
            return -1

    def __prepare_return_report(self, data: list[dict], scope_type: str) -> list[list]:
        
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
            payload

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
                item['accepted_from_customer_moment'].replace('T', ' ').replace('Z', '').split('.')[0],
                '-' if not item['returned_to_ozon_moment'] else \
                    item['returned_to_ozon_moment'].replace('T', ' ').replace('Z', '').split('.')[0],
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
        
        response = self.__p(f"https://api-seller.ozon.ru/v3/returns/company/{scope_type}", payload)
        
        if response.status_code == 200:
            data = response.json()['returns']
            if not data:
                return []
        
            return self.__prepare_return_report(data, scope_type)
        

    def create_postings_report(self, scope_type: str) -> list[list]:
        if scope_type == 'fbs' and self.__current_fbs is not None:
            return self.__current_fbs
        if scope_type == 'fbo' and self.__current_fbo is not None:
            return self.__current_fbo

        payload = {
            "filter": {
                "processed_at_from": self.__date_from,
                "processed_at_to": self.__date_to,
                "delivery_schema": [scope_type]
            },
            "language": "DEFAULT"
        }
        
        response = self.__p('https://api-seller.ozon.ru/v1/report/postings/create', payload)
        response = response.json()
        if not response:
            return []
        
        code = response['result']['code']
        
        url = self.__get_report_url(code)
        res =  self.__download_and_get_csv(url)

        if scope_type == 'fbo':
            self.__current_fbo = res
        if scope_type == 'fbs':
            self.__current_fbs = res

        return res

    def create_supply_orders_report(self) -> list[list]:
        payload = {
            "page": 1,
            "page_size": 100
        }
        
        response = self.__p(
            'https://api-seller.ozon.ru/v1/supply-order/list', 
            payload
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
        left = self.__get_ads_report()
        right = self.__get_daily_ads_report()

        while len(left) < len(right):
            left.append([''] * len(left[0]))

        while len(left) > len(right):
            right.append([])
        
        for i in range(min(len(left), len(right))):
            left[i] += [''] + right[i]

        return left

    def __get_ads_report(self) -> list[list]:
        
        payload = {
            "from": f"{self.__date_from}",
            "to": f"{self.__date_to}"
        }

        response = self.__p( 
            'https://performance.ozon.ru:443/api/client/statistic/products/generate/json', 
            payload
        )
        data = self.__prepare_ads_report(response.json())
        
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

    def __prepare_ads_report(self, data: dict) -> list[list]:
        
        uuid = data['UUID']
        
        sleep(5)
        
        report_response = self.__g(f'https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}')
        return report_response.json()['report']['rows']
        
    def __get_daily_ads_report(self):


        response = self.__g(
            f"https://performance.ozon.ru/api/client/statistics/daily/json?dateFrom={self.__date_from_truncated}&dateTo={self.__date_to_truncated}")
            
        data = self.__find_keys(response.json(), 'rows')
        if not data:
            return []
                
        result = []
        for item in data:
            row = [
                item['date'],
                item['id'] if 'sku' not in item else item['sku'],
                item['title'] if 'offerId' not in item else item['offerId'],
                item['orders'],
                item['ordersMoney'],
                'Нет данных',
                item['avgBid'],
                item['moneySpent'],
                '-' if 'drr' not in item else item['drr']
            ]
            result.append(row)
        
        return result

    def __get_stock_on_warehouses(self):
        json = {
            "limit": 100,
            "offset": 0,
            "warehouse_type": "ALL"
        }

        response = self.__p('https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses', json)
        response = self.__find_keys(response.json(), 'result', 'rows')
        if not response:
            return []
        
        return response

    def get_order_incomes(self):
        json = {
            'filter': {
                "transaction_type": "all",
                'date': {
                    'from': self.__date_from,
                    'to': self.__date_to
                },
                "operation_type": [],
                "posting_number": "",
            },
            "page": 1,
            "page_size": 1000
        }

        response = self.__p('https://api-seller.ozon.ru/v3/finance/transaction/list', json)
        response = self.__find_keys(response.json(), 'result', 'operations')
        if not response:
            return []
        
        return self.__prepare_order_incomes(response)

    
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
    
        return {i: self.__get_wh_name(i) for i in warehouse_ids}       
    
    def __get_wh_name(self, wh_id: int) -> str:
        json = {
            "filter": {
                "provider_id": 0,
                "status": "",
                "warehouse_id": wh_id
            },
            "limit": 100,
            "offset": 0
        }
        res = self.__p('https://api-seller.ozon.ru/v1/delivery-method/list', json)
        res = self.__find_keys(res.json(), 'result')
        if not res:
            return 'Нет данных'

        return res[0]['name']

    def __getGoodsInfo(self, good_skus: list[str]) -> dict:
        good_skus = list(set(good_skus) - set(self.__goods_info.keys()))

        if not good_skus:
            return self.__goods_info.copy()
        
        payload = {
            "offer_id": [],
            "product_id": [],
            "sku": good_skus
        }
        products = self.__p(
            'https://api-seller.ozon.ru/v2/product/info/list', 
            payload
        )
        products = self.__find_keys(products.json(), 'result', 'items')
        if not products:
            return self.__goods_info
        
        commissions = self.__getGoodCommissions([i['offer_id'] for i in products])
        for p in products:
            self.__goods_info[p['sku']] = {
                'is_kgt': p['is_kgt'],
                'currency_code': p['currency_code'],
                'offer_id': p['offer_id'],         
                'name': p['name'],
                "price_indexes": p['price_indexes'],
                'commissions': {'value': 'Нет данных'} if not commissions else {'value': commissions[p['offer_id']]},
                'stocks': p['stocks']
            }
        
        for s in good_skus:
            if s not in self.__goods_info:
                self.__goods_info[p['sku']] = {
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

        return self.__goods_info.copy()

    def __getGoodCommissions(self, offer_id: list[str]) -> dict:
        try:
            info = self.__p('https://api-seller.ozon.ru/v4/product/info/prices', {
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
        
        res = self.__find_keys(info.json()['result'], 'items')
        if not res:
            return {}
        
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

        def get_stock(sku: int):
            for i in whs_stocks:
                if int(i['sku']) == sku:
                    return i['free_to_sell_amount']
            return 'Нет данных'

        info = self.__getGoodsInfo([int(i[10]) for i in goods])
        res = {}
        for g in goods:
            sku = int(g[10])
            stock = get_stock(sku)
            if sku not in res:
                res[sku] = [
                    g[9],
                    '-',
                    sku,
                    '-',
                    0,
                    '-' if not g[-1].isdigit() or not isinstance(stock, int) else stock * int(g[-1]),
                    stock,
                    '-',
                    '-',
                    '-', # прсомотры карточки
                    '-',
                    '-',
                    info[sku]['price_indexes']['price_index'],
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-'
                ]
            try: res[sku][4] += int(info[sku]['commissions']['value'])
            except: pass

        return list(res.values())

    def get_products_awailability(self):
        payload = {
            "page": 1,
            "page_size": 100,
            "states": ['READY_TO_SUPPLY', 'ACCEPTED_AT_SUPPLY_WAREHOUSE', 'IN_TRANSIT', 'COMPLETED']
        }
        supplies = self.__p(
            'https://api-seller.ozon.ru/v1/supply-order/list', 
            payload
        )
        supplies = self.__find_keys(supplies.json(), 'supply_orders')
        if not supplies:
            return []
        
        payload["supply_order_id"] = None

        goods_supply_num = {}
        supplie_order_states = {}

        for s in supplies:
            items = self.__get_supply_orders(s['supply_order_id'], payload)
            if not items:
                continue
            for i in items:
                supplie_order_states[i['sku']] = s['state']
                if i['sku'] not in goods_supply_num:
                    goods_supply_num[i['sku']] = 0
                goods_supply_num[i['sku']] += i['quantity']

        if not goods_supply_num:
            return []

        info = self.__getGoodsInfo(list(goods_supply_num.keys()))

        result = {}
        for g in goods_supply_num:
            if g not in info:
                continue
            if g not in result:
                result[g] = [
                    info[g]['offer_id'],
                    info[g]['name'],
                    '-',
                    g,
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    0,
                    0,
                    '-',
                    '-',
                    '-',
                    '-'
                ]
            try:
                result[g][9] += int(info[g]['stocks']['present']) 
                result[g][10] += int(goods_supply_num[g]) if supplie_order_states[g] != 'COMPLETED' else 0
            except: 
                pass

        return list(result.values())


    def __get_supply_orders(self, supply_order_id: int, json: dict) -> list:
        json['supply_order_id'] = int(supply_order_id)
        try:
            products = self.__p(
                'https://api-seller.ozon.ru/v1/supply-order/items', 
                json
            )
        except:
            return []
        
        products = self.__find_keys(products.json(), 'items')
        if not products:
            return []
        
        return products


    def create_supply_await_report(self):
        payload = {
            "offset": 0,
            "limit": 1000,
            "date_from": self.__date_from_truncated,
            "date_to": self.__date_to_truncated, 
            "metrics": ['revenue', 'ordered_units'],
            "dimensions": ["sku"]
        }
        supplies = self.__p(
            'https://api-seller.ozon.ru/v1/analytics/data', 
            payload
        )

        supplies = self.__find_keys(supplies.json(), 'result', 'data')
        if not supplies:
            return []
        
        g_info = self.__getGoodsInfo([int(s['dimensions'][0]['id']) for s in supplies])
        result = {}
        for s in supplies:
            sku = s['dimensions'][0]['id']
            if sku not in result:
                result[sku] = [
                    sku,
                    '-',
                    '-',
                    'Нет данных' if sku not in g_info else g_info[sku]['offer_id'],
                    s['dimensions'][0]['name'],
                    0, 
                    '-',
                    'Нет данных' if sku not in g_info else 0,
                    'Нет данных' if sku not in g_info else 0,
                    '-',
                    0,
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                    '-',
                ]
            result[sku][5] += int(s['metrics'][1])
            if sku in g_info:
                try: result[sku][7] += int(g_info[sku]['stocks']['present'])
                except: pass
                try: result[sku][8] += sum(list(g_info[sku]['stocks'].values()))
                except: pass
            result[sku][10] += s['metrics'][1] / self.__period_days

        return list(result.values())
        
    def create_index_localizatioons(self):
        payload = {
            "date": 
            {
                "from": self.__date_from,
                "to": self.__date_to
            },
            "posting_number": "",
            "transaction_type": "all"
        }
        transactions = self.__p(
            'https://api-seller.ozon.ru/v3/finance/transaction/totals', 
            payload
        ).json()

        transactions = self.__find_keys(transactions, 'result')
        if not transactions:
            return []
        
        return [[self.__date_from_truncated, self.__date_to_truncated] + list(transactions.values())]
    
    def create_turnover_report(self):
        pass

if __name__ == '__main__':
    date_to = '2024-03-24T14:15:22Z'
    date_from = '2024-07-8T14:15:22Z'
    performance_client_id = '32024124-1720821364404@advertising.performance.ozon.ru'
    performance_client_key = "XSwXFVjkp_GDnH5CmksqM8XQjq45-LVZxecVtp23K512CEDSy7yMUUZ7Uith3O-UokT-hCWd0MNjeZbS1g"
    client_id = "550209"
    client_key = "23448f62-23dd-4156-8a60-76525944756c"
    # data = create_ads_report(performance_client_id, performance_client_key, date_to, date_from)
    parser = Parser(client_id, client_key, date_from, date_to)
    d = parser.__get_daily_ads_report()
    logger.info(d)