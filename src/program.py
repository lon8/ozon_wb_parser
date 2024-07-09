from datetime import datetime
from r_functions import ParserSession
from g_functions import GSheet
from loguru import logger
import dateutil.parser
# def run(marketplace, client_id, client_key):
#     spreadsheet_id = create_spreadsheet(marketplace)

#     data = create_products_report(client_id, client_key)
    
#     count_rows_data = len(data)
#     count_rows = count_rows_data if count_rows_data > 0 else 1
#     count_columns = len(data[0]) if count_rows > 0 else 1

#     create_worksheet("Товары", spreadsheet_id, count_rows, count_columns)
    
#     put_data_in_ws(data, "Товары", spreadsheet_id)
    
#     remove_columns_from_ws("Товары", spreadsheet_id, start_column=17, end_column=None)
    
#     create_worksheet("Товары", spreadsheet_id, count_rows, count_columns)
    
def run(marketplace, market, performance_key, performance_secret, client_id, client_key, startDate, endDate):

    spreadsheet = GSheet.create(
        f'{marketplace}_{datetime.now()}',
        dateutil.parser.isoparse(startDate),
        dateutil.parser.isoparse(endDate) 
        
    )
    parser = ParserSession(client_id, client_key, startDate, endDate, performance_key, performance_secret)

    execute_statistics_parsing(spreadsheet, {
        "Доступность товаров": {
            "GetData": parser.get_products_awailability
        },
        "Размещение": {
            "GetData": parser.create_supply_report
        },
        "Реклама": {
            "GetData": parser.create_ads_report
        },
        "Начисления по товарам":{
            "GetData": parser.get_order_incomes
        },
        "Товары":{
            "GetData": parser.create_products_report,
        },
        "Возвраты": {
            "GetData": lambda: parser.create_returns_report('fbo') + parser.create_returns_report('fbs')[1:]
        },
        "Продажи": {
            "GetData": lambda: parser.create_postings_report('fbo') + parser.create_postings_report('fbs')[1:],
        },
        "Заявки на поставку": {
            "GetData": parser.create_supply_orders_report
        },
    })

    parser.end()

def execute_statistics_parsing(spreadsheet: GSheet, callbacks: dict):
    '''The func goes through callbacks dict which includes:
        {
            "SheetName": {
                "GetData": function (required)
            }
        }
        And executes them
    '''
    for sheet_name in callbacks:
        try:
            data = callbacks[sheet_name]["GetData"]()
        except Exception as e:
            logger.exception(f'Failed to parse {sheet_name}')
            data = None
        if data:
            try:
                spreadsheet.put_data_in_ws(data, sheet_name)
            except Exception as e:
                logger.exception(f'Failed to save {sheet_name}')
                
def get_data_rows_and_columns_count(data: dict) -> tuple[int, int]:
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1

    return count_rows, count_columns

if __name__ == '__main__':
    run('ozon', '550209', '23448f62-23dd-4156-8a60-76525944756c', '2024-06-10T00:00:00Z', '2024-07-10T00:00:00Z')