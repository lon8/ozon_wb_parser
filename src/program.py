from datetime import datetime
from src.r_functions import ParserSession
from src.g_functions import GSheet
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
    
def run(marketplace, client_id, client_key, startDate, endDate):

    spreadsheet = GSheet.create(
        f'{marketplace}_{datetime.now()}',
        dateutil.parser.isoparse(startDate),
        dateutil.parser.isoparse(endDate) 
        
    )
    parser = ParserSession(client_id, client_key, startDate, endDate)

    execute_statistics_parsing(spreadsheet, {
        "Товары":{
            "GetData": parser.create_products_report,
            "RemoveColumns": lambda sheet: spreadsheet.remove_columns_from_ws(sheet, start_column=17, end_column=None)
        },
        "Возвраты": {
            "GetData": lambda: parser.create_returns_report('fbo') + parser.create_returns_report('fbs')[1:]
        },
        "Продажи": {
            "GetData": lambda: parser.create_postings_report('fbo') + parser.create_postings_report('fbs')[1:],
            "RemoveColumns": lambda sheet: spreadsheet.remove_columns_from_ws(sheet, start_column=18, end_column=None)
        },
        "Заявки на поставку": {
            "GetData": lambda: parser.create_supply_orders_report()
        }
    })

def execute_statistics_parsing(spreadsheet: GSheet, callbacks: dict):
    '''The func goes through callbacks dict which includes:
        {
            "SheetName": {
                "GetData": function (required),
                "RemoveColumns": function (optional, the func will pass SheetName as first argument by default)
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
                count_rows, count_columns = get_data_rows_and_columns_count(data)
                spreadsheet.create_worksheet(sheet_name, count_rows, count_columns)
                spreadsheet.put_data_in_ws(data, sheet_name)
                if "RemoveColumns" in callbacks[sheet_name]:
                    callbacks[sheet_name]["RemoveColumns"](sheet_name)
            except Exception as e:
                logger.exception(f'Failed to save {sheet_name}')
                
def get_data_rows_and_columns_count(data: dict) -> tuple[int, int]:
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1

    return count_rows, count_columns
