from ozon import Parser as OzonParser
from wildberries import Parser as WildberriesParser
from g_functions import GSheet
from loguru import logger
import dateutil.parser
    
def run(marketplace, spreadsheet_url, performance_key, performance_secret, client_id, client_key, startDate, endDate,
        background_tasks = None) -> str:

    try:
        spreadsheet = GSheet.create(
            spreadsheet_url,
            dateutil.parser.isoparse(startDate),
            dateutil.parser.isoparse(endDate) 
        )
        parser = (OzonParser if marketplace.lower() == 'ozon' else  WildberriesParser)(
            client_id, client_key, startDate, endDate, performance_key, performance_secret)
    except:
        logger.exception('Failed to get parser and spreadsheet:')

    def work():
        try:
            execute_statistics_parsing(spreadsheet, {
                "Реклама": {
                    "GetData": parser.create_ads_report
                },
                "Расчет поставок": {
                    "GetData": parser.create_supply_await_report
                },
                "Индекс локализации": {
                    "GetData": parser.create_index_localizatioons
                },
                "Размещение": {
                    "GetData": parser.create_supply_report
                },
                "Продажи": {
                    "GetData": lambda: parser.create_postings_report('fbo') + parser.create_postings_report('fbs'),
                },
                "Доступность товаров": {
                    "GetData": parser.get_products_awailability
                },
                "Начисления по товарам":{
                    "GetData": parser.get_order_incomes
                },
                "Товары":{
                    "GetData": parser.create_products_report,
                },
                "Возвраты": {
                    "GetData": lambda: parser.create_returns_report('fbo') + parser.create_returns_report('fbs')
                },
                "Заявки на поставку": {
                    "GetData": parser.create_supply_orders_report
                },
            })
        except:
            logger.exception('Failed to run program:')

    if background_tasks:
        background_tasks.add_task(work)
    else:
        work()

    return spreadsheet_url

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
            else:
                logger.info(f'{sheet_name} saved')
        else:
            logger.info(f'No data for {sheet_name}')

                
def get_data_rows_and_columns_count(data: dict) -> tuple[int, int]:
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1

    return count_rows, count_columns

if __name__ == '__main__':
    run('ozon', 'https://docs.google.com/spreadsheets/d/1mRDcne9rdAqkHSXuYuEAcdl0RxphVr0Hti_6Ua4ZHUg/edit?gid=681289397#gid=681289397', '32024124-1720821364404@advertising.performance.ozon.ru', 'XSwXFVjkp_GDnH5CmksqM8XQjq45-LVZxecVtp23K512CEDSy7yMUUZ7Uith3O-UokT-hCWd0MNjeZbS1g', '1781617', '13d53c41-b8db-4dad-a12b-d8f9343226a2', '2024-06-10T00:00:00Z', '2024-07-09T00:00:00Z')
