from src.r_functions import create_products_report
from src.g_functions import create_spreadsheet, create_worksheet, put_data_in_ws, remove_columns_from_ws

def run(marketplace, client_id, client_key):
    spreadsheet_id = create_spreadsheet(marketplace)

    data = create_products_report(client_id, client_key)
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1

    create_worksheet("Товары", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Товары", spreadsheet_id)
    
    remove_columns_from_ws("Товары", spreadsheet_id, start_column=17, end_column=None)