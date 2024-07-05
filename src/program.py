from src.r_functions import create_products_report, create_returns_report, create_postings_report, create_supply_orders_report
from src.g_functions import create_spreadsheet, create_worksheet, put_data_in_ws, remove_columns_from_ws

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
    
def run(marketplace, client_id, client_key):
    spreadsheet_id = create_spreadsheet(marketplace)

    data = create_products_report(client_id, client_key)
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1

    create_worksheet("Товары", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Товары", spreadsheet_id)
    
    remove_columns_from_ws("Товары", spreadsheet_id, start_column=17, end_column=None)
    
    data = create_returns_report(client_id, client_key, 'fbo')

    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1
    
    create_worksheet("Возвраты FBO", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Возвраты FBO", spreadsheet_id)
    
    data = create_returns_report(client_id, client_key, 'fbs')
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1
    
    create_worksheet("Возвраты FBS", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Возвраты FBS", spreadsheet_id)
    
    data = create_postings_report(client_id, client_key, 'fbo')
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1
    
    create_worksheet("Продажи FBO", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Продажи FBO", spreadsheet_id)
    
    remove_columns_from_ws("Продажи FBO", spreadsheet_id, start_column=18, end_column=None)
    
    data = create_postings_report(client_id, client_key, 'fbs')
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1
    
    create_worksheet("Продажи FBS", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Продажи FBS", spreadsheet_id)
    
    remove_columns_from_ws("Продажи FBS", spreadsheet_id, start_column=18, end_column=None)
    
    data = create_supply_orders_report(client_id, client_key)
    
    count_rows_data = len(data)
    count_rows = count_rows_data if count_rows_data > 0 else 1
    count_columns = len(data[0]) if count_rows > 0 else 1
    
    create_worksheet("Заявки на поставку", spreadsheet_id, count_rows, count_columns)
    
    put_data_in_ws(data, "Заявки на поставку", spreadsheet_id)
