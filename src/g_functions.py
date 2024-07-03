import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from time import sleep
from loguru import logger

# Путь к файлу JSON с учетными данными сервисного аккаунта
SERVICE_ACCOUNT_FILE = 'key.json'

# Области доступа
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# Авторизация
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Авторизация для gspread
gc = gspread.authorize(credentials)

# Авторизация для Google Drive API
drive_service = build('drive', 'v3', credentials=credentials)

def put_data_in_ws(data: list[list], worksheet_name: str, spreadsheet_id: str):
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # Открываем указанный лист по его названию
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"Worksheet '{worksheet_name}' not found. Creating a new worksheet...")
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="20")

    # Записываем данные в Google Sheet
    worksheet.clear()
    
    worksheet.insert_row([], index=1)
    worksheet.insert_row([], index=2)
    
    index = 0
    
    for i, row in enumerate(data):
        worksheet.insert_row(row, index=3 + i)
        index = i
    
    worksheet.insert_row(['Дата актуализации:', f'{datetime.now()}'], index=3 + index + 3)
    

def create_worksheet(worksheet_name: str, spreadsheet_id: str, count_rows: int, count_columns: int) -> None:
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=count_rows + 1000, cols=count_columns + 1000)
    worksheet.clear()
    
    logger.info(f"Worksheet with name {worksheet_name} has created!")
    

def create_spreadsheet(marketplace: str) -> str: # This func return spreadsheet_id
    # Создаем новый Google Sheet
    spreadsheet = gc.create(f'{marketplace}_{datetime.now()}')

    # ID созданного Google Sheet
    spreadsheet_id = spreadsheet.id

    # ID папки Google Drive, в которую нужно поместить файл
    folder_id = '16OxeWAKGglrIwix1mlKdDuRBmBN7MdZZ'

    # Перемещаем файл в указанную папку
    file = drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
    previous_parents = ",".join(file.get('parents'))
    file = drive_service.files().update(
        fileId=spreadsheet_id,
        addParents=folder_id,
        removeParents=previous_parents,
        fields='id, parents'
    ).execute()
    
    logger.info('Spreadsheet created successfully!')

    return spreadsheet_id


def remove_columns_from_ws(worksheet_name: str, spreadsheet_id: str, start_column: int, end_column=None):
    """
    Удаляет столбцы из Google Sheets, начиная с start_column по end_column.

    Args:
    - worksheet_name (str): Название листа в Google Sheets.
    - spreadsheet_id (str): ID таблицы Google Sheets.
    - start_column (int): Номер первого столбца для удаления (начиная с 1).
    - end_column (int, optional): Номер последнего столбца для удаления (включительно, начиная с 1). 
                                  Если не указан, будут удалены все столбцы, начиная с start_column.

    Note:
    - Если end_column не указан, будут удалены все столбцы, начиная с start_column до конца таблицы.
    """
    # Открываем таблицу
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # Получаем лист
    worksheet = spreadsheet.worksheet(worksheet_name)
    
    # Читаем все данные из листа
    all_data = worksheet.get_all_values()
    
    # Если end_column не указан, устанавливаем его в количество столбцов в таблице
    if end_column is None:
        end_column = worksheet.col_count
    
    # Удаляем столбцы из данных
    cleaned_data = []
    for row in all_data:
        # Оставляем только нужные столбцы
        cleaned_row = row[:start_column - 1] + row[end_column:]
        cleaned_data.append(cleaned_row)
    
    # Очищаем лист и записываем очищенные данные
    worksheet.clear()
    for row in cleaned_data:
        worksheet.append_row(row)