from __future__ import annotations
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from time import sleep
from loguru import logger
from xlsxwriter.utility import xl_col_to_name, xl_cell_to_rowcol


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

class GSheet:

    def __init__(self, spreadsheet, dateStart: datetime, dateEnd: datetime):
        self.__spreadsheet = spreadsheet
        self.__worksheets = {}
        self.__date_start = dateStart
        self.__date_end = dateEnd

    def create(name: str, startDate: datetime, endDate: datetime) -> GSheet:
        # Создаем новый Google Sheet
        spreadsheet = gc.create(name)

        # ID созданного Google Sheet
        spreadsheet_id = spreadsheet.id
        
        '''
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
        '''

        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={
                "type": "user",
                "role": "writer",
                "emailAddress": "semehich212166@gmail.com"
            },
            fields="id",
        ).execute()
        logger.debug('The spreadshet is: https://docs.google.com/spreadsheets/d/{}/edit?gid=0'.format(spreadsheet_id))

        return GSheet(spreadsheet, startDate, endDate)

    def create_worksheet(self, worksheet_name: str, count_rows: int, count_columns: int) -> None:
        self.__worksheets[worksheet_name] = self.__spreadsheet.add_worksheet(title=worksheet_name, rows=count_rows + 1000, cols=count_columns + 1000)
        self.__worksheets[worksheet_name].clear()

        try:    
            first_sh = self.__spreadsheet.worksheet('Sheet1')
            self.__spreadsheet.del_worksheet(first_sh)
        except: pass

        logger.info(f"Worksheet with name {worksheet_name} has created!")

    def __get_worksheet(self, worksheet_name: str, create_on_fail: bool = False):
        if worksheet_name not in self.__worksheets:
            try:
                self.__worksheets[worksheet_name] = self.__spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound as e:
                if create_on_fail:
                    logger.warning(f"Worksheet '{worksheet_name}' not found. Creating a new worksheet...")
                    self.__worksheets[worksheet_name] = self.__spreadsheet.add_worksheet(
                        title=worksheet_name, rows="100", cols="20")
                else:
                    raise(e)
                
        return self.__worksheets[worksheet_name]

    def put_data_in_ws(self, data: list[list], worksheet_name: str, actualization_date: datetime = None):
        
        worksheet = self.__get_worksheet(worksheet_name, True)
        # Записываем данные в Google Sheet
        worksheet.clear()
        self.__put_header(data, actualization_date)

        last_row, last_column = self.__get_last_row_and_column(data)
        r = f'A1:{last_column}{last_row}'
        worksheet.update(data, r)
        self.__set_format(worksheet, last_column, last_row)

    def __put_header(self, data: list, actualization_date: datetime|None):

        data.insert(0, ['Дата конца выгрузки', self.__date_end.strftime("%d.%m.%Y")])
        data.insert(0, ['Дата начала выгрузки', self.__date_start.strftime("%d.%m.%Y")])

        if not actualization_date:
            actualization_date = datetime.now()
        actualization_date = actualization_date.strftime("%d.%m.%Y %H:%M")

        data.insert(0, ['Актуальность данных:', f'{actualization_date}'])     


    def __get_last_row_and_column(self, data: dict) -> tuple[int, int]:
        last_row = len(data) + 2

        # the last row contains input values, so it must be the longest. Otherwise 2 is length of the last row(actualization date)
        last_column = xl_col_to_name(max(len(data[-1]) + 1, 2)) 
        return last_row, last_column

    def __set_format(self, worksheet, last_column: str, last_row: int):
        worksheet.format(f"A1:{last_column}4",{
            "textFormat": {
                "bold": True
            }
        })
        worksheet.rows_auto_resize(0, last_row)
        worksheet.columns_auto_resize(0, xl_cell_to_rowcol(f'{last_column}1')[1])

    def remove_columns_from_ws(self, worksheet_name: str, start_column: int, end_column=None):
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
        # Получаем лист
        worksheet = self.__get_worksheet(worksheet_name)
        
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
        last_row, last_column = self.__get_last_row_and_column(cleaned_data)
        worksheet.update(cleaned_data, f'A1:{last_column}{last_row}')