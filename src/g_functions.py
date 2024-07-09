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
        spreadsheet = gc.open_by_key("1mRDcne9rdAqkHSXuYuEAcdl0RxphVr0Hti_6Ua4ZHUg")

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
        logger.debug('The spreadshet is: https://docs.google.com/spreadsheets/d/{}/edit?gid=0'.format(spreadsheet_id))

        return GSheet(spreadsheet, startDate, endDate)

    def __get_worksheet(self, worksheet_name: str, create_on_fail: bool = False):
        if worksheet_name not in self.__worksheets:
            self.__worksheets[worksheet_name] = self.__spreadsheet.worksheet(worksheet_name)

                
        return self.__worksheets[worksheet_name]

    def put_data_in_ws(self, data: list[list], worksheet_name: str):
        
        worksheet = self.__get_worksheet(worksheet_name, True)
        current_data = worksheet.get_all_values()
        data = self.__deleteDuplicatesFrom(data, current_data)
        if not data:
            return

        dates = self.__get_dates(current_data)
        worksheet.update(dates, f'B1:B{len(dates)}')

        first_row, _ = self.__get_last_row_and_column(current_data)
        _, last_column = self.__get_last_row_and_column(data)
        last_row = first_row + len(data)
        r = f'A{first_row}:{last_column}{last_row+1}'

        worksheet.add_rows(len(data))
        worksheet.update(data, r)

    def __deleteDuplicatesFrom(self, data: list, check_by: list) -> list:
        for i in range(min(len(data), len(check_by))):
            while len(data[i]) < len(check_by[i]):
                data[i].append('')
            while len(data[i]) > len(check_by[i]):
                check_by[i].append('')

        data = set(tuple(
            tuple(list(map(str, i))) for i in data
        ))
        check_by = set(tuple(
            tuple(list(map(str, i))) for i in check_by
        ))
        return list(map(list, data - (data & check_by)))

    def __get_dates(self, data: list) -> list:
        res = []
        
        actualization_date = datetime.now()
        actualization_date = actualization_date.strftime("%d.%m.%Y %H:%M")
        
        if data[0][0] == 'Дата начала выгрузки':
            res.append([self.__date_start.strftime("%d.%m.%Y")])
            res.append([self.__date_end.strftime("%d.%m.%Y")])
        res.append([actualization_date])
        
        return res

    def __get_last_row_and_column(self, data: dict) -> tuple[int, int]:
        last_row = len(data) + 1

        # the last row contains input values, so it must be the longest. Otherwise 2 is length of the last row(actualization date)
        last_column = xl_col_to_name(max(list(map(len, data))) + 1) 
        return last_row, last_column