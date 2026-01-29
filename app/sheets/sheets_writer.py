from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime


class GoogleSheetsWriter:

    def __init__(self, spreadsheet_id: str, service_account_file: str):
        self.creds = Credentials.from_service_account_file(
            service_account_file,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        self.service = build("sheets", "v4", credentials=self.creds)
        self.spreadsheet_id = spreadsheet_id

    def _get_sheet_id_by_name(self, sheet_name: str):
        spreadsheet = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id
        ).execute()

        for sheet in spreadsheet["sheets"]:
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]

        raise Exception(f"Sheet not found: {sheet_name}")

    def write_block(self, sheet_name: str, start_row: int, start_col: int, data: list):
        """
        Writes a 2D list to the given sheet starting at row & column.
        start_row and start_col are 1-based (like Google Sheets).
        """

        if not data:
            return

        range_notation = f"{sheet_name}!{self._col_letter(start_col)}{start_row}"

        body = {
            "values": data
        }

        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_notation,
            valueInputOption="RAW",
            body=body
        ).execute()

    def write_single_column(self, sheet_name: str, start_row: int, start_col: int, values: list):
        """
        Writes a single column list.
        """
        data = [[v] for v in values]
        self.write_block(sheet_name, start_row, start_col, data)

    def _col_letter(self, col_num: int):
        """
        Converts column number to letter (1 -> A, 27 -> AA)
        """
        result = ""
        while col_num:
            col_num, remainder = divmod(col_num - 1, 26)
            result = chr(65 + remainder) + result
        return result
