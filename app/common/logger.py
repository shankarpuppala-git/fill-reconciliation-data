from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


class GoogleDocsLogger:
    def __init__(self, folder_id: str, service_account_file: str):
        self.folder_id = folder_id
        self.creds = Credentials.from_service_account_file(
            service_account_file,
            scopes=[
                "https://www.googleapis.com/auth/documents",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        self.docs_service = build("docs", "v1", credentials=self.creds)
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.document_id = self._create_log_document()

    def _create_log_document(self):
        title = f"Reconciliation_Log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        doc = self.docs_service.documents().create(
            body={"title": title}
        ).execute()

        document_id = doc["documentId"]

        # move document to folder
        self.drive_service.files().update(
            fileId=document_id,
            addParents=self.folder_id,
            removeParents="root",
            fields="id, parents"
        ).execute()

        return document_id

    def log(self, level: str, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = f"[{timestamp}] {level.upper()}  {message}\n"

        self.docs_service.documents().batchUpdate(
            documentId=self.document_id,
            body={
                "requests": [
                    {
                        "insertText": {
                            "location": {"index": 1},
                            "text": content
                        }
                    }
                ]
            }
        ).execute()
