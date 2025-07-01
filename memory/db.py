import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Tuple

DB_NAME = "metadata.db"

class MemoryDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row # Allows accessing columns by name
        self._create_tables()

    def close(self):
        if self.conn:
            self.conn.close()

    def _create_tables(self):
        cursor = self.conn.cursor()
        # file_hash is the primary key for uniqueness across all managed files
        # original_path: path where it was found/imported from
        # current_path: path within the .memory home folder
        # uploaded_s3, uploaded_gcloud, uploaded_azure: boolean flags
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_hash TEXT PRIMARY KEY,
                original_filename TEXT NOT NULL,
                current_filename TEXT NOT NULL,
                original_path TEXT NOT NULL,
                current_path TEXT NOT NULL,
                size INTEGER NOT NULL,
                media_type TEXT,
                date_added TEXT NOT NULL,
                extracted_metadata TEXT, -- Store JSON string of EXIF/video metadata
                uploaded_s3 BOOLEAN DEFAULT FALSE,
                uploaded_gcloud BOOLEAN DEFAULT FALSE,
                uploaded_azure BOOLEAN DEFAULT FALSE,
                metadata_extracted BOOLEAN DEFAULT FALSE,
                perceptual_hash TEXT
            )
        ''')
        self.conn.commit()

    def add_file_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Adds or updates file metadata. Returns True if added/updated, False if duplicate."""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO files (file_hash, original_filename, current_filename, original_path,
                                   current_path, size, media_type, date_added, extracted_metadata, metadata_extracted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metadata['file_hash'],
                metadata['original_filename'],
                metadata['current_filename'],
                metadata['original_path'],
                metadata['current_path'],
                metadata['size'],
                metadata['media_type'],
                metadata['date_added'],
                metadata.get('extracted_metadata', None),
                metadata.get('metadata_extracted', False)
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # This means file_hash already exists, it's a duplicate
            return False

    def get_file_by_hash(self, file_hash: str) -> Dict[str, Any] | None:
        """Retrieves file metadata by hash."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_file_hashes(self) -> List[str]:
        """Returns a list of all managed file hashes."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT file_hash FROM files")
        return [row[0] for row in cursor.fetchall()]

    def get_unuploaded_files(self, cloud_target: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves files not yet uploaded to a specific cloud target or any cloud.
        """
        cursor = self.conn.cursor()
        query = "SELECT * FROM files WHERE "
        if cloud_target == "s3":
            query += "uploaded_s3 = FALSE"
        elif cloud_target == "gcloud":
            query += "uploaded_gcloud = FALSE"
        elif cloud_target == "azure":
            query += "uploaded_azure = FALSE"
        else: # For --dryrun, list all unuploaded to *any* cloud
            query += "uploaded_s3 = FALSE OR uploaded_gcloud = FALSE OR uploaded_azure = FALSE"
        
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def mark_uploaded(self, file_hash: str, cloud_target: str):
        """Marks a file as uploaded to a specific cloud target."""
        cursor = self.conn.cursor()
        if cloud_target == "s3":
            cursor.execute("UPDATE files SET uploaded_s3 = TRUE WHERE file_hash = ?", (file_hash,))
        elif cloud_target == "gcloud":
            cursor.execute("UPDATE files SET uploaded_gcloud = TRUE WHERE file_hash = ?", (file_hash,))
        elif cloud_target == "azure":
            cursor.execute("UPDATE files SET uploaded_azure = TRUE WHERE file_hash = ?", (file_hash,))
        self.conn.commit()
