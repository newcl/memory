#!/bin/bash

# --- 0. Project Overview ---
# Memory: A command-line tool to manage your local photo/video collection.
# It handles de-duplication, organization, and prepares files for cloud upload.
#
# Key Features:
# - Pure Python Implementation (for now).
# - SQLite for metadata storage: easy querying and self-sufficient management.
# - Local-first approach: uses your current folder as the "home folder."
# - Duplicate prevention: ensures only unique files are managed.
# - Incremental imports: only new files are processed.
# - Cloud upload readiness: includes dry-run and simulated upload capabilities.

# --- 1. Create Project Directory and Navigate Into It ---
echo "Creating project directory 'memory_project'..."
mkdir memory_project
cd memory_project
echo "Navigated to $(pwd)"

# --- 2. Create Python Package Structure ---
echo "Creating 'memory' Python package structure..."
mkdir memory
touch memory/__init__.py
touch memory/cli.py
touch memory/core.py
touch memory/db.py
touch memory/hasher.py
touch memory/media.py
touch memory/utils.py

# --- 3. Create pyproject.toml ---
echo "Creating pyproject.toml..."
cat << 'EOF' > pyproject.toml
[project]
name = "memory-cli"
version = "0.1.0"
description = "A personal media manager for photos, videos, etc."
authors = [
    { name = "Your Name", email = "your.email@example.com" },
]
dependencies = [
    "click>=8.0.0",
    "Pillow>=10.0.0", # For image metadata (EXIF)
]
readme = "README.md"
requires-python = ">=3.9"

[project.scripts]
memory = "memory.cli:cli"

[build-system]
requires = ["setuptools>=61.0.0"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["."]
EOF
echo "pyproject.toml created."

# --- 4. Create .gitignore ---
echo "Creating .gitignore..."
cat << 'EOF' > .gitignore
# Python
__pycache__/
*.pyc
.pytest_cache/
.mypy_cache/
.venv/
venv/

# Memory-specific
.memory/
*.db
EOF
echo ".gitignore created."

# --- 5. Populate memory/utils.py ---
echo "Populating memory/utils.py..."
cat << 'EOF' > memory/utils.py
import datetime
import hashlib
from pathlib import Path

def generate_timestamp_suffix():
    """Generates a readable timestamp suffix for filenames."""
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_file_hash(filepath: Path, algorithm: str = 'sha256', buffer_size: int = 65536) -> str:
    """
    Calculates the hash of a file.
    """
    h = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        while chunk := f.read(buffer_size):
            h.update(chunk)
    return h.hexdigest()

def get_media_type(filepath: Path) -> str | None:
    """
    Determines if a file is a known media type (photo/video).
    Extend this list as needed.
    """
    ext = filepath.suffix.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']:
        return "photo"
    elif ext in ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.flv']:
        return "video"
    # Add other media types as needed
    return None
EOF
echo "memory/utils.py created."

# --- 6. Populate memory/db.py ---
echo "Populating memory/db.py..."
cat << 'EOF' > memory/db.py
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
                uploaded_azure BOOLEAN DEFAULT FALSE
            )
        ''')
        self.conn.commit()

    def add_file_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Adds or updates file metadata. Returns True if added/updated, False if duplicate."""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO files (file_hash, original_filename, current_filename, original_path,
                                   current_path, size, media_type, date_added, extracted_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metadata['file_hash'],
                metadata['original_filename'],
                metadata['current_filename'],
                metadata['original_path'],
                metadata['current_path'],
                metadata['size'],
                metadata['media_type'],
                metadata['date_added'],
                metadata.get('extracted_metadata', None)
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
EOF
echo "memory/db.py created."

# --- 7. Populate memory/hasher.py ---
echo "Populating memory/hasher.py..."
cat << 'EOF' > memory/hasher.py
from pathlib import Path
from memory.utils import get_file_hash

def calculate_file_hash(filepath: Path) -> str:
    """
    Calculates the SHA256 hash of a file.
    This implementation is pure Python.
    """
    return get_file_hash(filepath, algorithm='sha256')
EOF
echo "memory/hasher.py created."

# --- 8. Populate memory/media.py ---
echo "Populating memory/media.py..."
cat << 'EOF' > memory/media.py
import json
from pathlib import Path
from PIL import Image, UnidentifiedImageError # type: ignore
from typing import Dict, Any

def extract_image_metadata(filepath: Path) -> Dict[str, Any]:
    """Extracts basic EXIF metadata from an image using Pillow."""
    metadata = {}
    try:
        with Image.open(filepath) as img:
            if hasattr(img, '_getexif'):
                exif_data = img._getexif()
                if exif_data:
                    from PIL.ExifTags import TAGS
                    for tag, value in exif_data.items():
                        decoded = TAGS.get(tag, tag)
                        if isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8', errors='ignore')
                            except UnicodeDecodeError:
                                pass # Keep as bytes if it can't be decoded
                        metadata[decoded] = value
    except UnidentifiedImageError:
        pass
    except Exception as e:
        print(f"Warning: Could not extract image metadata from {filepath}: {e}")
    return metadata

def extract_video_metadata(filepath: Path) -> Dict[str, Any]:
    """
    Placeholder for video metadata extraction using pure Python.
    This would involve libraries like:
    - hachoir-parser/hachoir-metadata (older, pure Python)
    For more robust video metadata, you'd typically interface with external tools
    like FFmpeg's ffprobe, but for a pure Python approach, capabilities are limited.
    """
    print(f"Note: Basic video metadata extraction for {filepath} is a placeholder.")
    return {}

def get_media_metadata(filepath: Path, media_type: str) -> str:
    """Extracts metadata based on media type and returns it as a JSON string."""
    metadata = {}
    if media_type == "photo":
        metadata = extract_image_metadata(filepath)
    elif media_type == "video":
        metadata = extract_video_metadata(filepath)
    
    return json.dumps(metadata) # Store as JSON string in DB
EOF
echo "memory/media.py created."

# --- 9. Populate memory/core.py ---
echo "Populating memory/core.py..."
cat << 'EOF' > memory/core.py
import os
import shutil
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from memory.db import MemoryDB, DB_NAME
from memory.hasher import calculate_file_hash
from memory.utils import generate_timestamp_suffix, get_media_type
from memory.media import get_media_metadata # Ensure this is imported

MEMORY_FOLDER_NAME = ".memory"

def _get_home_folder_path() -> Path:
    return Path.cwd()

def _get_memory_path() -> Path:
    return _get_home_folder_path() / MEMORY_FOLDER_NAME

def _get_db_path() -> Path:
    return _get_memory_path() / DB_NAME

def init_memory():
    """Initializes the .memory folder and database."""
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if memory_path.exists():
        print(f"Error: '{MEMORY_FOLDER_NAME}' already exists in '{home_folder}'. Aborting init.")
        return False

    print(f"Initializing Memory in '{home_folder}'...")
    memory_path.mkdir(parents=True, exist_ok=True)
    print(f"Created '{memory_path}'.")

    db = MemoryDB(db_path)
    try:
        db.connect()
        print(f"Initialized database at '{db_path}'.")
    except Exception as e:
        print(f"Error initializing database: {e}")
        shutil.rmtree(memory_path) # Clean up on error
        return False
    finally:
        db.close()

    print("Scanning current folder for media files...")
    _scan_and_process_folder(home_folder, db_path, is_init=True)
    print("Memory initialization complete.")
    return True

def _scan_and_process_folder(
    source_folder: Path,
    db_path: Path,
    is_init: bool = False
):
    """
    Scans a folder for media files, processes them (hash, copy, metadata),
    and adds to the database.
    """
    db = MemoryDB(db_path)
    db.connect()
    try:
        managed_hashes = set(db.get_all_file_hashes())
        new_files_processed = 0

        for filepath in source_folder.iterdir():
            if filepath.is_file():
                # Avoid processing the .memory folder itself or its contents
                if MEMORY_FOLDER_NAME in filepath.parts:
                    continue

                media_type = get_media_type(filepath)
                if media_type:
                    file_hash = calculate_file_hash(filepath)

                    if file_hash in managed_hashes:
                        print(f"  Skipping '{filepath.name}': Duplicate file (hash: {file_hash}).")
                        continue

                    dest_folder = _get_home_folder_path()
                    
                    # Resolve potential filename conflicts within the destination (home) folder
                    current_filename = filepath.name
                    dest_path = dest_folder / current_filename
                    
                    # Only apply filename suffix if the destination file already exists AND it's not the same content
                    if dest_path.exists():
                        existing_file_hash = calculate_file_hash(dest_path)
                        if existing_file_hash == file_hash:
                            print(f"  Skipping '{filepath.name}': Already present as '{dest_path.name}'.")
                            continue # File already exists in home folder with same content

                        # Filename conflict with different content
                        suffix = generate_timestamp_suffix()
                        new_name = f"{filepath.stem}_{suffix}{filepath.suffix}"
                        dest_path = dest_folder / new_name
                        print(f"  Filename conflict for '{filepath.name}'. Copying as '{new_name}'.")

                    if is_init:
                        # For init, if the file is already in the current folder, we just index it.
                        # If it needs a new name due to conflict with another file already there,
                        # we'd copy it to the new name.
                        if filepath != dest_path: # Means filename conflict resolution led to new name
                             print(f"  Indexing and potentially renaming '{filepath.name}' to '{dest_path.name}'...")
                             shutil.copy2(filepath, dest_path) # Copy to new name
                             # Optionally remove original if you want strict management
                        else:
                             print(f"  Indexing '{filepath.name}'...")

                    else: # For import, we always copy from source_folder to dest_folder
                        print(f"  Copying '{filepath.name}' to '{dest_path.name}'...")
                        shutil.copy2(filepath, dest_path)

                    # Extract media metadata
                    extracted_metadata_json = get_media_metadata(filepath, media_type)

                    metadata = {
                        'file_hash': file_hash,
                        'original_filename': filepath.name,
                        'current_filename': dest_path.name,
                        'original_path': str(filepath),
                        'current_path': str(dest_path),
                        'size': filepath.stat().st_size,
                        'media_type': media_type,
                        'date_added': datetime.now().isoformat(),
                        'extracted_metadata': extracted_metadata_json
                    }

                    if db.add_file_metadata(metadata):
                        print(f"  Added '{dest_path.name}' to database.")
                        new_files_processed += 1
                        managed_hashes.add(file_hash) # Add to set for immediate future checks
                    else:
                        print(f"  Failed to add '{dest_path.name}' to database (should not happen if hash check passed).")
        
        if new_files_processed == 0:
            print("No new media files found.")
        else:
            print(f"Processed {new_files_processed} new media files.")

    except Exception as e:
        print(f"An error occurred during scan and process: {e}")
    finally:
        db.close()


def import_folder(source_folder_str: str):
    """Imports media files from a source folder into the memory home folder."""
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    source_folder = Path(source_folder_str).resolve() # Resolve to absolute path
    if not source_folder.is_dir():
        print(f"Error: Source folder '{source_folder_str}' does not exist or is not a directory.")
        return
    
    if source_folder == home_folder:
        print(f"Error: Cannot import '{source_folder_str}' into itself. Use 'memory init' for initial scan.")
        return

    print(f"Importing from '{source_folder}' into '{home_folder}'...")
    _scan_and_process_folder(source_folder, db_path)
    print("Import complete.")

def upload_dry_run():
    """Lists files that would be uploaded to cloud storage."""
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        unuploaded_files = db.get_unuploaded_files()
        if not unuploaded_files:
            print("No new files to upload.")
            return

        print("\n--- Files to be uploaded (Dry Run) ---")
        for file_meta in unuploaded_files:
            print(f"- {file_meta['current_filename']} (Size: {file_meta['size']} bytes, Hash: {file_meta['file_hash'][:8]}...)")
        print("--------------------------------------\n")
        print(f"Total: {len(unuploaded_files)} files would be uploaded.")
    except Exception as e:
        print(f"An error occurred during dry run: {e}")
    finally:
        db.close()

def upload_to_cloud(cloud_target: str):
    """Uploads new files to the specified cloud storage."""
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return
    
    if cloud_target not in ['s3', 'gcloud', 'azure']:
        print(f"Error: Invalid cloud target '{cloud_target}'. Choose from 's3', 'gcloud', 'azure'.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        unuploaded_files = db.get_unuploaded_files(cloud_target)
        if not unuploaded_files:
            print(f"No new files to upload to {cloud_target.upper()}.")
            return

        print(f"\n--- Uploading {len(unuploaded_files)} files to {cloud_target.upper()} ---")
        for i, file_meta in enumerate(unuploaded_files):
            file_path = Path(file_meta['current_path'])
            if not file_path.exists():
                print(f"Warning: File '{file_meta['current_filename']}' not found at '{file_path}'. Skipping.")
                continue

            print(f"  [{i+1}/{len(unuploaded_files)}] Uploading '{file_meta['current_filename']}'...")
            
            # --- THIS IS WHERE ACTUAL CLOUD UPLOAD LOGIC WOULD GO ---
            # For bootstrap, we'll simulate it.
            # In a real scenario, you'd use libraries like boto3 for S3,
            # google-cloud-storage for GCloud, azure-storage-blob for Azure.
            try:
                # Simulate upload success with a delay to mimic network operation
                import time
                time.sleep(0.5) # Simulate network latency
                print(f"    (Simulated upload successful for {file_meta['current_filename']})")
                db.mark_uploaded(file_meta['file_hash'], cloud_target)
            except Exception as e:
                print(f"    Error uploading '{file_meta['current_filename']}': {e}")
                # Potentially log this and continue or retry
            # --------------------------------------------------------

        print(f"Upload to {cloud_target.upper()} complete.")

    except Exception as e:
        print(f"An error occurred during upload: {e}")
    finally:
        db.close()
EOF
echo "memory/core.py created."

# --- 10. Populate memory/cli.py ---
echo "Populating memory/cli.py..."
cat << 'EOF' > memory/cli.py
import click
from memory import core

@click.group()
def cli():
    """A tool to manage your photo/video/etc. collection."""
    pass

@cli.command()
def init():
    """
    Initializes Memory in the current folder.
    Scans for media files and sets up the .memory database.
    """
    core.init_memory()

@cli.command('import') # Use 'import' as the command name, but func remains import_cmd
@click.argument('source_folder', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
def import_cmd(source_folder):
    """
    Imports media files from SOURCE_FOLDER into the Memory home folder.
    Only new files are copied and added to the database.
    """
    core.import_folder(source_folder)

@cli.command()
@click.option('--dryrun', is_flag=True, help="List files that would be uploaded without performing the upload.")
@click.argument('cloud_target', required=False, type=click.Choice(['s3', 'gcloud', 'azure']))
def upload(dryrun, cloud_target):
    """
    Uploads new files to cloud storage.
    Specify 's3', 'gcloud', or 'azure' as the target.
    """
    if dryrun:
        if cloud_target:
            click.echo(f"Warning: --dryrun option ignores the cloud_target '{cloud_target}'. Listing all unuploaded files.")
        core.upload_dry_run()
    elif cloud_target:
        core.upload_to_cloud(cloud_target)
    else:
        click.echo("Error: Please specify a cloud target (s3, gcloud, azure) or use --dryrun.")
        click.echo("Usage: memory upload [--dryrun] <cloud_target>")

if __name__ == '__main__':
    cli()
EOF
echo "memory/cli.py created."

# --- 11. Virtual Environment Setup and Installation ---
echo "Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
echo "Virtual environment activated."

echo "Installing project dependencies..."
pip install -e .
echo "Dependencies installed."

# --- 12. Usage Instructions ---
echo -e "\n--- Setup Complete! ---"
echo "You're now inside the 'memory_project' directory with your virtual environment active."
echo "Here's how to use your 'Memory' tool:"
echo ""
echo "## Usage Examples"
echo "To manage your media, use the 'memory' command:"
echo ""
echo "### 1. Initialize Memory"
echo "   This sets up the '.memory' folder and database in your current directory."
echo "   It also scans and indexes existing media files in this folder."
echo "   If '.memory' already exists, it'll abort safely."
echo "   Example: "
echo "   cd memory_project"
echo "   memory init"
echo ""
echo "### 2. Import Media from another Folder"
echo "   Copies new media files from a specified source folder into your 'Memory' home folder."
echo "   It intelligently skips duplicates."
echo "   Example: "
echo "   mkdir ~/my_old_photos # Create a dummy folder with some images/videos"
echo "   # Copy some test files into ~/my_old_photos"
echo "   memory import ~/my_old_photos"
echo ""
echo "### 3. See Files to be Uploaded (Dry Run)"
echo "   Lists all files that are managed by 'Memory' but haven't been marked as uploaded."
echo "   No actual files are sent to the cloud during this command."
echo "   Example: "
echo "   memory upload --dryrun"
echo ""
echo "### 4. Upload Files to Cloud Storage (Simulated)"
echo "   Initiates the upload of new files to a specified cloud service (S3, Google Cloud, Azure)."
echo "   For now, this is a simulated upload."
echo "   Example: "
echo "   memory upload s3"
echo "   memory upload gcloud"
echo "   memory upload azure"
echo ""
echo "### To exit the virtual environment:"
echo "   deactivate"
echo ""
echo "Enjoy managing your memories!"