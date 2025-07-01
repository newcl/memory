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
