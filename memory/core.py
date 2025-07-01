import os
import shutil
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import sys
import logging
import urllib.parse
from PIL import Image
import imagehash
import cv2

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
    is_init: bool = False,
    logger=None,
    recursive: bool = True
):
    """
    Scans a folder for media files, processes them (hash, copy, metadata),
    and adds to the database.
    """
    import itertools
    db = MemoryDB(db_path)
    db.connect()
    try:
        managed_hashes = set(db.get_all_file_hashes())
        new_files_processed = 0

        file_counter = 0
        memory_path = _get_memory_path()
        if recursive:
            file_iter = source_folder.rglob('*')
        else:
            file_iter = source_folder.iterdir()
        for filepath in file_iter:
            if not filepath.is_file():
                continue
            file_counter += 1
            print(f"Processing file {file_counter} ...", end='\r', flush=True)

            # Avoid processing the .memory folder itself or its contents
            if MEMORY_FOLDER_NAME in filepath.parts:
                continue

            media_type = get_media_type(filepath)
            if media_type:
                file_hash = calculate_file_hash(filepath)

                if file_hash in managed_hashes:
                    msg = f"  Skipping '{filepath.name}': Duplicate file (hash: {file_hash})."
                    if logger: logger.info(msg)
                    continue

                # Always copy into .memory folder
                dest_folder = memory_path
                current_filename = filepath.name
                dest_path = dest_folder / current_filename

                # Only apply filename suffix if the destination file already exists AND it's not the same content
                if dest_path.exists():
                    existing_file_hash = calculate_file_hash(dest_path)
                    if existing_file_hash == file_hash:
                        msg = f"  Skipping '{filepath.name}': Already present as '{dest_path.name}'."
                        if logger: logger.info(msg)
                        continue # File already exists in .memory folder with same content

                    # Filename conflict with different content
                    suffix = generate_timestamp_suffix()
                    new_name = f"{filepath.stem}_{suffix}{filepath.suffix}"
                    dest_path = dest_folder / new_name
                    msg = f"  Filename conflict for '{filepath.name}'. Copying as '{new_name}'."
                    if logger: logger.info(msg)

                msg = f"  Copying '{filepath.name}' to '{dest_path.name}'..."
                if logger: logger.info(msg)
                shutil.copy2(filepath, dest_path)

                # Extract media metadata
                extracted_metadata_json = get_media_metadata(filepath, media_type)
                metadata_extracted = False
                try:
                    meta_obj = json.loads(extracted_metadata_json)
                    if meta_obj:
                        metadata_extracted = True
                except Exception:
                    metadata_extracted = False

                perceptual_hash = _get_perceptual_hash(filepath, media_type)
                metadata = {
                    'file_hash': file_hash,
                    'original_filename': filepath.name,
                    'current_filename': dest_path.name,
                    'original_path': _to_relative_path(filepath, memory_path),
                    'current_path': _to_relative_path(dest_path, memory_path),
                    'size': filepath.stat().st_size,
                    'media_type': media_type,
                    'date_added': datetime.now().isoformat(),
                    'extracted_metadata': extracted_metadata_json,
                    'metadata_extracted': metadata_extracted,
                    'perceptual_hash': perceptual_hash
                }

                if db.add_file_metadata(metadata):
                    msg = f"  Added '{dest_path.name}' to database."
                    if logger: logger.info(msg)
                    new_files_processed += 1
                    managed_hashes.add(file_hash)
                else:
                    msg = f"  Failed to add '{dest_path.name}' to database (should not happen if hash check passed)."
                    if logger: logger.info(msg)

        print(' ' * 60, end='\r')  # Clear the progress line
        if new_files_processed == 0:
            print("No new media files found.")
            if logger: logger.info("No new media files found.")
        else:
            print(f"Processed {new_files_processed} new media files.")
            if logger: logger.info(f"Processed {new_files_processed} new media files.")

    except Exception as e:
        print(f"An error occurred during scan and process: {e}")
        if logger: logger.error(f"An error occurred during scan and process: {e}")
    finally:
        db.close()

def _get_perceptual_hash(filepath: Path, media_type: str) -> str | None:
    try:
        if media_type == 'photo':
            with Image.open(filepath) as img:
                return str(imagehash.phash(img))
        elif media_type == 'video':
            cap = cv2.VideoCapture(str(filepath))
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if frame_count == 0:
                return None
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_count // 2)
            ret, frame = cap.read()
            if ret:
                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                return str(imagehash.phash(img))
    except Exception as e:
        pass
    return None

def import_folder(source_folder_str: str, recursive: bool = True):
    """Imports media files from a source folder into the memory home folder."""
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Memory not initialized in '{home_folder}'. Running 'init'...")
        if not init_memory():
            print("Failed to initialize memory. Aborting import.")
            return

    source_folder = Path(source_folder_str).resolve() # Resolve to absolute path
    if not source_folder.is_dir():
        print(f"Error: Source folder '{source_folder_str}' does not exist or is not a directory.")
        return
    
    if source_folder == home_folder:
        print(f"Error: Cannot import '{source_folder_str}' into itself. Use 'memory init' for initial scan.")
        return

    # Set up logging to file
    log_file = _get_home_folder_path() / 'memory_import.log'
    logging.basicConfig(filename=log_file, filemode='w', level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger('memory_import')

    print(f"Importing from '{source_folder}' into '{home_folder}'... (recursive={recursive})")
    logger.info(f"Importing from '{source_folder}' into '{home_folder}'... (recursive={recursive})")
    _scan_and_process_folder(source_folder, db_path, logger=logger, recursive=recursive)
    print("Import complete.")
    logger.info("Import complete.")

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
            file_path = _from_relative_path(file_meta['current_path'], memory_path)
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

def delete_memory():
    """
    Deletes all files tracked in the database, then deletes the .memory folder and its contents, undoing 'memory init'.
    """
    memory_path = _get_memory_path()
    db_path = _get_db_path()
    if not memory_path.exists():
        print(f"No '{MEMORY_FOLDER_NAME}' folder found in '{_get_home_folder_path()}'. Nothing to delete.")
        return
    # Delete all files tracked in the database
    try:
        db = MemoryDB(db_path)
        db.connect()
        cursor = db.conn.cursor()
        cursor.execute("SELECT current_path FROM files")
        files = cursor.fetchall()
        for row in files:
            file_path = _from_relative_path(row[0], memory_path)
            try:
                if file_path.exists():
                    file_path.unlink()
                    print(f"Deleted file: {file_path}")
                else:
                    print(f"File not found (already deleted?): {file_path}")
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
        db.close()
    except Exception as e:
        print(f"Error deleting files from database: {e}")
    # Now remove the .memory folder
    try:
        import shutil
        shutil.rmtree(memory_path)
        print(f"Deleted '{memory_path}'. Memory has been reset.")
    except Exception as e:
        print(f"Error deleting '{memory_path}': {e}")

def print_stats():
    """
    Print statistics about the managed files: total count, total size, metadata extraction rate, upload status.
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files")
        total_files, total_size = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) FROM files WHERE metadata_extracted = 1")
        metadata_extracted_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM files WHERE uploaded_s3 = 1 OR uploaded_gcloud = 1 OR uploaded_azure = 1")
        uploaded_count = cursor.fetchone()[0]

        not_uploaded_count = total_files - uploaded_count

        def human_readable_size(size_bytes):
            for unit in ['B','KB','MB','GB','TB','PB']:
                if size_bytes < 1024:
                    return f"{size_bytes:.2f} {unit}"
                size_bytes /= 1024
            return f"{size_bytes:.2f} PB"

        print("\n--- Memory Stats ---")
        print(f"Total files: {total_files}")
        print(f"Total size: {total_size} bytes ({human_readable_size(total_size)})")
        if total_files > 0:
            print(f"Files with metadata extracted: {metadata_extracted_count} ({metadata_extracted_count/total_files*100:.1f}%)")
            print(f"Files uploaded: {uploaded_count} ({uploaded_count/total_files*100:.1f}%)")
            print(f"Files not uploaded: {not_uploaded_count} ({not_uploaded_count/total_files*100:.1f}%)")

            # Per-format stats
            cursor.execute("SELECT current_filename, size FROM files")
            from collections import defaultdict
            format_counts = defaultdict(int)
            format_sizes = defaultdict(int)
            for filename, size in cursor.fetchall():
                ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
                format_counts[ext] += 1
                format_sizes[ext] += size
            print("\nPer-format statistics:")
            print(f"{'Format':<8} {'Count':>8} {'%Files':>8} {'Size':>14} {'%Size':>8}")
            for ext in sorted(format_counts, key=lambda x: (-format_counts[x], x)):
                count = format_counts[ext]
                size = format_sizes[ext]
                pct_files = count / total_files * 100
                pct_size = size / total_size * 100 if total_size > 0 else 0
                print(f"{ext or '(none)':<8} {count:8d} {pct_files:8.1f} {human_readable_size(size):>14} {pct_size:8.1f}")
        else:
            print("No files managed yet.")
        print("-------------------\n")
    except Exception as e:
        print(f"Error gathering stats: {e}")
    finally:
        db.close()

def delete_file_by_id(record_id):
    """
    Deletes a single file from the database and disk by its record id (file_hash).
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT current_path FROM files WHERE file_hash = ?", (record_id,))
        row = cursor.fetchone()
        if not row:
            print(f"No file found with record id (file_hash): {record_id}")
            return
        file_path = _from_relative_path(row[0], memory_path)
        try:
            if file_path.exists():
                file_path.unlink()
                print(f"Deleted file: {file_path}")
            else:
                print(f"File not found on disk (already deleted?): {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")
        cursor.execute("DELETE FROM files WHERE file_hash = ?", (record_id,))
        db.conn.commit()
        print(f"Deleted database record for file_hash: {record_id}")
    except Exception as e:
        print(f"Error deleting file by id: {e}")
    finally:
        db.close()

def detect_samesize(videos=False, photos=False):
    """
    List groups of files with the same file size (more than one per group).
    Restrict to videos or photos if specified.
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        query = "SELECT size, current_filename, current_path, media_type FROM files"
        cursor.execute(query)
        from collections import defaultdict
        size_groups = defaultdict(list)
        for size, filename, path, media_type in cursor.fetchall():
            if videos and media_type != 'video':
                continue
            if photos and media_type != 'photo':
                continue
            size_groups[size].append((filename, path))
        found = False
        for size, files in size_groups.items():
            if len(files) > 1:
                found = True
                print(f"\nSize: {size} bytes - {len(files)} files:")
                for filename, path in files:
                    abs_path = str(_from_relative_path(path, memory_path).resolve())
                    file_url = 'file://' + urllib.parse.quote(abs_path)
                    print(f"  {filename}  ({file_url})")
        if not found:
            print("No groups of files with the same size found.")
    except Exception as e:
        print(f"Error during same-size detection: {e}")
    finally:
        db.close()

def detect_visual(videos=False, photos=False, threshold=5):
    """
    Detect visually similar files using perceptual hashes. Restrict to videos/photos if specified.
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        query = "SELECT file_hash, current_filename, current_path, media_type, perceptual_hash FROM files WHERE perceptual_hash IS NOT NULL"
        cursor.execute(query)
        files = []
        for file_hash, filename, path, media_type, phash in cursor.fetchall():
            if videos and media_type != 'video':
                continue
            if photos and media_type != 'photo':
                continue
            files.append((file_hash, filename, path, media_type, phash))
        # Group by similarity
        from collections import defaultdict
        visited = set()
        groups = []
        for i, (fh1, fn1, p1, mt1, ph1) in enumerate(files):
            if fh1 in visited:
                continue
            group = [(fn1, p1)]
            visited.add(fh1)
            hash1 = imagehash.hex_to_hash(ph1)
            for j, (fh2, fn2, p2, mt2, ph2) in enumerate(files):
                if i == j or fh2 in visited:
                    continue
                hash2 = imagehash.hex_to_hash(ph2)
                if hash1 - hash2 <= threshold:
                    group.append((fn2, p2))
                    visited.add(fh2)
            if len(group) > 1:
                groups.append(group)
        if not groups:
            print("No visually similar files found.")
        else:
            for group in groups:
                print(f"\nVisually similar files:")
                for filename, path in group:
                    abs_path = str(_from_relative_path(path, memory_path).resolve())
                    file_url = 'file://' + urllib.parse.quote(abs_path)
                    print(f"  {filename}  ({file_url})")
    except Exception as e:
        print(f"Error during visual similarity detection: {e}")
    finally:
        db.close()

def populate_perceptual_hashes():
    """
    Populate missing perceptual hashes for all files in the database, with progress and logging.
    """
    import logging
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    log_file = _get_home_folder_path() / 'memory_populate_hash.log'
    logging.basicConfig(filename=log_file, filemode='w', level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger('memory_populate_hash')

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT file_hash, current_path, media_type FROM files WHERE perceptual_hash IS NULL OR perceptual_hash = ''")
        rows = cursor.fetchall()
        total = len(rows)
        if not rows:
            print("All files already have perceptual hashes.")
            logger.info("All files already have perceptual hashes.")
            return
        updated = 0
        for idx, (file_hash, path, media_type) in enumerate(rows, 1):
            print(f"Processing file {idx} of {total} ...", end='\r', flush=True)
            file_path = _from_relative_path(path, memory_path)
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}, skipping.")
                continue
            phash = _get_perceptual_hash(file_path, media_type)
            if phash:
                cursor.execute("UPDATE files SET perceptual_hash = ? WHERE file_hash = ?", (phash, file_hash))
                updated += 1
                logger.info(f"Populated perceptual hash for: {file_path}")
            else:
                logger.warning(f"Could not compute perceptual hash for: {file_path}")
        db.conn.commit()
        print(' ' * 60, end='\r')  # Clear the progress line
        print(f"Populated perceptual hashes for {updated} files.")
        logger.info(f"Populated perceptual hashes for {updated} files.")
    except Exception as e:
        print(f"Error during perceptual hash population: {e}")
        logger.error(f"Error during perceptual hash population: {e}")
    finally:
        db.close()

def migrate_files_to_memory():
    """
    Move all files referenced by current_path in the database into the .memory folder (if not already there), update current_path to the new relative path, and warn about any missing or conflicting files.
    """
    import shutil
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT file_hash, current_path FROM files")
        rows = cursor.fetchall()
        moved = 0
        for file_hash, current_path in rows:
            abs_path = _from_relative_path(current_path, memory_path)
            if abs_path.parent == memory_path:
                continue  # Already in .memory
            if not abs_path.exists():
                print(f"File not found: {abs_path}, skipping.")
                continue
            dest_path = memory_path / abs_path.name
            # Handle name conflicts
            if dest_path.exists():
                # If same file, skip; else, add suffix
                if calculate_file_hash(dest_path) == calculate_file_hash(abs_path):
                    print(f"File already exists in .memory: {dest_path}, skipping.")
                    continue
                else:
                    suffix = generate_timestamp_suffix()
                    dest_path = memory_path / f"{abs_path.stem}_{suffix}{abs_path.suffix}"
                    print(f"Filename conflict, moving as: {dest_path}")
            shutil.move(str(abs_path), str(dest_path))
            rel_path = _to_relative_path(dest_path, memory_path)
            cursor.execute("UPDATE files SET current_path = ? WHERE file_hash = ?", (rel_path, file_hash))
            moved += 1
        db.conn.commit()
        print(f"Moved {moved} files into .memory folder.")
    except Exception as e:
        print(f"Error during file migration: {e}")
    finally:
        db.close()

def migrate_files_table():
    """
    Check for and add any missing columns to the files table in the database.
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    expected_columns = {
        'file_hash': 'TEXT',
        'original_filename': 'TEXT',
        'current_filename': 'TEXT',
        'original_path': 'TEXT',
        'current_path': 'TEXT',
        'size': 'INTEGER',
        'media_type': 'TEXT',
        'date_added': 'TEXT',
        'extracted_metadata': 'TEXT',
        'uploaded_s3': 'BOOLEAN DEFAULT FALSE',
        'uploaded_gcloud': 'BOOLEAN DEFAULT FALSE',
        'uploaded_azure': 'BOOLEAN DEFAULT FALSE',
        'metadata_extracted': 'BOOLEAN DEFAULT FALSE',
        'perceptual_hash': 'TEXT'
    }
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(files);")
    existing_columns = {row[1] for row in cursor.fetchall()}
    added = 0
    for col, coltype in expected_columns.items():
        if col not in existing_columns:
            print(f"Adding missing column: {col}")
            cursor.execute(f"ALTER TABLE files ADD COLUMN {col} {coltype};")
            added += 1
    conn.commit()
    conn.close()
    if added == 0:
        print("No columns needed to be added. Schema is up to date.")
    else:
        print(f"Migration complete. {added} columns added.")
    # After column migration, migrate paths to relative
    migrate_paths_to_relative()
    migrate_files_to_memory()

def migrate_paths_to_relative():
    """
    Migrate all absolute paths in the database to relative paths (relative to the .memory folder) for current_path and original_path.
    """
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not memory_path.exists():
        print(f"Error: Memory not initialized in '{home_folder}'. Run 'memory init' first.")
        return

    db = MemoryDB(db_path)
    db.connect()
    try:
        cursor = db.conn.cursor()
        cursor.execute("SELECT file_hash, current_path, original_path FROM files")
        rows = cursor.fetchall()
        updated = 0
        for file_hash, current_path, original_path in rows:
            rel_current = _to_relative_path(_from_relative_path(current_path, memory_path), memory_path)
            rel_original = _to_relative_path(_from_relative_path(original_path, memory_path), memory_path)
            if rel_current != current_path or rel_original != original_path:
                cursor.execute("UPDATE files SET current_path = ?, original_path = ? WHERE file_hash = ?", (rel_current, rel_original, file_hash))
                updated += 1
        db.conn.commit()
        print(f"Migrated {updated} records to relative paths.")
    except Exception as e:
        print(f"Error during path migration: {e}")
    finally:
        db.close()

def scan_unmanaged_files(folder):
    """
    Scan the specified folder for files not under management and print stats by total number, total size, and by extension (case-insensitive).
    """
    folder = Path(folder).resolve()
    home_folder = _get_home_folder_path()
    memory_path = _get_memory_path()
    db_path = _get_db_path()

    if not folder.is_dir():
        print(f"Error: {folder} is not a directory.")
        return

    managed_hashes = set()
    if memory_path.exists():
        db = MemoryDB(db_path)
        db.connect()
        try:
            managed_hashes = set(db.get_all_file_hashes())
        except Exception as e:
            print(f"Error reading managed hashes: {e}")
        finally:
            db.close()

    total_files = 0
    total_size = 0
    from collections import defaultdict
    ext_counts = defaultdict(int)
    ext_sizes = defaultdict(int)

    import hashlib
    def get_sha256(path):
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    for file_path in folder.rglob('*'):
        if not file_path.is_file():
            continue
        try:
            file_hash = get_sha256(file_path)
        except Exception as e:
            print(f"Error hashing {file_path}: {e}")
            continue
        if file_hash in managed_hashes:
            continue
        total_files += 1
        size = file_path.stat().st_size
        total_size += size
        ext = file_path.suffix.lower().lstrip('.')
        ext_counts[ext] += 1
        ext_sizes[ext] += size

    def human_readable_size(size_bytes):
        for unit in ['B','KB','MB','GB','TB','PB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"

    print(f"\nUnmanaged files in {folder}:")
    print(f"Total files: {total_files}")
    print(f"Total size: {total_size} bytes ({human_readable_size(total_size)})")
    print(f"{'Extension':<10} {'Count':>8} {'%Files':>8} {'Size':>14} {'%Size':>8}")
    for ext in sorted(ext_counts, key=lambda x: (-ext_counts[x], x)):
        count = ext_counts[ext]
        size = ext_sizes[ext]
        pct_files = count / total_files * 100 if total_files > 0 else 0
        pct_size = size / total_size * 100 if total_size > 0 else 0
        print(f"{ext or '(none)':<10} {count:8d} {pct_files:8.1f} {human_readable_size(size):>14} {pct_size:8.1f}")
    print()

def _to_relative_path(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except Exception:
        return str(path)

def _from_relative_path(rel_path: str, base: Path) -> Path:
    return base / rel_path
