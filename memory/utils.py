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

def is_valid_media_file(filepath: Path) -> bool:
    """
    Quickly validates if a media file is valid by checking file headers.
    Returns True if the file appears to be a valid media file, False otherwise.
    """
    try:
        with open(filepath, 'rb') as f:
            header = f.read(16)  # Read first 16 bytes
            
        ext = filepath.suffix.lower()
        
        # JPEG
        if ext in ['.jpg', '.jpeg']:
            return header.startswith(b'\xff\xd8\xff')
        
        # PNG
        elif ext == '.png':
            return header.startswith(b'\x89PNG\r\n\x1a\n')
        
        # GIF
        elif ext == '.gif':
            return header.startswith(b'GIF87a') or header.startswith(b'GIF89a')
        
        # BMP
        elif ext == '.bmp':
            return header.startswith(b'BM')
        
        # TIFF
        elif ext == '.tiff':
            return header.startswith(b'II*\x00') or header.startswith(b'MM\x00*')
        
        # WebP
        elif ext == '.webp':
            return header.startswith(b'RIFF') and header[8:12] == b'WEBP'
        
        # MP4
        elif ext == '.mp4':
            # MP4 files start with ftyp box
            return len(header) >= 8 and header[4:8] == b'ftyp'
        
        # MOV (QuickTime)
        elif ext == '.mov':
            # MOV files can start with ftyp or moov
            return len(header) >= 8 and (header[4:8] == b'ftyp' or header[4:8] == b'moov')
        
        # AVI
        elif ext == '.avi':
            return header.startswith(b'RIFF') and header[8:12] == b'AVI '
        
        # MKV
        elif ext == '.mkv':
            # MKV files start with EBML header
            return header.startswith(b'\x1a\x45\xdf\xa3')
        
        # WebM
        elif ext == '.webm':
            # WebM is a subset of MKV
            return header.startswith(b'\x1a\x45\xdf\xa3')
        
        # FLV
        elif ext == '.flv':
            return header.startswith(b'FLV')
        
        # Unknown format
        else:
            return True  # Assume valid if we don't know the format
            
    except Exception:
        return False  # File can't be read or other error
