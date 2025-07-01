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
