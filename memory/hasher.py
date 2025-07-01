from pathlib import Path
from memory.utils import get_file_hash

def calculate_file_hash(filepath: Path) -> str:
    """
    Calculates the SHA256 hash of a file.
    This implementation is pure Python.
    """
    return get_file_hash(filepath, algorithm='sha256')
