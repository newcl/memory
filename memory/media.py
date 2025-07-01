# memory/media.py
import json
from pathlib import Path
from PIL import Image, UnidentifiedImageError # type: ignore
from typing import Dict, Any

# Define a helper to convert non-JSON serializable types
def _json_serializable_value(value):
    """Converts a value to a JSON-serializable type."""
    try:
        from PIL.TiffImagePlugin import IFDRational
    except ImportError:
        IFDRational = None

    if IFDRational is not None and isinstance(value, IFDRational):
        try:
            # Convert IFDRational to a float
            return float(value)
        except ZeroDivisionError:
            # Handle cases where denominator might be zero, though rare for valid EXIF
            return 0.0
    elif isinstance(value, bytes):
        try:
            # Attempt to decode bytes to string (e.g., for copyright strings)
            # 'replace' error handler will substitute un-decodable bytes with a placeholder
            return value.decode('utf-8', errors='replace')
        except UnicodeDecodeError:
            # If it's not decodeable as text, represent as a hex string
            return value.hex()
    # If it's a tuple or list, iterate through it to process nested non-serializable items
    elif isinstance(value, (list, tuple)):
        return [_json_serializable_value(item) for item in value]
    # For any other complex object that json.dumps might struggle with, convert to string as a fallback
    # This is a generic fallback, more specific handling is better if known types emerge
    elif not isinstance(value, (str, int, float, bool, type(None), dict, list)):
        return str(value) # Convert unknown types to string
    return value

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
                        # Process value to ensure it's JSON serializable
                        metadata[decoded] = _json_serializable_value(value)
    except UnidentifiedImageError:
        # File is not a recognized image format or is corrupted
        pass
    except Exception as e:
        print(f"Warning: Could not extract image metadata from {filepath}: {e}")
    return metadata

def extract_video_metadata(filepath: Path) -> Dict[str, Any]:
    """
    Placeholder for video metadata extraction using pure Python.
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
    
    # The default=str fallback is a safety net for any types _json_serializable_value misses.
    # ensure_ascii=False allows non-ASCII characters directly in the JSON string for better readability.
    return json.dumps(metadata, ensure_ascii=False, default=str)