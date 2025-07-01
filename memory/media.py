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
