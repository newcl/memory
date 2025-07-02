import sqlite3
import os
from pathlib import Path

def analyze_database():
    db_path = Path('mymemory/.memory/metadata.db')
    if not db_path.exists():
        print("Database not found!")
        return
    
    print(f"Database file size: {db_path.stat().st_size / (1024*1024):.2f} MB")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total rows
    cursor.execute('SELECT COUNT(*) FROM files')
    total_rows = cursor.fetchone()[0]
    print(f"Total rows: {total_rows}")
    
    # Analyze column sizes
    print("\nColumn analysis:")
    columns = [
        'file_hash', 'original_filename', 'current_filename', 
        'original_path', 'current_path', 'size', 'media_type', 
        'date_added', 'extracted_metadata', 'uploaded_s3', 
        'uploaded_gcloud', 'uploaded_azure', 'metadata_extracted', 
        'perceptual_hash'
    ]
    
    for col in columns:
        cursor.execute(f'SELECT AVG(LENGTH({col})) FROM files WHERE {col} IS NOT NULL')
        avg_len = cursor.fetchone()[0] or 0
        cursor.execute(f'SELECT COUNT(*) FROM files WHERE {col} IS NOT NULL')
        non_null_count = cursor.fetchone()[0]
        total_size = avg_len * non_null_count
        print(f"  {col}: avg {avg_len:.1f} chars, {non_null_count} non-null, est. {total_size/1024:.1f} KB")
    
    # Analyze metadata specifically
    print("\nMetadata analysis:")
    cursor.execute('SELECT LENGTH(extracted_metadata) as meta_len, COUNT(*) as count FROM files WHERE extracted_metadata IS NOT NULL AND extracted_metadata != "{}" GROUP BY meta_len ORDER BY meta_len DESC LIMIT 10')
    meta_dist = cursor.fetchall()
    print("  Non-empty metadata size distribution:")
    for meta_len, count in meta_dist:
        print(f"    {meta_len} chars: {count} files")
    
    cursor.execute('SELECT COUNT(*) FROM files WHERE extracted_metadata = "{}" OR extracted_metadata IS NULL')
    empty_meta = cursor.fetchone()[0]
    print(f"  Empty metadata: {empty_meta} files")
    
    # Analyze path lengths
    print("\nPath analysis:")
    cursor.execute('SELECT AVG(LENGTH(original_path)), AVG(LENGTH(current_path)) FROM files')
    orig_avg, curr_avg = cursor.fetchone()
    print(f"  Average original_path length: {orig_avg:.1f} chars")
    print(f"  Average current_path length: {curr_avg:.1f} chars")
    
    # Check for potential optimizations
    print("\nOptimization opportunities:")
    
    # Check if original_filename is mostly empty
    cursor.execute('SELECT COUNT(*) FROM files WHERE original_filename IS NULL OR original_filename = ""')
    empty_orig_filename = cursor.fetchone()[0]
    if empty_orig_filename > 0:
        print(f"  - {empty_orig_filename} files have empty original_filename (can be dropped)")
    
    # Check metadata compression potential
    cursor.execute('SELECT SUM(LENGTH(extracted_metadata)) FROM files WHERE extracted_metadata IS NOT NULL AND extracted_metadata != "{}"')
    total_meta_size = cursor.fetchone()[0] or 0
    if total_meta_size > 0:
        print(f"  - Metadata takes ~{total_meta_size/1024:.1f} KB (could be compressed)")
    
    # Check perceptual hash usage
    cursor.execute('SELECT COUNT(*) FROM files WHERE perceptual_hash IS NOT NULL')
    phash_count = cursor.fetchone()[0]
    print(f"  - {phash_count} files have perceptual hashes")
    
    conn.close()

if __name__ == "__main__":
    analyze_database() 