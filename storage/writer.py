# Write to Parquet
# storage/writer.py

import polars as pl
from pathlib import Path

def write_parquet(df: pl.DataFrame, output_path: str) -> str:
    """
    Write Polars DataFrame to Parquet file.
    
    Returns the path of the saved file.
    """
    if df.is_empty():
        raise ValueError("Cannot write empty DataFrame to Parquet")

    # Ensure parent directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Write Parquet
    df.write_parquet(output_path)
    return output_path