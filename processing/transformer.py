# Polars-based ETL (cleaning, feature extraction)
# processing/transformer.py

import polars as pl
from datetime import datetime

def transform_messages(messages: list[dict]) -> pl.DataFrame:
    """
    Convert list of sensor JSON messages to Polars DataFrame and add features.
    """
    if not messages:
        return pl.DataFrame()  # return empty frame

    # Ensure all records have a timestamp field (or add it)
    for msg in messages:
        msg.setdefault("timestamp", datetime.now().isoformat())

    df = pl.DataFrame(messages)

    # Convert timestamp to datetime
    if "timestamp" in df.columns:
        df = df.with_columns([
            pl.col("timestamp").str.to_datetime().alias("timestamp")
        ])

    # Add sample features (you can customize!)
    if "temperature" in df.columns and "vibration" in df.columns:
        df = df.with_columns([
            (pl.col("temperature") * 1.8 + 32).alias("temp_fahrenheit"),
            (pl.col("vibration") > 0.5).alias("vibration_alert")
        ])

    return df