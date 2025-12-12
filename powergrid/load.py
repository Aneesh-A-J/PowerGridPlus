"""
powergrid.load
--------------
Database load step for PowerGrid+ pipeline.
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1].parent


def _latest_processed_file(processed_dir: Path) -> Path:
    files = list(processed_dir.glob("processed_energy_*")) + \
            list(processed_dir.glob("anomaly_tagged_*"))
    if not files:
        raise FileNotFoundError("No processed/anomaly-tagged parquet files found.")
    return max(files, key=lambda p: p.stat().st_mtime)


def _postgres_engine():
    USER = "postgres"
    PASSWORD = "postgres"
    HOST = "localhost"
    PORT = 5432
    DB = "powergrid"

    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
    return create_engine(url, future=True)


def load_to_postgres() -> None:
    root = _repo_root()
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    processed_file = _latest_processed_file(processed_dir)
    df = pd.read_parquet(processed_file)

    engine = _postgres_engine()

    create_stmt = text("""
        CREATE TABLE IF NOT EXISTS readings (
            timestamp TIMESTAMP,
            meter_id INT,
            region VARCHAR(50),
            voltage DOUBLE PRECISION,
            current DOUBLE PRECISION,
            power_kw DOUBLE PRECISION,
            power_factor DOUBLE PRECISION,
            temperature_c DOUBLE PRECISION,
            hour_of_day DOUBLE PRECISION,
            rolling_kw_1h DOUBLE PRECISION,
            kw_pct_change DOUBLE PRECISION,
            anomaly_flag BOOLEAN,
            anomaly_reason TEXT
        );
    """)

    with engine.begin() as conn:
        conn.execute(create_stmt)

    df.to_sql(
        name="readings",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

    print(f"[powergrid] loaded {len(df)} rows into PostgreSQL table 'readings'")


def load_cli():
    load_to_postgres()
