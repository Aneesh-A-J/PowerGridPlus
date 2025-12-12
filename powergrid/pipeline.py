"""
powergrid.pipeline
------------------
Full end-to-end ETL orchestration for PowerGrid+.

Pipeline order:
1. Generate synthetic raw data
2. Transform & clean data
3. Detect anomalies
4. Load into PostgreSQL

CLI command:
    powergrid pipeline
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime

from .data_gen import generate_data
from .transform import transform_data
from .anomalies import run_anomaly_detection
from .load import load_to_postgres


def run_pipeline() -> None:
    print("\n=== POWERGRID+ PIPELINE START ===")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[pipeline] starting at {ts}")

    try:
        print("\n[1/4] Generating raw data...")
        raw_path = generate_data()
        print(f"[OK] Raw data -> {raw_path}")

        print("\n[2/4] Transforming data...")
        processed_path = transform_data()
        print(f"[OK] Processed data -> {processed_path}")

        print("\n[3/4] Detecting anomalies...")
        anomaly_path = run_anomaly_detection()
        print(f"[OK] Anomaly-tagged data -> {anomaly_path}")

        print("\n[4/4] Loading into PostgreSQL...")
        load_to_postgres()
        print("[OK] Loaded into database.")

    except Exception as e:
        print("\n[ERROR] Pipeline failed:")
        print(e)
        raise

    print("\n=== POWERGRID+ PIPELINE COMPLETE ===\n")


# CLI wrapper
def pipeline_cli():
    run_pipeline()


if __name__ == "__main__":
    run_pipeline()
