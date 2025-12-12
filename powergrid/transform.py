"""
powergrid.transform
-------------------
Transformation + cleaning phase of the PowerGrid ETL pipeline.

Steps:
1. Load latest raw dataset (CSV/Parquet)
2. Parse timestamps + types
3. Clean impossible values (voltage/current/power)
4. Engineer features (hour_of_day, rolling means, pct changes)
5. Save processed dataset to /data/processed
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from datetime import datetime


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1].parent


def _latest_raw_file(raw_dir: Path) -> Path:
    files = list(raw_dir.glob("raw_energy_*"))
    if not files:
        raise FileNotFoundError("No raw input files found in data/raw/. Run `powergrid generate` first.")
    return max(files, key=lambda p: p.stat().st_mtime)


def _clean_types(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["meter_id"] = df["meter_id"].astype(int)
    df["region"] = df["region"].astype("category")

    float_cols = ["voltage", "current", "power_kw", "power_factor", "temperature_c"]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _clean_values(df: pd.DataFrame) -> pd.DataFrame:
    # impossible voltage values
    df.loc[df["voltage"] < 50, "voltage"] = None
    df.loc[df["current"] < 0, "current"] = None
    df.loc[df["power_kw"] < 0, "power_kw"] = None
    df.loc[df["power_factor"] > 1.0, "power_factor"] = 1.0
    df.loc[df["power_factor"] < 0.0, "power_factor"] = None

    # drop rows where everything is broken
    df = df.dropna(subset=["voltage", "current", "power_kw"])

    return df


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df["hour_of_day"] = df["timestamp"].dt.hour + df["timestamp"].dt.minute / 60.0

    # Group by meter for rolling calculations
    df = df.sort_values(["meter_id", "timestamp"])

    df["rolling_kw_1h"] = (
        df.groupby("meter_id")["power_kw"]
        .rolling(window=4, min_periods=1)   # 4 × 15min = 1 hour
        .mean()
        .reset_index(level=0, drop=True)
    )

    # percent change in consumption
    df["kw_pct_change"] = (
        df.groupby("meter_id")["power_kw"]
        .pct_change()
        .fillna(0)
    )

    return df


def transform_data() -> Path:
    """
    Main transform function.
    Loads latest raw file, cleans, engineers features, and writes output parquet.
    """
    root = _repo_root()
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    raw_file = _latest_raw_file(raw_dir)

    # Load
    if raw_file.suffix == ".parquet":
        df = pd.read_parquet(raw_file)
    else:
        df = pd.read_csv(raw_file)

    # Transform pipeline
    df = _clean_types(df)
    df = _clean_values(df)
    df = _engineer_features(df)

    # Save processed file
    ts_suffix = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_path = processed_dir / f"processed_energy_{ts_suffix}.parquet"
    df.to_parquet(out_path, index=False)

    return out_path


def transform_cli() -> None:
    """CLI wrapper for `powergrid etl`."""
    out = transform_data()
    print(f"[powergrid] processed data saved -> {out}")


if __name__ == "__main__":
    p = transform_data()
    print("Processed:", p)
