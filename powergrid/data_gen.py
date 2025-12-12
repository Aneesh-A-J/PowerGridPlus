"""
powergrid.data_gen
------------------
Synthetic data generator for PowerGrid+.

Design goals:
- Package-friendly: no hard-coded absolute paths.
- Saves reproducible CSV and Parquet files under /data/raw.
- Fast on CPU and deterministic with a seed.
- Exposes a simple function `generate_data()` used by the CLI.
"""

from __future__ import annotations
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional
import numpy as np
import pandas as pd


DEFAULT_NUM_METERS = 500
DEFAULT_DAYS = 7
DEFAULT_INTERVAL_MIN = 15  # reading every 15 minutes
DEFAULT_REGIONS = ["North", "South", "East", "West"]
RNG_SEED = 42


def _repo_root() -> Path:
    # file is in powergrid/, repo root is two parents up
    return Path(__file__).resolve().parents[1].parent


def _ensure_dirs(root: Path) -> None:
    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    raw.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)


def _generate_timestamps(days: int, interval_min: int) -> List[datetime]:
    total_minutes = days * 24 * 60
    steps = total_minutes // interval_min
    start = datetime.now() - timedelta(days=days)
    return [start + timedelta(minutes=i * interval_min) for i in range(int(steps))]


def _generate_meter_profile(num_meters: int, regions: List[str], rng: random.Random):
    """
    Create per-meter static metadata like region and base_load.
    base_load is typical kW at baseline; variation occurs in readings.
    """
    meters = []
    for meter_id in range(1, num_meters + 1):
        region = rng.choice(regions)
        # base_load in kW, differ by region slightly
        base = float(round(rng.uniform(0.2, 5.0), 3))
        # assign a quality flag probability (for occasional zero readings)
        quality = rng.uniform(0.995, 1.0)  # closer to 1 => fewer corrupt readings
        meters.append({"meter_id": meter_id, "region": region, "base_load_kw": base, "quality": quality})
    return pd.DataFrame(meters)


def _simulate_readings(meters_df: pd.DataFrame, timestamps: List[datetime], rng: random.Random):
    """
    For each meter and timestamp, produce a reading row.
    Columns: timestamp, meter_id, region, voltage, current, power_kw, power_factor, temperature
    """
    rows = []
    for _, m in meters_df.iterrows():
        meter_id = int(m["meter_id"])
        region = m["region"]
        base = float(m["base_load_kw"])
        quality = float(m["quality"])

        # per-meter noise parameters
        volatility = rng.uniform(0.02, 0.25)  # relative volatility multiplier

        for ts in timestamps:
            # simulate diurnal pattern: sine wave across hours
            hour = ts.hour + ts.minute / 60.0
            diurnal = 0.5 * (1 + np.sin((hour - 6) / 24 * 2 * np.pi))  # peaks ~18:00
            # power in kW
            power_kw = max(0.0, base * (1.0 + diurnal * volatility + rng.normalvariate(0, volatility / 3)))
            # introduce occasional dropouts / corrupt readings
            if rng.random() > quality:
                # corrupt reading: zero or NaN (we will represent as 0 here)
                power_kw = 0.0

            # derive simple voltage/current/power_factor values around realistic ranges
            voltage = round(rng.uniform(210.0, 250.0) - (0.5 if region == "North" else 0.0), 2)
            current = round((power_kw * 1000.0) / max(voltage, 1.0), 3)  # I = P/V (approx)
            power_factor = round(rng.uniform(0.85, 0.99), 3)
            temperature = round(rng.uniform(15, 45), 1)

            rows.append({
                "timestamp": ts.isoformat(),
                "meter_id": meter_id,
                "region": region,
                "voltage": voltage,
                "current": current,
                "power_kw": round(power_kw, 3),
                "power_factor": power_factor,
                "temperature_c": temperature
            })

    return pd.DataFrame(rows)


def generate_data(
    num_meters: int = DEFAULT_NUM_METERS,
    days: int = DEFAULT_DAYS,
    interval_min: int = DEFAULT_INTERVAL_MIN,
    regions: Optional[List[str]] = None,
    seed: int = RNG_SEED,
    output_root: Optional[Path] = None,
    save_parquet: bool = True
) -> Path:
    """
    Generate synthetic power-meter readings and write outputs to disk.

    Returns the path to the raw CSV file written.
    """
    if regions is None:
        regions = DEFAULT_REGIONS

    rng = random.Random(seed)
    np.random.seed(seed)

    root = Path(output_root) if output_root else _repo_root()
    _ensure_dirs(root)
    raw_dir = root / "data" / "raw"

    # timestamps
    timestamps = _generate_timestamps(days=days, interval_min=interval_min)

    # per-meter metadata
    meters_df = _generate_meter_profile(num_meters=num_meters, regions=regions, rng=rng)

    # readings
    readings_df = _simulate_readings(meters_df, timestamps, rng)

    # Save files
    ts_suffix = datetime.now().strftime("%Y%m%dT%H%M%S")
    csv_path = raw_dir / f"raw_energy_{ts_suffix}.csv"
    parquet_path = raw_dir / f"raw_energy_{ts_suffix}.parquet"

    readings_df.to_csv(csv_path, index=False)
    if save_parquet:
        # parquet is smaller and more professional for pipelines
        readings_df.to_parquet(parquet_path, index=False)

    return csv_path


# CLI-friendly wrapper used by powergrid.cli
def generate_data_cli() -> None:
    """
    Small wrapper used by the CLI entrypoint. Keeps CLI code tiny.
    """
    path = generate_data()
    print(f"[powergrid] generated data -> {path}")


if __name__ == "__main__":
    # allowed for direct dev testing: python -m powergrid.data_gen
    p = generate_data()
    print(f"Generated: {p}")
