"""
powergrid.anomalies
-------------------
Anomaly detection phase for PowerGrid+ pipeline.

Rules implemented:
- Voltage anomaly: voltage < 180 or > 250
- Current anomaly: negative or > 50A (based on typical household/industrial ranges)
- Sudden spike: kw_pct_change > +150%
- Sudden drop: kw_pct_change < -80%
- Zero-power anomaly: power_kw == 0 but voltage/current present

Output:
Adds two columns:
- anomaly_flag (bool)
- anomaly_reason (str)
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from datetime import datetime


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1].parent


def _latest_processed(processed_dir: Path) -> Path:
    files = list(processed_dir.glob("processed_energy_*"))
    if not files:
        raise FileNotFoundError("No processed files found. Run `powergrid etl` first.")
    return max(files, key=lambda p: p.stat().st_mtime)


def _apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    reasons = []

    for idx, row in df.iterrows():
        r = []

        # Voltage anomaly
        if row["voltage"] < 180 or row["voltage"] > 250:
            r.append("voltage_out_of_range")

        # Current anomaly
        if row["current"] < 0 or row["current"] > 50:
            r.append("current_out_of_range")

        # Sudden spike in consumption
        if row["kw_pct_change"] > 1.5:
            r.append("sudden_spike")

        # Sudden drop
        if row["kw_pct_change"] < -0.8:
            r.append("sudden_drop")

        # Zero power but voltage/current present
        if row["power_kw"] == 0 and row["voltage"] > 150 and row["current"] > 0.1:
            r.append("zero_power_but_active_line")

        if len(r) == 0:
            reasons.append(None)
        else:
            reasons.append(",".join(r))

    df["anomaly_reason"] = reasons
    df["anomaly_flag"] = df["anomaly_reason"].notna()

    return df


def run_anomaly_detection() -> Path:
    root = _repo_root()
    processed_dir = root / "data" / "processed"

    processed_file = _latest_processed(processed_dir)
    df = pd.read_parquet(processed_file)

    df = _apply_rules(df)

    ts_suffix = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_path = processed_dir / f"anomaly_tagged_{ts_suffix}.parquet"
    df.to_parquet(out_path, index=False)

    return out_path


def anomalies_cli():
    """CLI wrapper."""
    out = run_anomaly_detection()
    print(f"[powergrid] anomaly-tagged file saved -> {out}")


if __name__ == "__main__":
    p = run_anomaly_detection()
    print("Anomaly file:", p)
