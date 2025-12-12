import os
import glob
import pandas as pd

def export_gold():
    # find latest processed + latest anomaly file
    processed = sorted(glob.glob("data/processed/processed_energy_*.parquet"))
    anomalies = sorted(glob.glob("data/processed/anomaly_tagged_*.parquet"))

    if not processed or not anomalies:
        print("[error] processed files not found")
        return

    p_path = processed[-1]
    a_path = anomalies[-1]

    print("[powergrid] using:")
    print(" processed:", p_path)
    print(" anomalies:", a_path)

    df_proc = pd.read_parquet(p_path)
    df_anom = pd.read_parquet(a_path)

    # merge
    df = df_proc.merge(
        df_anom[["timestamp", "meter_id", "anomaly_flag", "anomaly_reason"]],
        on=["timestamp", "meter_id"],
        how="left"
    )

    os.makedirs("data/final", exist_ok=True)
    out_path = "data/final/gold_dataset.csv"

    df.to_csv(out_path, index=False)
    print("[powergrid] gold dataset file ready ->", out_path)


def main():
    export_gold()


if __name__ == "__main__":
    main()
