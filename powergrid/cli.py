import argparse
from .data_gen import generate_data_cli
from .transform import transform_cli
from .anomalies import anomalies_cli
from .load import load_cli
from .pipeline import pipeline_cli


def main():
    parser = argparse.ArgumentParser(
        prog="powergrid",
        description="PowerGrid+ ETL Pipeline CLI"
    )

    subparsers = parser.add_subparsers(dest="command")

    # All supported commands
    subparsers.add_parser("generate")
    subparsers.add_parser("etl")
    subparsers.add_parser("anomalies")
    subparsers.add_parser("load")
    subparsers.add_parser("pipeline")

    args = parser.parse_args()

    if args.command == "generate":
        generate_data_cli()
    elif args.command == "etl":
        transform_cli()
    elif args.command == "anomalies":
        anomalies_cli()
    elif args.command == "load":
        load_cli()
    elif args.command == "pipeline":
        pipeline_cli()
    else:
        parser.print_help()
