# Copyright (C) 2025 Aakash Shankar

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    from .ingest import ingest
except ImportError:
    print("Error: Ensure 'ingest.py' exists and contains the 'ingest' function.", file=sys.stderr)
    sys.exit(1)

try:
    from .config import (
        TARGET_VARIABLES, S3_BUCKET, CYCLE, FILE_TYPE,
        FORECAST_HOURS_START, FORECAST_HOURS_END,
        DUCKDB_FILE, TABLE_NAME
    )
except ImportError:
    print("Error: Ensure 'config.py' exists and contains necessary constants "
          "(TARGET_VARIABLES, S3_BUCKET, CYCLE, etc.).", file=sys.stderr)
    sys.exit(1)


def parse_target_points_file(filepath):
    """Reads a CSV file containing latitude,longitude pairs."""
    points = []
    if not os.path.exists(filepath):
        print(f"Error: Target points file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(filepath, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, None) # Skip header row if present
            print(f"Reading target points from: {filepath} (Skipped header: {header})")
            for i, row in enumerate(reader):
                if len(row) >= 2:
                    try:
                        lat = float(row[0].strip())
                        lon = float(row[1].strip())
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                             points.append((lat, lon))
                        else:
                             print(f"Warning: Skipping row {i+2} in {filepath}: Invalid lat/lon values ({lat},{lon})", file=sys.stderr)
                    except ValueError:
                        print(f"Warning: Skipping row {i+2} in {filepath}: Non-numeric lat/lon values ('{row[0]}','{row[1]}')", file=sys.stderr)
                else:
                    print(f"Warning: Skipping row {i+2} in {filepath}: Row does not contain at least 2 columns.", file=sys.stderr)
    except IOError as e:
        print(f"Error reading target points file {filepath}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while parsing {filepath}: {e}", file=sys.stderr)
        sys.exit(1)

    if not points:
        print(f"Error: No valid target points found in file: {filepath}", file=sys.stderr)
        sys.exit(1)

    print(f"Successfully parsed {len(points)} target points.")
    return points


def main_cli():
    """Parses CLI arguments and triggers the ingestion process."""

    parser = argparse.ArgumentParser(
        description="HRRR GRIB2 Data Ingestion Tool. Fetches HRRR data for specified points, "
                    "variables, and time range, storing results in DuckDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show defaults in help
    )

    parser.add_argument(
        "target_points_file",
        type=str,
        help="Path to a CSV file containing target points (latitude,longitude one per line, optional header)."
    )

    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="The latest forecast run date (YYYYMMDD) to include. If omitted, defaults to YESTERDAY'S date (UTC)."
    )

    parser.add_argument(
        "--variables",
        type=str,
        default=None,
        help="Comma-separated list of variable user_names to ingest (e.g., 'temperature_2m,u_component_wind_10m'). "
             "If omitted, all supported variables are ingested."
    )

    parser.add_argument(
        "--num-hours",
        type=int,
        default=48,
        help="Number of hours of forecast runs (06Z cycle) to look back from the run-date, inclusive."
    )

    parser.add_argument(
        "--db-file",
        type=str,
        default=DUCKDB_FILE, # Default from config
        help="Path to the DuckDB database file."
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default=TABLE_NAME, # Default from config
        help="Name of the table within the DuckDB file."
    )

    args = parser.parse_args()

    # 1. Determine Run Date
    run_date_to_use = args.run_date
    if run_date_to_use is None:
        # Default to yesterday UTC
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        run_date_to_use = yesterday.strftime("%Y%m%d")
        print(f"No --run-date provided, defaulting to yesterday (UTC): {run_date_to_use}")
    else:
        # Validate format if provided
        try:
            datetime.strptime(run_date_to_use, "%Y%m%d")
            print(f"Using specified --run-date: {run_date_to_use}")
        except ValueError:
            print(f"Error: Invalid --run-date format '{run_date_to_use}'. Please use YYYYMMDD.", file=sys.stderr)
            sys.exit(1)

    # 2. Determine Variables
    variables_to_use = []
    all_variable_names = {v['user_name'] for v in TARGET_VARIABLES} # Set for quick lookup

    if args.variables is None:
        variables_to_use = TARGET_VARIABLES # Use all default variables
        print(f"No --variables specified, ingesting all {len(variables_to_use)} supported variables.")
    else:
        requested_names = {v.strip() for v in args.variables.split(',') if v.strip()}
        print(f"Requested variables: {', '.join(sorted(list(requested_names)))}")
        variables_to_use = [v for v in TARGET_VARIABLES if v['user_name'] in requested_names]

        # Validation
        found_names = {v['user_name'] for v in variables_to_use}
        missing_names = requested_names - found_names
        if missing_names:
            print(f"Warning: The following requested variables are not supported or misspelled and will be ignored: {', '.join(sorted(list(missing_names)))}", file=sys.stderr)
            print(f"Supported variables are: {', '.join(sorted(list(all_variable_names)))}", file=sys.stderr)

        if not variables_to_use:
            print(f"Error: No valid variables selected after filtering. Please check the --variables argument.", file=sys.stderr)
            sys.exit(1)
        print(f"Ingesting {len(variables_to_use)} specified variable(s).")

    # 3. Determine Target Points
    target_points_list = parse_target_points_file(args.target_points_file)

    # 4. Number of Hours (already validated by argparse)
    num_hours_to_use = args.num_hours
    if num_hours_to_use <= 0:
         print(f"Error: --num-hours must be positive.", file=sys.stderr)
         sys.exit(1)
    print(f"Looking back {num_hours_to_use} hours for {CYCLE}Z runs.")


    # --- Call the Ingestion Logic ---
    print("\nStarting data ingestion...")
    try:
        ingest(
            run_date=run_date_to_use,
            num_hours=num_hours_to_use,
            cycle=CYCLE,
            file_type=FILE_TYPE,
            f_start=FORECAST_HOURS_START,
            f_end=FORECAST_HOURS_END,
            target_vars=variables_to_use,
            target_points=target_points_list,
            db_filename=args.db_file,
            table_name=args.table_name,
            s3_bucket=S3_BUCKET
        )
        print("\nIngestion process completed.")
    except Exception as e:
        print(f"\nAn error occurred during the ingestion process: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main_cli()