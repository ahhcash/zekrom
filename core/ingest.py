import math
import os
import sys
import tempfile
from datetime import timedelta, datetime, timezone

import boto3
import duckdb
import eccodes
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from scipy.spatial import KDTree

from .utils import latlon_to_xyz


def _get_grid_details(temp_filepath, target_points_xyz, grid_cache):
    """
    Reads grid definition from a GRIB file, checks cache, calculates
    nearest indices if needed, and updates the cache.

    Args:
        temp_filepath: Path to the local temporary GRIB file.
        target_points_xyz: NumPy array of target points in XYZ coordinates.
        grid_cache: Dictionary holding cached grid information (mutated).

    Returns:
        A tuple: (success_flag, nearest_indices, lats_flat, lons_flat)
        success_flag (bool): True if grid details were successfully obtained/calculated.
        nearest_indices (np.array or None): Indices of nearest grid points.
        lats_flat (np.array or None): Flattened latitudes of the grid.
        lons_flat (np.array or None): Flattened longitudes (adjusted) of the grid.
    """
    gid_peek = None
    grid_calc_success = False
    nearest_indices = None
    cached_lats = None
    cached_lons = None

    try:
        print(f"  Peeking into temporary file for grid info: {temp_filepath}")
        with open(temp_filepath, 'rb') as peek_f:
            gid_peek = eccodes.codes_grib_new_from_file(peek_f)
            if not gid_peek:
                print(f"  Warning: Could not read first GRIB message to determine grid.", file=sys.stderr)
                return False, None, None, None # Indicate failure

            grid_ni = eccodes.codes_get_long(gid_peek, "Ni")
            grid_nj = eccodes.codes_get_long(gid_peek, "Nj")
            grid_lat1 = eccodes.codes_get_double(gid_peek, "latitudeOfFirstGridPointInDegrees")
            grid_lon1 = eccodes.codes_get_double(gid_peek, "longitudeOfFirstGridPointInDegrees")
            grid_dt = eccodes.codes_get_long(gid_peek, "gridDefinitionTemplateNumber")
            current_file_grid_id = f"{grid_dt}-{grid_ni}-{grid_nj}-{grid_lat1:.4f}-{grid_lon1:.4f}"
            print(f"  File Grid ID: {current_file_grid_id}")

            if current_file_grid_id not in grid_cache:
                print(f"  Grid not in cache. Calculating nearest points...")
                grid_lats_arr = eccodes.codes_get_array(gid_peek, 'latitudes')
                grid_lons_arr = eccodes.codes_get_array(gid_peek, 'longitudes')

                if 0 < grid_lats_arr.size == grid_lons_arr.size:
                    grid_lats_flat = grid_lats_arr.flatten()
                    grid_lons_flat = grid_lons_arr.flatten()
                    grid_lons_flat[grid_lons_flat > 180] -= 360 # Adjust longitude
                    grid_xyz = np.array(latlon_to_xyz(grid_lats_flat, grid_lons_flat)).T
                    kdtree = KDTree(grid_xyz)
                    distances, indices = kdtree.query(target_points_xyz, k=1)
                    grid_cache[current_file_grid_id] = {
                        'lats': grid_lats_flat,
                        'lons': grid_lons_flat,
                        'indices': indices
                    }
                    print(f"  Calculated and cached nearest points.")
                    grid_calc_success = True
                else:
                    print(f"  Warning: Invalid grid coordinates found in GRIB message.", file=sys.stderr)
                    grid_calc_success = False
            else:
                print(f"  Grid found in cache.")
                grid_calc_success = True

            if grid_calc_success and current_file_grid_id in grid_cache:
                cached_grid = grid_cache[current_file_grid_id]
                nearest_indices = cached_grid['indices']
                cached_lats = cached_grid['lats']
                cached_lons = cached_grid['lons']
                return True, nearest_indices, cached_lats, cached_lons
            else:
                print(f"  Warning: Could not get/calculate nearest indices for grid {current_file_grid_id}.", file=sys.stderr)
                return False, None, None, None # Indicate failure

    except Exception as peek_err:
        print(f"  Error during grid calculation/peek: {peek_err}", file=sys.stderr)
        return False, None, None, None # Indicate failure
    finally:
        if gid_peek:
            try:
                eccodes.codes_release(gid_peek)
            except Exception as exc:
               print(f"  Warning: Encountered exception releasing peek gid: {exc}", file=sys.stderr)

def _extract_data_from_messages(temp_filepath, nearest_indices, grid_lats, grid_lons, target_variables, s3_source_path):
    """
    Iterates through GRIB messages in a file, matches target variables,
    extracts data at specified points, and returns rows to insert.

    Args:
        temp_filepath: Path to the local temporary GRIB file.
        nearest_indices: NumPy array of nearest grid point indices.
        grid_lats: Flattened NumPy array of grid latitudes.
        grid_lons: Flattened NumPy array of grid longitudes.
        target_variables: List of target variable dictionaries.
        s3_source_path: The S3 path string for this file.

    Returns:
        A tuple: (rows_to_insert, messages_scanned, variables_found)
        rows_to_insert (list): List of tuples, each representing a row for DB insertion.
        messages_scanned (int): Number of GRIB messages scanned in the file.
        variables_found (set): Set of 'user_name' strings for matched variables.
    """
    rows_to_insert = []
    messages_scanned = 0
    variables_found = set()

    try:
        with open(temp_filepath, 'rb') as local_f:
            while True:
                gid = None
                try:
                    gid = eccodes.codes_grib_new_from_file(local_f)
                    if not gid: break # End of file

                    messages_scanned += 1
                    for target in target_variables:
                        target_name = target["user_name"]
                        matched = True
                        for key, target_val in target.items():
                            if "user_name" == key: continue
                            if not eccodes.codes_is_defined(gid, key):
                                matched = False; break
                            try:
                                if isinstance(target_val, str): msg_value = eccodes.codes_get_string(gid, key)
                                elif isinstance(target_val, int): msg_value = eccodes.codes_get_long(gid, key)
                                elif isinstance(target_val, float): msg_value = eccodes.codes_get_double(gid, key)
                                else: msg_value = eccodes.codes_get(gid, key)
                                if msg_value != target_val: matched = False; break
                            except (eccodes.KeyValueNotFoundError, eccodes.WrongTypeError, eccodes.EncodingError):
                                matched = False; break

                        if matched:
                            variables_found.add(target_name)
                            try:
                                values_flat = eccodes.codes_get_values(gid).flatten()
                                if values_flat.size != grid_lats.size:
                                    print(f"  Warning: Size mismatch for {target_name} msg {messages_scanned}.", file=sys.stderr)
                                    continue

                                date_val = eccodes.codes_get_long(gid, 'date')
                                time_val = eccodes.codes_get_long(gid, 'time')
                                step_val = eccodes.codes_get_long(gid, 'step')
                                run_dt_utc = datetime.strptime(f"{date_val}{time_val:04d}", "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                                valid_dt_utc = run_dt_utc + timedelta(hours=step_val)

                                for i, flat_idx in enumerate(nearest_indices):
                                    point_lat = grid_lats[flat_idx]
                                    point_lon = grid_lons[flat_idx]
                                    point_val = values_flat[flat_idx]
                                    if np.isnan(point_val): # Or check against eccodes missing value if needed
                                        print(f"    Skipping NaN value for {target_name} at index {flat_idx}") # Optional debug print
                                        continue
                                    row = (valid_dt_utc, run_dt_utc, point_lat, point_lon, target_name, point_val, s3_source_path)
                                    rows_to_insert.append(row)

                            except Exception as extract_err:
                                print(f"  Error extracting data for {target_name} msg {messages_scanned}: {extract_err}", file=sys.stderr)
                            break
                finally:
                    if gid: eccodes.codes_release(gid)

    except FileNotFoundError:
        print(f"Error: Temp file {temp_filepath} disappeared during message processing loop?", file=sys.stderr)
        raise
    except Exception as proc_err:
        print(f"Error during message processing loop for {temp_filepath}: {proc_err}", file=sys.stderr)
        raise

    return rows_to_insert, messages_scanned, variables_found


def process_grib_file(s3_client, s3_bucket, s3_key, target_points_xyz, target_variables, grid_cache, db_con, table_name):
    """
    Downloads, processes a single GRIB2 file from S3, extracts target data,
    and inserts it into the database. Updates grid_cache in place.

    Args:
        s3_client: Initialized boto3 S3 client.
        s3_bucket: Name of the S3 bucket.
        s3_key: The specific S3 key (file path) to process.
        target_points_xyz: NumPy array of target points in XYZ coordinates.
        target_variables: List of target variable dictionaries (from config).
        grid_cache: Dictionary used for caching grid information (modified in place).
        db_con: Active DuckDB database connection.
        table_name: Name of the target table in DuckDB.

    Returns:
        A dictionary containing processing results for this file, e.g.:
        {
            "messages_scanned": int,
            "rows_inserted": int,
            "status": "processed" | "skipped_no_grid" | "not_found" | "download_error" | "processing_error",
            "variables_found_in_file": set # Set of user_names found
        }
    """
    print(f"Attempting to process S3 file: s3://{s3_bucket}/{s3_key}")
    temp_filepath = None
    messages_scanned_in_file = 0
    rows_inserted_for_file = 0
    variables_found_in_file = set()

    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_f:
            temp_filepath = temp_f.name
        print(f"  Downloading temporary file to {temp_filepath}")
        s3_client.download_file(s3_bucket, s3_key, temp_filepath)
        print(f"  Download complete.")

        grid_ok, nearest_indices, grid_lats, grid_lons = _get_grid_details(
            temp_filepath, target_points_xyz, grid_cache
        )

        if grid_ok:
            s3_source_path = f"s3://{s3_bucket}/{s3_key}"  # Define source path once
            try:
                rows_to_insert, messages_scanned_in_file, variables_found_in_file = _extract_data_from_messages(
                    temp_filepath=temp_filepath,
                    nearest_indices=nearest_indices,
                    grid_lats=grid_lats,
                    grid_lons=grid_lons,
                    target_variables=target_variables,
                    s3_source_path=s3_source_path
                )

                if rows_to_insert:
                    print(f"  Inserting {len(rows_to_insert)} rows into DuckDB for file {s3_key}")
                    try:
                        db_con.executemany(
                            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                            rows_to_insert)
                        rows_inserted_for_file = len(rows_to_insert)
                        print(
                            f"  Attempted insert of {rows_inserted_for_file} rows complete. Scanned {messages_scanned_in_file} messages.")
                        file_status = "processed"
                    except Exception as dberr:
                        print(f"  Error inserting batch of {len(rows_to_insert)} into DuckDB: {dberr}", file=sys.stderr)
                        file_status = "processing_error"
                else:
                    print(
                        f"  No matching data found or extracted for target points in {s3_key}. Scanned {messages_scanned_in_file} messages.")
                    file_status = "processed"

            except (FileNotFoundError, Exception) as proc_err:
                print(f"  Error during message extraction phase for {s3_key}: {proc_err}", file=sys.stderr)
                file_status = "processing_error"

        else:  # grid_ok is False
            print(f"  Skipping message processing for {s3_key} due to missing/failed grid indices.")
            file_status = "skipped_no_grid"

        return {
            "messages_scanned": messages_scanned_in_file,
            "rows_inserted": rows_inserted_for_file,
            "status": file_status,
            "variables_found_in_file": variables_found_in_file
        }

    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            print(f"  Warning: File not found on S3: s3://{s3_bucket}/{s3_key}")
            return {"messages_scanned": 0, "rows_inserted": 0, "status": "not_found", "variables_found_in_file": set()}
        else:
            print(f"  Error accessing S3 file {s3_key}: {ce}", file=sys.stderr)
            return {"messages_scanned": 0, "rows_inserted": 0, "status": "download_error", "variables_found_in_file": set()}
    except Exception as e:
        print(f"  Unhandled error during processing of {s3_key}: {e}", file=sys.stderr)
        return {"messages_scanned": messages_scanned_in_file, "rows_inserted": rows_inserted_for_file, "status": "processing_error", "variables_found_in_file": variables_found_in_file}
    finally:
        if temp_filepath and os.path.exists(temp_filepath):
            try:
                print(f"  Cleaning up temporary file: {temp_filepath}")  # Optional debug print
                os.remove(temp_filepath)
            except OSError as ose:
                print(f"  Warning: Could not delete temporary file {temp_filepath}: {ose}", file=sys.stderr)

def ingest(run_date, num_hours, cycle, file_type, f_start, f_end, target_vars, target_points, db_filename, table_name, s3_bucket):
    total_rows_attempted_insert = 0
    total_messages_scanned = 0
    files_successfully_downloaded = 0
    files_processed_ok = 0
    files_skipped_no_grid = 0
    files_not_found = 0
    files_with_download_errors = 0
    files_with_processing_errors = 0
    targets_found = set()
    s3_keys_to_process = []
    grid_cache = {}
    con = None

    try:
        print(f"Connecting to DuckDB database: {db_filename}")
        con = duckdb.connect(database=db_filename, read_only=False)
        print(f"Creating table {table_name} with idempotence")
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    valid_time_utc TIMESTAMP,
                    run_time_utc   TIMESTAMP,
                    latitude       FLOAT,
                    longitude      FLOAT,
                    variable       VARCHAR,
                    value          FLOAT,
                    source_s3      VARCHAR,
                    PRIMARY KEY (valid_time_utc, run_time_utc, latitude, longitude, variable)
                );
                """
        con.execute(create_table_sql)
        print(f"Table creation complete.")

        print("Initializing S3 client for unsigned access...")
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        print(f"Targeting S3 Bucket: {s3_bucket}")

        print(f"Generating file list for {num_hours} hours back from RUN_DATE={run_date}, CYCLE={cycle}Z, TYPE={file_type}, F{f_start:02d}-F{f_end:02d}...")

        try:
            run_date_dt = datetime.strptime(run_date, "%Y%m%d")

            num_runs_needed = 1 + math.floor(num_hours / 24)

            print(f"  Will process {num_runs_needed} forecast runs (06Z cycle).")

            dates_to_process_str = []
            for i in range(num_runs_needed):
                process_date_dt = run_date_dt - timedelta(days=i)
                date_str = process_date_dt.strftime("%Y%m%d")
                dates_to_process_str.append(date_str)

                for hour in range(f_start, f_end + 1):
                    key = f"hrrr.{date_str}/conus/hrrr.t{cycle}z.{file_type}f{hour:02d}.grib2"
                    s3_keys_to_process.append(key)

            print(f"  Generated {len(s3_keys_to_process)} S3 file keys for runs on dates: {', '.join(sorted(dates_to_process_str))}")

        except ValueError:
            print(f"Error: Invalid RUN_DATE format '{run_date}'. Please use YYYYMMDD.", file=sys.stderr)
            sys.exit(1)

        if not s3_keys_to_process:
             print("Warning: No S3 keys were generated to process. Check configuration.", file=sys.stderr)
             sys.exit(0)

        target_points_xyz = np.array([latlon_to_xyz(lat, lon) for lat, lon in target_points])

        for s3_key in s3_keys_to_process:
            result = process_grib_file(
                s3_client=s3_client,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                target_points_xyz=target_points_xyz,
                target_variables=target_vars,
                grid_cache=grid_cache,
                db_con=con,
                table_name=table_name
            )
            total_messages_scanned += result["messages_scanned"]
            total_rows_attempted_insert += result["rows_inserted"]
            targets_found.update(result["variables_found_in_file"])

            if result["status"] == "processed":
                files_successfully_downloaded += 1
                files_processed_ok += 1
            elif result["status"] == "skipped_no_grid":
                files_successfully_downloaded += 1
                files_skipped_no_grid += 1
            elif result["status"] == "not_found":
                files_not_found += 1
            elif result["status"] == "download_error":
                 files_with_download_errors += 1
            elif result["status"] == "processing_error":
                 files_successfully_downloaded += 1
                 files_with_processing_errors += 1

    except duckdb.Error as dberr:
         print(f"A DuckDB error occurred outside file processing loop: {dberr}", file=sys.stderr)
    except Exception as ex:
        print(f"An unexpected error occurred in the main process: {ex}", file=sys.stderr)
    finally:
        if con:
            print(f"Closing DuckDB connection")
            con.close()

    print("\n" + "=" * 40)
    print("          Processing Summary")
    print("=" * 40)
    print(f"Attempted to process {len(s3_keys_to_process)} S3 files.")
    print(f"Files successfully downloaded: {files_successfully_downloaded}")
    if files_not_found > 0: print(f"Files not found on S3: {files_not_found}")
    if files_with_download_errors > 0: print(f"Files with download errors: {files_with_download_errors}")
    if files_skipped_no_grid > 0: print(f"Files downloaded but skipped (grid issue): {files_skipped_no_grid}")
    if files_with_processing_errors > 0: print(f"Files downloaded but failed during processing: {files_with_processing_errors}")
    print(f"Total GRIB messages scanned across all processed files: {total_messages_scanned}")
    print(f"\nTotal rows prepared for insertion into '{table_name}': {total_rows_attempted_insert}")
    print(f"(Actual rows inserted depends on conflicts 'ON CONFLICT DO NOTHING')")
    print(f"\nTarget variable types found across all files ({len(targets_found)} out of {len(target_vars)} types):")
    for target_def in target_vars:
        t_name = target_def["user_name"]
        status = "[X]" if t_name in targets_found else "[ ]"
        print(f"  {status} {t_name}")
    print("=" * 40)