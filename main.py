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

TARGET_VARIABLES = [
    {
        "user_name": "surface_pressure",
        "paramId": 134, "shortName": "sp", "typeOfLevel": "surface", "level": 0,
    }, {
        "user_name": "surface_roughness",
        "paramId": 173, "shortName": "sro", "typeOfLevel": "surface", "level": 0,
    }, {
        "user_name": "visible_beam_downward_solar_flux",
        "paramId": 186, "shortName": "fdir", "typeOfLevel": "surface", "level": 0,
    }, {
        "user_name": "visible_diffuse_downward_solar_flux",
        "paramId": 175, "shortName": "ssrd", "typeOfLevel": "surface", "level": 0,
    }, {
        "user_name": "temperature_2m",
        "paramId": 167, "shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2,
    }, {
        "user_name": "dewpoint_2m",
        "paramId": 168, "shortName": "2d", "typeOfLevel": "heightAboveGround", "level": 2,
    }, {
        "user_name": "relative_humidity_2m",
        "paramId": 157, "shortName": "r", "typeOfLevel": "heightAboveGround", "level": 2,
    }, {
        "user_name": "u_component_wind_10m",
        "paramId": 165, "shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10,
    }, {
        "user_name": "v_component_wind_10m",
        "paramId": 166, "shortName": "10v", "typeOfLevel": "heightAboveGround", "level": 10,
    }, {
        "user_name": "u_component_wind_80m",
        "paramId": 246, "shortName": "u", "typeOfLevel": "heightAboveGround", "level": 80,
    }, {
        "user_name": "v_component_wind_80m",
        "paramId": 247, "shortName": "v", "typeOfLevel": "heightAboveGround", "level": 80,
    },
]

USER_TARGET_POINTS = [
    # (Latitude, Longitude)
    (31.006900, -88.010300),
    (31.756900, -106.375000),
    (32.583889, -86.283060),
    (32.601700, -87.781100),
    (32.618900, -86.254800),
    (33.255300, -87.449500),
    (33.425878, -86.337550),
    (33.458665, -87.356820),
    (33.784500, -86.052400),
    (55.339722, -160.497200),
]

S3_BUCKET = "noaa-hrrr-bdp-pds"
DATE = "20150323"
CYCLE = "06"
FILE_TYPE = "wrfnat"
FORECAST_HOURS_START = 0
FORECAST_HOURS_END = 15

DUCKDB_FILE = "data.duckdb"
TABLE_NAME = "hrrr_forecasts"

def latlon_to_xyz(lat, lon):
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    x = np.cos(lat_rad) * np.cos(lon_rad)
    y = np.cos(lat_rad) * np.sin(lon_rad)
    z = np.sin(lat_rad)
    return x, y, z

def main():
    total_rows_inserted = 0
    total_messages_scanned= 0
    files_processed = 0
    files_not_found = 0
    targets_found = set()
    s3_keys_to_process = []
    grid_cache = {}

    con = None
    try:
        print(f"Connecting to DuckDB database: {DUCKDB_FILE}")
        con = duckdb.connect(database=DUCKDB_FILE, read_only=False)
        print(f"Creating table {TABLE_NAME} with idempotence")
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
        print(f"Targeting S3 Bucket: {S3_BUCKET}")

        print(
            f"Generating file list for date {DATE}, cycle {CYCLE}Z, type {FILE_TYPE}, F{FORECAST_HOURS_START:02d}-F{FORECAST_HOURS_END:02d}...")
        for hour in range(FORECAST_HOURS_START, FORECAST_HOURS_END + 1):
            key = f"hrrr.{DATE}/conus/hrrr.t{CYCLE}z.{FILE_TYPE}f{hour:02d}.grib2"
            s3_keys_to_process.append(key)
        print(f"Generated {len(s3_keys_to_process)} S3 file keys to process.")

        target_points = np.array([latlon_to_xyz(lat, lon) for lat, lon in USER_TARGET_POINTS])

        for s3_key in s3_keys_to_process:
            temp_filepath = None
            print(f"Attempting to process S3 file: s3://{S3_BUCKET}/{s3_key}")
            nearest_indices = None
            cached_lats = None
            cached_lons = None
            grid_calc_success = False

            try:
                with tempfile.NamedTemporaryFile(delete=False) as temp_f:
                    temp_filepath = temp_f.name

                print(f"Downloading temporary file to {temp_filepath}")
                s3_client.download_file(S3_BUCKET, s3_key, temp_filepath)
                print(f"Download complete.")

                files_processed += 1

                print(f"  Peeking into temporary file for grid info: {temp_filepath}")
                with open(temp_filepath, 'rb') as peek_f:
                    gid_peek = eccodes.codes_grib_new_from_file(peek_f)
                    if gid_peek:
                        grid_ni = eccodes.codes_get_long(gid_peek, "Ni")  # Number of points along X axis
                        grid_nj = eccodes.codes_get_long(gid_peek, "Nj")  # Number of points along Y axis
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
                                grid_lats_flat = grid_lats_arr.flatten();
                                grid_lons_flat = grid_lons_arr.flatten()
                                grid_lons_flat[grid_lons_flat > 180] -= 360
                                grid_xyz = np.array(latlon_to_xyz(grid_lats_flat, grid_lons_flat)).T
                                kdtree = KDTree(grid_xyz)
                                distances, indices = kdtree.query(target_points, k=1)
                                grid_cache[current_file_grid_id] = {'lats': grid_lats_flat, 'lons': grid_lons_flat,
                                                                    'indices': indices}
                                print(f"  Calculated and cached nearest points.")
                                grid_calc_success = True
                            else:
                                print(f"  Warning: Invalid grid coordinates.", file=sys.stderr)
                        else:  # Grid is in cache
                            print(f"  Grid found in cache.")
                            grid_calc_success = True

                        if grid_calc_success and current_file_grid_id in grid_cache:
                            cached_grid = grid_cache[current_file_grid_id]
                            nearest_indices = cached_grid['indices']
                            cached_lats = cached_grid['lats']
                            cached_lons = cached_grid['lons']
                        else:
                            grid_calc_success = False
                            print(f"  Warning: Could not get/calculate nearest indices for grid {current_file_grid_id}. No point data will be extracted from this file.", file=sys.stderr)

            except Exception as peek_err:  # Catch errors during peek/grid calc
                print(f"  Error during grid calculation/peek: {peek_err}", file=sys.stderr)
                grid_calc_success = False  # Ensure flag is false on error
            finally:
                try:
                    if gid_peek:
                        eccodes.codes_release(gid_peek)
                except Exception as exc:
                    print(f"Encountered exception when releasing gid for peek: {exc}")

            if nearest_indices is not None and cached_lats is not None and cached_lons is not None:
                rows_to_insert = []
                messages_scanned_in_file = 0
                try:
                    with open(temp_filepath, 'rb') as local_f:  # Open #2
                        while True:
                            gid = None
                            try:
                                gid = eccodes.codes_grib_new_from_file(local_f)
                                if not gid: break  # End of file

                                total_messages_scanned += 1
                                messages_scanned_in_file += 1
                                for target in TARGET_VARIABLES:
                                    target_name = target["user_name"]
                                    matched = True
                                    for key, target_val in target.items():
                                        if "user_name" == key:
                                            continue
                                        if not eccodes.codes_is_defined(gid, key):
                                            matched = False
                                            break
                                        try:
                                            if isinstance(target_val, str):
                                                msg_value = eccodes.codes_get_string(gid, key)
                                            elif isinstance(target_val, int):
                                                msg_value = eccodes.codes_get_long(gid, key)
                                            elif isinstance(target_val, float):
                                                msg_value = eccodes.codes_get_double(gid, key)
                                            else:
                                                msg_value = eccodes.codes_get(gid, key)
                                            if msg_value != target_val:
                                                matched = False
                                                break
                                        except (eccodes.KeyValueNotFoundError, eccodes.WrongTypeError,
                                                eccodes.EncodingError):
                                            matched = False
                                            break
                                    if matched:
                                        targets_found.add(target_name)
                                        try:
                                            values_flat = eccodes.codes_get_values(gid).flatten()
                                            if values_flat.size != cached_lats.size:
                                                print(
                                                    f"  Warning: Size mismatch for {target_name} msg {messages_scanned_in_file}.",
                                                    file=sys.stderr)
                                                continue

                                            date_val = eccodes.codes_get_long(gid, 'date');
                                            time_val = eccodes.codes_get_long(gid, 'time');
                                            step_val = eccodes.codes_get_long(gid, 'step')
                                            run_dt_utc = datetime.strptime(f"{date_val}{time_val:04d}",
                                                                           "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                                            valid_dt_utc = run_dt_utc + timedelta(hours=step_val)
                                            source_s3_path = f"s3://{S3_BUCKET}/{s3_key}"

                                            # --- Prepare rows using nearest_indices ---
                                            for i, flat_idx in enumerate(nearest_indices):
                                                point_lat = cached_lats[flat_idx]
                                                point_lon = cached_lons[flat_idx]
                                                point_val = values_flat[flat_idx]
                                                row = (valid_dt_utc, run_dt_utc, point_lat, point_lon, target_name,
                                                       point_val, source_s3_path)
                                                rows_to_insert.append(row)
                                            total_rows_inserted += len(nearest_indices)  # Count rows prepared

                                        except Exception as extract_err:
                                            print(
                                                f"  Error extracting data for {target_name} msg {messages_scanned_in_file}: {extract_err}",
                                                file=sys.stderr)
                                        # Break inner target loop
                                        break
                            finally:
                                if gid: eccodes.codes_release(gid)
                except FileNotFoundError:
                    print(f"Error: Temp file {temp_filepath} disappeared before message processing?", file=sys.stderr)
                except Exception as proc_err:
                    print(f"Error during message processing loop for {temp_filepath}: {proc_err}", file=sys.stderr)

                if rows_to_insert:
                    print(f"Inserting {len(rows_to_insert)} rows into DuckDB for file {s3_key}")
                    try:
                        con.executemany(f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING", rows_to_insert)
                        total_rows_inserted += len(rows_to_insert)
                    except Exception as dberr:
                        print(f"Error inserting batch of {len(rows_to_insert)} into DuckDB: {dberr}", file=sys.stderr)
                    print(f"Successfully inserted {len(rows_to_insert)} rows into DuckDB. Scanned {messages_scanned_in_file} messages for file {s3_key}")
            else:
                print(f"  Skipping message processing for {s3_key} due to missing grid indices")

    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            print(f"Warning: File not found on S3")
            files_not_found += 1
        else:
            print(f"Error accessing S3 file: {ce}", file=sys.stderr)
    except duckdb.ConnectionException as conne:
        print(f"Error connecting to DuckDB database {DUCKDB_FILE}: {conne}", file=sys.stderr)
    except Exception as ex:
        print(f"Encountered exception: {ex}", file=sys.stderr)
    finally:
        if con:
            print(f"closing DucklDB connection")
            con.close()
        if temp_filepath and os.path.exists(temp_filepath):
            try:
                # print(f"Deleting temporary file: {temp_filepath}") # Verbose
                os.remove(temp_filepath)
            except OSError as ose:
                print(f"Warning: Could not delete temporary file {temp_filepath}: {ose}", file=sys.stderr)

    print("\n" + "=" * 40)
    print("          Processing Summary")
    print("=" * 40)
    print(f"Attempted to process {len(s3_keys_to_process)} S3 files.")
    print(f"Successfully downloaded: {files_processed} files.")
    if files_not_found > 0:
        print(f"Files not found on S3: {files_not_found}")
    print(f"Total GRIB messages scanned across all processed files: {total_messages_scanned}")
    print(f"\nAttempted to insert approx {total_rows_inserted} rows into '{TABLE_NAME}'.")
    print(f"(Actual rows inserted depends on conflicts)")
    print(
        f"\nTarget variable types found and processed ({len(targets_found)} out of {len(TARGET_VARIABLES)} types):")
    for target_def in TARGET_VARIABLES:
        t_name = target_def["user_name"]
        if t_name in targets_found:
            print(f"  [X] {t_name}")
        else:
            print(f"  [ ] {t_name}")
    print("=" * 40)

if __name__ == "__main__":
    try:
        _ = eccodes.codes_get_api_version()
        print(f"Found eccodes version: {_}")
    except Exception as e:
        print("Error: Failed eccodes check...", file=sys.stderr); sys.exit(1)
    try:
        _ = boto3.__version__
        print(f"Found boto3 version: {_}")
    except ImportError:
        print("Error: boto3 library not found...", file=sys.stderr); sys.exit(1)
    try:
        _ = duckdb.sql('SELECT 42;')
        print(f"Found duckdb version: {duckdb.__version__}")
    except Exception as e:
        print("Error: duckdb library not found or failed check...", file=sys.stderr); sys.exit(1)
    try:
        _ = np.__version__
        print(f"Found numpy version: {_}")
    except ImportError:
        print("Error: numpy library not found...", file=sys.stderr); sys.exit(1)

    main()