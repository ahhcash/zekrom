# zekrom

A command-line tool to fetch HRRR (High-Resolution Rapid Refresh) GRIB2 forecast data for specific geographic points and variables from AWS S3, process it, and store the results in a DuckDB database.

## Features

*   Fetches specific variables from HRRR GRIB2 files hosted on AWS S3 (`s3://hrrrzarr/`).
*   Extracts data for a list of target latitude/longitude points.
*   Processes data for a specified date range and forecast cycle (06Z by default).
*   Stores extracted data efficiently in a DuckDB database.
*   Configurable via `core/config.py` and command-line arguments.

## Prerequisites

*   **Python:** Version 3.12 or higher.
*   **UV:** The `uv` package manager is recommended for installing dependencies. You can install it via `pipx install uv` or `pip install uv`. See the [uv installation guide](https://github.com/astral-sh/uv#installation).
*   **eccodes:** This library is required for reading GRIB files. Installation methods vary by OS:
    *   **conda:** `conda install -c conda-forge eccodes python-eccodes`
    *   **macOS (Homebrew):** `brew install eccodes` (You might still need `pip install eccodes` or `uv pip install eccodes` for the Python bindings).
    *   **Linux (apt):** `sudo apt-get update && sudo apt-get install libeccodes-dev` (Then use `pip` or `uv` for Python bindings).
    *   Refer to the official [ecCodes documentation](https://confluence.ecmwf.int/display/ECC/ecCodes+installation) for detailed instructions.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd zekrom
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    # Using venv
    python -m venv .venv
    source .venv/bin/activate # On Windows use `.venv\Scripts\activate`

    # Or using uv
    uv venv
    source .venv/bin/activate
    ```

3.  **Install dependencies using UV:**
    Ensure `eccodes` (the system library) is installed first (see Prerequisites).
    ```bash
    uv pip install -r requirements.txt # Assuming you generate one from pyproject.toml
    # Or directly install the project in editable mode
    uv pip install -e .
    ```
    *(Alternatively, if not using uv, you can use pip with the eccodes bindings installed separately)*
    ```bash
    # Make sure eccodes system lib is installed first!
    pip install eccodes # Or python-eccodes if using conda for eccodes
    pip install -e .
    ```

## Configuration

Core settings are defined in `core/config.py`. You may want to review or modify:

*   `TARGET_VARIABLES`: Default list of weather variables to extract.
*   `S3_BUCKET`: The AWS S3 bucket containing HRRR data (defaults to `hrrrzarr`).
*   `CYCLE`: The forecast cycle hour (e.g., "06").
*   `FILE_TYPE`: Type of HRRR file (e.g., "sfc").
*   `FORECAST_HOURS_START`/`END`: Range of forecast hours within each cycle file.
*   `DUCKDB_FILE`: Default path for the output database file (`data.duckdb`).
*   `TABLE_NAME`: Default table name within the database (`hrrr_data`).

## Input Data Format

The tool requires a CSV file specifying the target latitude and longitude points.

*   The file should contain two columns: latitude and longitude.
*   A header row is optional and will be skipped if present.
*   Example (`points.csv`):
    ```csv
    latitude,longitude
    40.7128,-74.0060
    34.0522,-118.2437
    ```
    *(The project includes a sample `points.csv` file.)*

## Usage

The tool is run using the `zekrom` command-line script after installation.

```bash
zekrom <target_points_file> [OPTIONS]
```

**Arguments:**

*   `target_points_file`: (Required) Path to the CSV file containing target points (e.g., `points.csv`).

**Options:**

*   `--run-date <YYYYMMDD>`: Latest forecast run date to include. Defaults to yesterday (UTC).
*   `--num-hours <INTEGER>`: How many hours of 06Z forecast runs to look back from `--run-date`. Defaults to 48.
*   `--variables <VAR1,VAR2,...>`: Comma-separated list of variables to ingest (use user_names from `config.py`). Defaults to all variables in `config.TARGET_VARIABLES`.
*   `--db-file <PATH>`: Path to the output DuckDB file. Defaults to `data.duckdb`.
*   `--table-name <NAME>`: Name for the table within the database. Defaults to `hrrr_data`.
*   `--help`: Show the help message and exit.

**Examples:**

1.  **Ingest data for default variables and time range using `points.csv`:**
    ```bash
    zekrom points.csv
    ```

2.  **Ingest data for a specific date (2024-04-28) and look back 24 hours:**
    ```bash
    zekrom points.csv --run-date 20140428 --num-hours 24
    ```

3.  **Ingest only temperature and wind components, saving to a different database:**
    ```bash
    zekrom points.csv --variables "temperature_2m,u_component_wind_10m,v_component_wind_10m" --db-file my_weather.db
    ```

## Dependencies

*   [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html): AWS SDK for Python.
*   [DuckDB](https://duckdb.org/): In-process analytical data management system.
*   [ecCodes](https://confluence.ecmwf.int/display/ECC/ecCodes+Home): ECMWF library for decoding/encoding meteorological data formats (GRIB, BUFR).
*   [scikit-learn](https://scikit-learn.org/stable/): Used for finding nearest neighbors (likely for matching points to grid).
*   [uv](https://github.com/astral-sh/uv): Fast Python package installer and resolver.
