S3_BUCKET = "noaa-hrrr-bdp-pds"
RUN_DATE = "20150323"  # The most recent date for the 06Z run to include
NUM_HOURS = 48
CYCLE = "06"
FILE_TYPE = "wrfsfc"
FORECAST_HOURS_START = 0
FORECAST_HOURS_END = 15

DUCKDB_FILE = "data.duckdb"
TABLE_NAME = "hrrr_forecasts"


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