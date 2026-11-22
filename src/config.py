"""Shared names and settings for the pipeline."""
CATALOG = "nyc_weather_risk"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

SOCRATA_BASE = "https://data.cityofnewyork.us/resource/{}.json"
DATASETS = {
    "crashes": "h9gi-nx95",
}
PAGE_SIZE = 50_000