"""
clean.py

Netflix dataset cleaning script.

Input:
  data/raw/netflix_titles.csv

Outputs (written to data/processed):
  - netflix_clean.csv                (one row per title)
  - netflix_genres_exploded.csv      (one row per title-genre)
  - data_profile.json                (summary stats)
  - logs/parse_errors.csv            (rows with problematic fields if any)

Usage:
  python src/data_prep/clean.py \
      --input data/raw/netflix_titles.csv \
      --out_dir data/processed

Notes:
- We explode genres into a separate CSV to keep Tableau responsive.
- We keep country as raw string and also write `primary_country` = first entry.
- We coerce problematic date/duration to NaN and log them. This keeps rows intact.
"""

import argparse
import json
import logging
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# Helper parsing functions

def parse_date_added(s):
    """
    Parse date_added into pandas Timestamp.
    Accepts formats like 'September 9, 2019' and ISO-like strings.
    Returns pd.NaT on failure.
    """
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    # sometimes the date is empty or 'null'
    if s.lower() in {"", "nan", "none", "null"}:
        return pd.NaT
    # try pandas parsing first (handles most human-readable formats)
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT


def parse_duration(s):
    """
    Parse duration strings like:
      '90 min' -> (90, 'min')
      '1 Season' -> (1, 'seasons')
      '3 Seasons' -> (3, 'seasons')
    Returns (None, None) on failure.

    Honest note: we'll coerce unrealistic minute values (>600) to NaN,
    because some scraped data has bad entries. Adjust threshold if you disagree.
    """
    if pd.isna(s):
        return (np.nan, None)
    s = str(s).strip()
    if s == "":
        return (np.nan, None)

    # numeric part
    m = re.search(r"(\d+)", s)
    if not m:
        return (np.nan, None)
    num = int(m.group(1))

    # determine type
    s_lower = s.lower()
    if "min" in s_lower:
        duration_type = "min"
        # sanity check
        if num <= 0 or num > 600:
            # log potential anomaly to be captured by validator
            return (np.nan, "min")
        return (num, duration_type)
    elif "season" in s_lower:
        duration_type = "seasons"
        if num <= 0 or num > 100:
            return (np.nan, "seasons")
        return (num, duration_type)
    else:
        # unknown unit — fallback: treat as minutes but mark type None
        return (num, None)


def normalize_text_field(s):
    """Trim whitespace and collapse multiple spaces. Return NaN if empty."""
    if pd.isna(s):
        return np.nan
    t = str(s).strip()
    t = re.sub(r"\s+", " ", t)
    return t if t != "" else np.nan


def extract_primary_country(country_field):
    """
    From 'United States, India' -> 'United States'
    If NaN -> NaN
    """
    if pd.isna(country_field):
        return np.nan
    parts = [p.strip() for p in str(country_field).split(",") if p.strip()]
    return parts[0] if parts else np.nan


def explode_genres(df, genres_col="listed_in"):
    """
    Returns a new DataFrame with one row per (show_id, genre).
    Input df must have 'show_id' and the `genres_col` which is comma-separated.
    """
    df = df.copy()
    # fillna to avoid errors
    df[genres_col] = df[genres_col].fillna("")
    # split into lists
    df["__genre_list"] = df[genres_col].apply(lambda x: [g.strip() for g in x.split(",") if g.strip()])
    # explode
    exploded = df.explode("__genre_list")
    # rename and select columns
    exploded = exploded.rename(columns={"__genre_list": "genre"})
    exploded = exploded[exploded["genre"].notna() & (exploded["genre"] != "")]
    # keep relevant columns for genre table
    cols_to_keep = [
        "show_id",
        "title",
        "type",
        "genre",
        "primary_country",
        "added_year",
        "release_year",
        "rating",
        "duration_num",
        "duration_type",
    ]
    # some columns may not exist yet; guard
    existing = [c for c in cols_to_keep if c in exploded.columns]
    return exploded[existing].reset_index(drop=True)


# Validation & profiling

def basic_profile_and_validate(df, out_dir):
    """
    Run basic assertions and write a small profile JSON.
    Returns a dict profile.
    """
    profile = {}
    profile["raw_rows"] = int(len(df))
    profile["unique_show_id"] = int(df["show_id"].nunique())
    profile["type_counts"] = df["type"].value_counts(dropna=False).to_dict()
    profile["null_counts"] = df.isna().sum().to_dict()

    # added_year sanity
    current_year = datetime.now().year
    if "added_year" in df.columns:
        yr_min = int(df["added_year"].min(skipna=True)) if df["added_year"].notna().any() else None
        yr_max = int(df["added_year"].max(skipna=True)) if df["added_year"].notna().any() else None
        profile["added_year_min"] = yr_min
        profile["added_year_max"] = yr_max
        # warn if outside reasonable range
        if yr_min and (yr_min < 1920 or yr_max > current_year + 1):
            logging.warning("added_year contains values outside expected range (1920 - next year)")

    # write profile
    os.makedirs(out_dir, exist_ok=True)
    profile_path = os.path.join(out_dir, "data_profile.json")
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2)
    logging.info(f"Wrote data profile to {profile_path}")
    return profile


# Main cleaning pipeline

def clean_netflix(
    input_csv,
    out_dir="data/processed",
    logs_dir="logs",
    write_outputs=True,
    drop_rows=False,
):
    """
    Main pipeline:
      - loads input CSV
      - normalizes fields
      - parses date and duration
      - creates derived columns
      - writes netflix_clean.csv and netflix_genres_exploded.csv
    """
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    logging.info(f"Loading input CSV: {input_csv}")
    df = pd.read_csv(input_csv, dtype=str, low_memory=False)

    logging.info(f"Initial rows: {len(df)}")
    # normalize column names to snake_case
    df.columns = [re.sub(r"\s+", "_", c.strip()).lower() for c in df.columns]

    # ensure show_id exists
    if "show_id" not in df.columns:
        raise ValueError("Input CSV must contain 'show_id' column.")

    # strip whitespace from string fields (apply to a subset for speed)
    for col in ["title", "director", "cast", "country", "listed_in", "rating", "description", "duration"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text_field)

    # Parse date_added
    if "date_added" in df.columns:
        df["date_added_parsed"] = df["date_added"].apply(parse_date_added)
        df["date_added"] = df["date_added_parsed"]
        df = df.drop(columns=["date_added_parsed"])
    else:
        df["date_added"] = pd.NaT

    # derive added_year and added_month
    df["added_year"] = df["date_added"].dt.year
    df["added_month"] = df["date_added"].dt.month
    df["added_quarter"] = df["date_added"].dt.to_period("Q").astype(str)

    # release_year -> numeric
    if "release_year" in df.columns:
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce").astype("Int64")
    else:
        df["release_year"] = pd.NA

    # parse duration
    df["duration_num"] = np.nan
    df["duration_type"] = pd.NA
    if "duration" in df.columns:
        parsed = df["duration"].apply(parse_duration)
        df["duration_num"] = parsed.apply(lambda x: x[0])
        df["duration_type"] = parsed.apply(lambda x: x[1])

    # normalize rating
    if "rating" in df.columns:
        df["rating"] = df["rating"].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # primary_country
    if "country" in df.columns:
        df["primary_country"] = df["country"].apply(extract_primary_country)
    else:
        df["primary_country"] = pd.NA

    # title_slug
    df["title_slug"] = df["title"].fillna("").str.lower().str.replace(r"[^a-z0-9]+", "-", regex=True).str.strip("-")

    # keep original genres column as 'listed_in' (already normalized) and create 'genres' alias
    if "listed_in" in df.columns:
        df["genres"] = df["listed_in"]
    else:
        df["genres"] = pd.NA

    # Logging unusual values to parse_errors
    parse_errors = []
    # duration anomalies
    dur_anom = df[(df["duration_type"] == "min") & df["duration_num"].notna() & (df["duration_num"] > 600)]
    if not dur_anom.empty:
        for idx, row in dur_anom.iterrows():
            parse_errors.append({"show_id": row["show_id"], "field": "duration_num", "value": row["duration_num"]})

    # date anomalies: added_year outside reasonable range
    if df["added_year"].notna().any():
        current_year = datetime.now().year
        bad_dates = df[(df["added_year"] < 1920) | (df["added_year"] > current_year + 1)]
        if not bad_dates.empty:
            for idx, row in bad_dates.iterrows():
                parse_errors.append({"show_id": row["show_id"], "field": "added_year", "value": int(row["added_year"])})

    # write parse errors if any
    if parse_errors:
        parse_errors_df = pd.DataFrame(parse_errors)
        err_path = os.path.join(logs_dir, "parse_errors.csv")
        parse_errors_df.to_csv(err_path, index=False)
        logging.warning(f"Wrote parse errors to {err_path}")

    # Basic profile
    profile = basic_profile_and_validate(df, out_dir=out_dir)

    # Save netflix_clean.csv
    clean_path = os.path.join(out_dir, "netflix_clean.csv")
    if write_outputs:
        # select a stable column order if available
        cols_order = [
            "show_id",
            "title",
            "title_slug",
            "type",
            "director",
            "cast",
            "country",
            "primary_country",
            "genres",
            "release_year",
            "date_added",
            "added_year",
            "added_month",
            "added_quarter",
            "rating",
            "duration_num",
            "duration_type",
            "description",
        ]
        cols_existing = [c for c in cols_order if c in df.columns]
        # include any other columns at the end
        remaining = [c for c in df.columns if c not in cols_existing]
        final_cols = cols_existing + remaining
        df[final_cols].to_csv(clean_path, index=False, encoding="utf-8")
        logging.info(f"Wrote cleaned data to {clean_path}")

    # Create exploded genres CSV
    genres_df = explode_genres(df, genres_col="genres")
    genres_path = os.path.join(out_dir, "netflix_genres_exploded.csv")
    if write_outputs:
        genres_df.to_csv(genres_path, index=False, encoding="utf-8")
        logging.info(f"Wrote exploded genres to {genres_path} (rows: {len(genres_df)})")

    return {
        "clean_path": clean_path,
        "genres_path": genres_path,
        "profile": profile,
        "parse_errors": parse_errors,
    }


# CLI

def main():
    parser = argparse.ArgumentParser(description="Clean Netflix titles CSV for Tableau use.")
    parser.add_argument("--input", "-i", required=True, help="Input raw netflix_titles.csv")
    parser.add_argument("--out_dir", "-o", default="data/processed", help="Output directory for processed CSVs")
    parser.add_argument("--logs_dir", default="logs", help="Directory to write parse error logs")
    args = parser.parse_args()

    res = clean_netflix(input_csv=args.input, out_dir=args.out_dir, logs_dir=args.logs_dir, write_outputs=True)
    logging.info("Cleaning finished.")
    logging.info(json.dumps(res["profile"], indent=2))


if __name__ == "__main__":
    main()
