import os
import pandas as pd
import numpy as np
import pytest

from ..utils import parse_duration, parse_date_added, explode_genres


def test_parse_duration_minutes():
    val = "90 min"
    num, dtype = parse_duration(val)
    assert dtype == "min"
    assert num == 90

    # unrealistic minutes flagged as NaN
    val2 = "1000 min"
    num2, dtype2 = parse_duration(val2)
    assert np.isnan(num2)


def test_parse_duration_seasons():
    v = "3 Seasons"
    num, dtype = parse_duration(v)
    assert dtype == "seasons"
    assert num == 3

    v2 = "1 Season"
    num2, dtype2 = parse_duration(v2)
    assert dtype2 == "seasons"
    assert num2 == 1


def test_parse_date_added_various():
    d1 = "September 9, 2019"
    parsed1 = parse_date_added(d1)
    assert parsed1.year == 2019

    d2 = "2018-04-05"
    parsed2 = parse_date_added(d2)
    assert parsed2.year == 2018

    d3 = "not a date"
    parsed3 = parse_date_added(d3)
    assert pd.isna(parsed3)


def test_genre_explosion(tmp_path):
    # create a simple df
    df = pd.DataFrame({
        "show_id": ["s1", "s2"],
        "title": ["A", "B"],
        "type": ["Movie", "TV Show"],
        "listed_in": ["Dramas, International Movies", "Comedies"],
        "country": ["United States", "India"],
    })
    # add columns used by explode_genres
    df["primary_country"] = df["country"].apply(lambda x: x.split(",")[0].strip())
    df["added_year"] = 2020
    df["release_year"] = 2019
    df["rating"] = "PG"
    df["duration_num"] = 90
    df["duration_type"] = "min"

    exploded = explode_genres(df, genres_col="listed_in")
    # s1 -> 2 genres, s2 -> 1 genre => total 3 rows
    assert len(exploded) == 3
    assert set(exploded["genre"].unique()) == {"Dramas", "International Movies", "Comedies"}
    # check required columns present
    for c in ["show_id", "title", "type", "genre", "primary_country", "added_year"]:
        assert c in exploded.columns
