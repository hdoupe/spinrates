from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import argparse
from datetime import datetime
import pandas as pd
import numpy as np
import os

from s3fs import S3FileSystem

import pybaseball as pybll


def make_local_archive():
    START_DATE = "2021-04-01"
    TODAY = str(datetime.now().date())

    pybll.cache.enable()

    TODAY = str(datetime.now().date())

    print("Loading data with pybll now.")
    df = pybll.statcast(START_DATE, TODAY)

    print(df.tail())

    df = df[
        [
            "game_date",
            "pitch_type",
            "release_speed",
            "batter",
            "pitcher",
            "effective_speed",
            "release_spin_rate",
            "home_team",
            "away_team",
            "inning_topbot",
            "player_name",
        ]
    ]
    df["game_date"] = pd.to_datetime(df["game_date"], format="%y%m%d")
    fastballs = ["FF", "FC", "SI", "FS", "FA", "FT", "SF"]
    other = ["UN", "XX", "PO", "FO"]
    df["pitch_class"] = np.where(
        df["pitch_type"].isin(fastballs),
        "fastball",
        np.where(df["pitch_type"].isin(other), "other", "offspeed"),
    )

    # ughh \_o_/: ValueError: Don't know how to convert df type: Float64
    df["release_speed"] = df.release_speed.astype(np.float64)
    df["effective_speed"] = df.effective_speed.astype(np.float64)
    df.to_parquet("statcast_data.parquet", engine="fastparquet")
    return df, "statcast_data.parquet"

def make_s3_archive():
    df, local_path = make_local_archive()
    # local_path = "statcast_data.parquet"
    bucket = os.environ["BUCKET"]
    s3_creds = {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    fs = S3FileSystem(**s3_creds)
    s3_archive = f"s3://{bucket}/statcast_data.parquet"
    print(f"Writing to s3 bucket: {s3_archive}")

    with fs.open(s3_archive, "wb") as s3_cache:
        with open(local_path, "rb") as local_cache:
            s3_cache.write(local_cache.read())

    print(f"Updated cache file at {s3_archive}.")

    return df, s3_archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for writing the spinrates cache.")
    parser.add_argument("--storage", "-s", required=True, help="Where to store data. Options are: 'local', 's3'.")
    args = parser.parse_args()

    if args.storage == "local":
        make_local_archive()
    elif args.storage == "s3":
        make_s3_archive()
    else:
        raise ValueError(f"Unknown storage backend: {args.storage}.")