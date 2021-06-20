from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

import argparse
from datetime import datetime
import shutil
import os

from s3fs import S3FileSystem

import pybaseball as pybll


def make_local_archive():
    pybll.cache.enable()
    START_DATE = "2021-04-01"
    TODAY = str(datetime.now().date())
    # ENFOREMENT_DATE = "2021-06-15"

    data = pybll.statcast(START_DATE, TODAY)

    archive = shutil.make_archive("pybaseball_cache", root_dir=os.environ["PYBASEBALL_CACHE"], format="zip")

    print(archive)

    return archive, data

def make_s3_archive():
    local_archive, data = make_local_archive()
    bucket = os.environ["BUCKET"]
    s3_creds = {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    fs = S3FileSystem(**s3_creds)
    s3_archive = f"s3://{bucket}/pybaseball_cache.zip"
    with fs.open(s3_archive, "wb") as s3_zip:
        with open(local_archive, "rb") as local_zip:
            s3_zip.write(local_zip.read())
        
    print(f"Updated cache file at {s3_archive}.")

    return s3_archive, data


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