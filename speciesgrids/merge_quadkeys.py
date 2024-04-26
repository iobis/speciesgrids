import sys
import os
import logging
import glob
import re
import duckdb


logger = logging.getLogger(__name__)
TEMP_DIR = "output"
OUTPUT_DIR = "output"


def merge_quadkeys():

    # get relevant files within datasets and subsets

    files = [file for file in glob.iglob(os.path.join("output", "**"), recursive=True) if re.search("/[0-9]+$", file)]
    quadkeys = list(set([re.search("[0-9]+$", file).group(0) for file in files]))

    # merge

    for quadkey in quadkeys:
        quadkey_files = [file for file in files if re.search(f"/{quadkey}$", file)]
        for file in quadkey_files:
            df = duckdb.query(f"select * from read_parquet('{file}')").df()
            if len(df) > 0:
                print(df)

    # output


if __name__ == "__main__":
    merge_quadkeys()
