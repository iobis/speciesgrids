import sys
import os
import logging
import glob
import re
import duckdb
import pandas as pd


logger = logging.getLogger(__name__)
TEMP_DIR = "temp"
OUTPUT_DIR = "output"


def read_df(file):
    source = re.match(os.path.join(TEMP_DIR, "(.+?)", ""), file)[1]
    df = duckdb.query(f"select * from read_parquet('{file}')").df()
    df[f"source_{source}"] = True
    return df


def merge_quadkeys(h3_resolution, sources):

    # get relevant files within datasets and subsets

    files = [file for file in glob.iglob(os.path.join(TEMP_DIR, "**"), recursive=True) if re.search("/[0-9]+$", file) and os.path.isfile(file)]
    quadkeys = list(set([re.search("[0-9]+$", file).group(0) for file in files]))

    # merge

    for quadkey in quadkeys:
        quadkey_files = [file for file in files if re.search(f"/{quadkey}$", file)]

        dfs = list(map(lambda x: read_df(x), quadkey_files))
        df = pd.concat(dfs)

        aggs = {
            "records": lambda x: x.sum(),
            "min_year": "min",
            "max_year": "max"
        }
        for source in sources:
            aggs[f"source_{source}"] = "any"

        df = df.groupby([f"h3_0{h3_resolution}", "species", "AphiaID"], dropna=False).agg(aggs).reset_index()

        df["min_year"] = df["min_year"].astype("Int64")
        df["max_year"] = df["max_year"].astype("Int64")
        df["AphiaID"] = df["AphiaID"].astype("Int32")

        # to geopandas

        df.set_index(f"h3_0{h3_resolution}", inplace=True)
        df = df.h3.h3_to_geo()

        # output

        output_file = os.path.join(OUTPUT_DIR, quadkey)
        logger.info(f"Writing {len(df)} results to {output_file}")
        df.to_parquet(output_file, index=False)


if __name__ == "__main__":
    merge_quadkeys()
