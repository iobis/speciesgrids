import sys
from speciesgrids.lib import get_quadkeys, row_to_quadkey
from pyquadkey2.quadkey import TileAnchor, QuadKey
import pandas as pd
import duckdb
import logging
import h3pandas
import re
import os


logger = logging.getLogger(__name__)
OUTPUT_DIR = "temp"


def index_to_quadkey(input_file, quadkey_level, h3_resolution, overwrite=False):
    """Indexes an occurrence parquet file to h3 and partitions output by quadkey.
    Input files are expected to be in the format data/{dataset}/{subset}.
    Output files are written to output/{dataset}/{subset}/{quadkey}."""

    dataset, subset = re.search("data/(.*)/(.*)", input_file).groups()
    logger.info(f"Indexing and partitioning {input_file}")
    os.makedirs(os.path.join(OUTPUT_DIR, dataset, subset), exist_ok=True)

    quadkeys = get_quadkeys(quadkey_level)

    # check if output already exists

    output_files = [os.path.join(OUTPUT_DIR, dataset, subset, quadkey) for quadkey in quadkeys]
    if overwrite and all([os.path.exists(output_file) for output_file in output_files]):
        logger.info("Output already exists, skipping")
        return
    else:
        for output_file in output_files:
            if os.path.exists(output_file):
                os.remove(output_file)

    # create query

    res_cols = duckdb.query(f"describe select * from read_parquet('{input_file}')").fetchall()
    cols = [col[0] for col in res_cols]

    if "gbifid" in cols:
        query = f"""
            select
                decimallongitude as decimalLongitude,
                decimallatitude as decimalLatitude,
                species,
                null as AphiaID,
                min(year) as min_year,
                max(year) as max_year,
                count(*) as records
            from read_parquet('{input_file}')
            where species is not null and decimallongitude is not null and decimallatitude is not null
            group by decimallongitude, decimallatitude, species
        """
    else:
        query = f"""
            select
                decimalLongitude,
                decimalLatitude,
                species,
                AphiaID,
                min(date_year) as min_year,
                max(date_year) as max_year,
                count(*) as records
            from read_parquet('{input_file}')
            where species is not null and decimalLongitude is not null and decimalLatitude is not null
            group by decimalLongitude, decimalLatitude, species, AphiaID
        """

    df = duckdb.query(query).to_df()

    # calculate h3, drop coordinates

    df = df.h3.geo_to_h3(h3_resolution, "decimalLatitude", "decimalLongitude", set_index=True)
    df.reset_index(inplace=True)

    # if (df['h3_07'] == "0").any():
    #     print(1)

    df.drop(["decimalLongitude", "decimalLatitude"], axis=1, inplace=True)

    # aggregate by h3

    df = df.groupby([f"h3_0{h3_resolution}", "species", "AphiaID"], dropna=False).agg(
        records=("records", lambda x: x.sum()),
        min_year=("min_year", lambda x: x.min(skipna=True)),
        max_year=("max_year", lambda x: x.max(skipna=True))
    ).reset_index()

    # calculate center geometry and quadkey, drop geometry

    df.set_index(f"h3_0{h3_resolution}", inplace=True)
    df = df.h3.h3_to_geo()
    df["quadkey"] = df.apply(lambda row: row_to_quadkey(row, quadkey_level), axis=1)
    df = pd.DataFrame(df.drop(columns="geometry"))
    df.reset_index(inplace=True)

    # output

    for quadkey in quadkeys:
        df_quadkey = df[df["quadkey"] == quadkey]
        output_file = os.path.join(OUTPUT_DIR, dataset, subset, quadkey)
        logger.info(f"Writing {len(df_quadkey)} results to {output_file}")
        df_quadkey.drop(columns="quadkey").to_parquet(output_file)


if __name__ == "__main__":
    input = sys.argv[1]
    quadkey_level = sys.argv[2]
    h3_resolution = sys.argv[3]
    index_to_quadkey(input, quadkey_level, h3_resolution)
