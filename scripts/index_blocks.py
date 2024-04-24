import sys
from lib import get_quadkeys
from pyquadkey2.quadkey import TileAnchor
import duckdb
import logging
import h3pandas
import re
import os


logger = logging.getLogger(__name__)


def main(input_file, quadkey_level, h3_resolution):

    quadkeys = get_quadkeys(int(quadkey_level))
    dataset, subset = re.search("data/(.*)/(.*)", input_file).groups()

    logger.info(f"Indexing {input_file}, dataset {dataset}, subset {subset}")

    os.makedirs(os.path.join("output", dataset), exist_ok=True)

    # get columns

    res_cols = duckdb.query(f"describe select * from read_parquet('{input_file}')").fetchall()
    cols = [col[0] for col in res_cols]

    # query blocks

    for quadkey in quadkeys:
        south, west = quadkey.to_geo(anchor=TileAnchor.ANCHOR_SW)
        north, east = quadkey.to_geo(anchor=TileAnchor.ANCHOR_NE)

        if "gbifid" in cols:
            query = f"""
                select decimallongitude as decimalLongitude, decimallatitude as decimalLatitude, species, null as AphiaID, year
                from read_parquet('{input_file}')
                where decimallongitude >= {west} and decimallongitude <= {east} and
                decimallatitude >= {south} and decimallatitude <= {north}
            """
        else:
            query = f"""
                select decimalLongitude, decimalLatitude, species, AphiaID, date_year as year
                from read_parquet('{input_file}')
                where decimalLongitude >= {west} and decimalLongitude <= {east} and
                decimalLatitude >= {south} and decimalLatitude <= {north}
            """

        df = duckdb.query(query).df()
        df = df.h3.geo_to_h3(h3_resolution, "decimalLatitude", "decimalLongitude")

        if len(df) > 0:
            print(1)

        result = df.groupby([f"h3_0{h3_resolution}", "species", "AphiaID"], dropna=False).agg(
            records=("species", "size"),
            min_year=("year", lambda x: x.min(skipna=True)),
            max_year=("year", lambda x: x.max(skipna=True))
        ).reset_index()

        output_file = f"output/{dataset}/{subset}_{quadkey}.parquet"
        logger.info(f"Writing results to {output_file}")
        result.to_parquet(output_file)


if __name__ == "__main__":
    print(sys.argv)
    input = sys.argv[1]
    quadkey_level = sys.argv[2]
    h3_resolution = sys.argv[3]
    main(input, quadkey_level, h3_resolution)
