from lib import row_to_quadkey
import pandas as pd
import duckdb
import logging
import os
import shutil
import h3pandas  # noqa: F401
import geohash2
from pyquadkey2.quadkey import QuadKey


logger = logging.getLogger(__name__)


class Indexer:

    def index(self):

        for source, source_path in self.sources.items():

            logger.info(f"Processing source {source}")
            source_output_path = os.path.join(self.temp_path, source)
            logger.info(f"Clearing output directory {source_output_path}")
            if os.path.exists(source_output_path) and os.path.isdir(source_output_path):
                shutil.rmtree(source_output_path)
            os.makedirs(source_output_path, exist_ok=False)

            for file in self.get_source_files(source_path):
                source_file = os.path.join(source_path, file)
                output_path = os.path.join(self.temp_path, source, file)
                os.makedirs(output_path, exist_ok=False)
                self.index_to_quadkey(source_file, output_path)

    def summarize_occurrences(self, df: pd.DataFrame) -> pd.DataFrame:

        # fix types

        df["min_year"] = df["min_year"].astype("Int64")
        df["max_year"] = df["max_year"].astype("Int64")

        # summarize

        if self.aggregation["type"] == "h3":
            return self.summarize_occurrences_h3(df)
        elif self.aggregation["type"] == "geohash":
            return self.summarize_occurrences_geohash(df)
        else:
            raise ValueError(f"Unsupported aggregation type {self.aggregation['type']}")

    def summarize_occurrences_geohash(self, df: pd.DataFrame) -> pd.DataFrame:

        # calculate geohash, drop coordinates

        df["cell"] = df.apply(lambda row: geohash2.encode(row["decimalLatitude"], row["decimalLongitude"], precision=self.aggregation["resolution"]), axis=1)
        df.drop(["decimalLongitude", "decimalLatitude"], axis=1, inplace=True)

        # aggregate by geohash

        df = df.groupby(["cell", "species", "AphiaID"], dropna=False).agg(
            records=("records", lambda x: x.sum()),
            min_year=("min_year", lambda x: x.min(skipna=True)),
            max_year=("max_year", lambda x: x.max(skipna=True))
        ).reset_index()

        # calculate center geometry and quadkey, drop geometry

        df[["y", "x"]] = df["cell"].apply(lambda x: geohash2.decode_exactly(x)[0:2]).apply(pd.Series)
        df["quadkey"] = df.apply(lambda row: str(QuadKey.from_geo((row["y"], row["x"]), self.quadkey_level)), axis=1)
        df = pd.DataFrame(df.drop(columns=["x", "y"]))
        df.reset_index(inplace=True)

        return df

    def summarize_occurrences_h3(self, df: pd.DataFrame) -> pd.DataFrame:

        # calculate h3, drop coordinates

        df = df.h3.geo_to_h3(self.aggregation["resolution"], "decimalLatitude", "decimalLongitude", set_index=True)
        df["cell"] = df.index
        df.drop(["decimalLongitude", "decimalLatitude"], axis=1, inplace=True)

        # aggregate by h3

        df = df.groupby(["cell", "species", "AphiaID"], dropna=False).agg(
            records=("records", lambda x: x.sum()),
            min_year=("min_year", lambda x: x.min(skipna=True)),
            max_year=("max_year", lambda x: x.max(skipna=True))
        ).reset_index()

        # calculate center geometry and quadkey, drop geometry

        df.set_index("cell", inplace=True)
        df = df.h3.h3_to_geo()
        df["quadkey"] = df.apply(lambda row: row_to_quadkey(row, self.quadkey_level), axis=1)
        df = pd.DataFrame(df.drop(columns="geometry"))
        df.reset_index(inplace=True)

        return df

    def index_to_quadkey(self, source_file, output_path):
        """Indexes an occurrence parquet file to H3 and partitions output by quadkey."""

        # create query

        res_cols = duckdb.query(f"describe select * from read_parquet('{source_file}')").fetchall()
        cols = [col[0] for col in res_cols]

        if "gbifid" in cols:
            query = f"""
                select
                    decimallongitude as decimalLongitude,
                    decimallatitude as decimalLatitude,
                    worms.scientificName as species,
                    worms.AphiaID::int64 as AphiaID,
                    min(year) as min_year,
                    max(year) as max_year,
                    count(*) as records
                from read_parquet('{source_file}')
                left join read_parquet('{self.worms_output_path}') worms on specieskey = ID
                where worms.AphiaID is not null and decimallongitude is not null and decimallatitude is not null
                group by decimallongitude, decimallatitude, worms.scientificName, worms.AphiaID
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
                from read_parquet('{source_file}')
                where species is not null and decimalLongitude is not null and decimalLatitude is not null
                group by decimalLongitude, decimalLatitude, species, AphiaID
            """

        df = duckdb.query(query).to_df()

        # summarize

        if len(df) == 0:
            for quadkey in self.quadkeys:
                output_file = os.path.join(output_path, quadkey)
                pd.DataFrame().to_parquet(output_file)
            return

        df = self.summarize_occurrences(df)

        # output

        for quadkey in self.quadkeys:
            df_quadkey = df[df["quadkey"] == quadkey]
            output_file = os.path.join(output_path, quadkey)
            logger.info(f"Writing {len(df_quadkey)} results to {output_file}")
            df_quadkey.drop(columns="quadkey").to_parquet(output_file, index=False)
