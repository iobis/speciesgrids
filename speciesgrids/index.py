import pandas as pd
import duckdb
import logging
import os
import shutil
import h3pandas  # noqa: F401


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

        # handle empy dataframe

        if len(df) == 0:
            for quadkey in self.quadkeys:
                output_file = os.path.join(output_path, quadkey)
                pd.DataFrame().to_parquet(output_file)
            return

        # fix types

        df["min_year"] = df["min_year"].astype("Int64")
        df["max_year"] = df["max_year"].astype("Int64")

        # summarize

        df = self.grid.summarize_occurrences(df)

        # output

        for quadkey in self.quadkeys:
            df_quadkey = df[df["quadkey"] == quadkey]
            output_file = os.path.join(output_path, quadkey)
            logger.info(f"Writing {len(df_quadkey)} results to {output_file}")
            df_quadkey.drop(columns="quadkey").to_parquet(output_file, index=False)
