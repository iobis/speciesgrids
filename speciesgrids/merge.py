import os
import logging
import duckdb
import pandas as pd
from lib import clear_directory


logger = logging.getLogger(__name__)


class Merger:

    def read_df(self, file):
        return duckdb.query(f"select * from read_parquet('{file}')").df()

    def merge(self):

        clear_directory(self.output_path)

        for quadkey in self.quadkeys:

            dfs = []

            for source, source_path in self.sources.items():
                for file in self.get_source_files(source_path):
                    output_path = os.path.join(self.temp_path, source, file, quadkey)
                    file_df = self.read_df(output_path)
                    file_df[f"source_{source}"] = True
                    dfs.append(file_df)

            df = pd.concat(dfs)

            aggs = {
                "records": lambda x: x.sum(),
                "min_year": "min",
                "max_year": "max"
            }
            for source in self.sources:
                aggs[f"source_{source}"] = "any"

            df = df.groupby([f"h3_0{self.h3_resolution}", "species", "AphiaID"], dropna=False).agg(aggs).reset_index()

            df["min_year"] = df["min_year"].astype("Int64")
            df["max_year"] = df["max_year"].astype("Int64")
            df["AphiaID"] = df["AphiaID"].astype("Int32")

            # to geopandas

            df.set_index(f"h3_0{self.h3_resolution}", inplace=True)
            df = df.h3.h3_to_geo()
            df[f"h3_0{self.h3_resolution}"] = df.index

            # output

            output_file = os.path.join(self.output_path, quadkey)
            logger.info(f"Writing {len(df)} results to {output_file}")
            df.to_parquet(output_file, index=False)
