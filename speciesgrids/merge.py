"""Merge per-source H3 aggregates and write GeoParquet output.

The final file is written with GeoPandas (as in earlier speciesgrids releases) so
column dtypes, pandas metadata, and GeoArrow CRS match the historical product.
"""

import logging
import os
import shutil

import h3pandas  # noqa: F401
import pandas as pd

from speciesgrids.db import connect
from speciesgrids.progress import run_busy, stage


logger = logging.getLogger(__name__)

# Column order matches the historical H3 GeoParquet product.
_BASE_COLUMNS = [
    "species",
    "AphiaID",
    "records",
    "min_year",
    "max_year",
    "source_obis",
    "source_gbif",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
]


class Merger:

    def merge(self, force: bool = False):
        output_file = os.path.join(self.output_path, "data.parquet")
        if os.path.isfile(output_file) and not force:
            logger.info("[dim]Output already exists at %s; skip merge (force=False)[/dim]", output_file)
            return

        os.makedirs(self.output_path, exist_ok=True)
        for name in os.listdir(self.output_path):
            path = os.path.join(self.output_path, name)
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)

        parts = []
        if "obis" in self.sources:
            parts.append(("obis", os.path.join(self.temp_path, "obis_h3.parquet")))
        if "gbif" in self.sources:
            parts.append(("gbif", os.path.join(self.temp_path, "gbif_h3.parquet")))

        missing = [p for _, p in parts if not os.path.isfile(p)]
        if missing:
            raise FileNotFoundError(f"Missing aggregate parquet(s): {missing}")

        unions = []
        for source, path in parts:
            unions.append(
                f"""
                select
                    cell,
                    species,
                    AphiaID,
                    records,
                    min_year,
                    max_year,
                    {'true' if source == 'obis' else 'false'} as source_obis,
                    {'true' if source == 'gbif' else 'false'} as source_gbif
                from read_parquet('{path}')
                """
            )

        union_sql = " union all ".join(unions)
        taxonomy = self.taxon.taxonomy_path
        redlist_join = ""
        redlist_select = ""
        include_category = self.worms_redlist_path is not None
        if include_category:
            redlist_select = "red.category,"
            redlist_join = f"""
                left join read_parquet('{self.worms_redlist_path}') red
                    on red.species = m.species
            """

        merged_path = os.path.join(self.temp_path, "merged.parquet")

        with stage(f"Merge sources and write GeoParquet → {output_file}"):
            def _merge_tables():
                con = connect(os.path.join(self.temp_path, "duckdb"))
                con.execute(
                    f"""
                    copy (
                        with merged as (
                            select
                                cell,
                                species,
                                AphiaID,
                                sum(records)::bigint as records,
                                min(min_year)::bigint as min_year,
                                max(max_year)::bigint as max_year,
                                bool_or(source_obis) as source_obis,
                                bool_or(source_gbif) as source_gbif
                            from ({union_sql})
                            group by cell, species, AphiaID
                        )
                        select
                            m.species,
                            m.AphiaID::integer as AphiaID,
                            m.records,
                            m.min_year,
                            m.max_year,
                            m.source_obis,
                            m.source_gbif,
                            t.kingdom,
                            t.phylum,
                            t.class,
                            t."order",
                            t.family,
                            t.genus,
                            {redlist_select}
                            m.cell
                        from merged m
                        left join read_parquet('{taxonomy}') t on m.AphiaID = t.AphiaID
                        {redlist_join}
                    ) to '{merged_path}' (format parquet, compression zstd)
                    """
                )
                con.close()

            run_busy("Merging OBIS + GBIF tables…", _merge_tables)

            def _write_geoparquet():
                df = pd.read_parquet(merged_path)

                # Match historical pandas dtypes / parquet metadata
                df["min_year"] = df["min_year"].astype("Int64")
                df["max_year"] = df["max_year"].astype("Int64")
                df["AphiaID"] = df["AphiaID"].astype("Int32")
                for col in ["kingdom", "phylum", "class", "order", "family", "genus", "species", "cell"]:
                    df[col] = pd.Series(df[col], dtype="string")
                if include_category:
                    df["category"] = pd.Series(df["category"], dtype="string")

                df = df.set_index("cell")
                gdf = df.h3.h3_to_geo()
                gdf["cell"] = gdf.index
                gdf = gdf.set_crs("EPSG:4326")

                columns = list(_BASE_COLUMNS)
                if include_category:
                    columns.append("category")
                columns.extend(["geometry", "cell"])
                gdf[columns].to_parquet(output_file, index=False)

            run_busy(
                "Adding H3 centroids and writing GeoPandas GeoParquet…",
                _write_geoparquet,
                detail="Uses the same GeoPandas/h3pandas writer as previous releases (EPSG:4326).",
            )

            if os.path.isfile(merged_path):
                os.remove(merged_path)

            n = connect(os.path.join(self.temp_path, "duckdb")).execute(
                f"select count(*) from read_parquet('{output_file}')"
            ).fetchone()[0]
            size_mb = os.path.getsize(output_file) / (1024 ** 2)
            logger.info("Wrote %s rows (%.1f MiB) → %s", f"{n:,}", size_mb, output_file)
