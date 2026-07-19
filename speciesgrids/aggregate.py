"""Per-source H3 aggregation via DuckDB."""

import logging
import os

from speciesgrids.db import connect
from speciesgrids.progress import run_busy, stage


logger = logging.getLogger(__name__)


def _fmt_bytes(n: int) -> str:
    for unit, div in (("GiB", 1024 ** 3), ("MiB", 1024 ** 2), ("KiB", 1024)):
        if n >= div:
            return f"{n / div:.1f} {unit}"
    return f"{n} B"


class Aggregator:
    """Aggregate OBIS parquet and GBIF occurrence cube to H3 cells."""

    def cache_gbif_cube(self, force: bool = False) -> str:
        """Convert GBIF cube TSV to zstd Parquet under cache_path; return parquet path."""
        cube_csv = self.sources.get("gbif")
        if cube_csv is None:
            raise ValueError("sources must include 'gbif' pointing at the occurrence cube TSV")
        os.makedirs(self.cache_path, exist_ok=True)
        out = os.path.join(self.cache_path, "gbif_cube.parquet")
        if os.path.isfile(out) and not force:
            logger.info("[dim]Using cached GBIF cube parquet %s[/dim]", out)
            return out

        input_size = os.path.getsize(cube_csv)
        with stage("Convert GBIF cube TSV → parquet"):
            logger.info(
                "Reading [bold]%s[/bold] (%s) → writing %s",
                cube_csv,
                _fmt_bytes(input_size),
                out,
            )

            def _convert():
                con = connect(os.path.join(self.temp_path, "duckdb"))
                con.execute(
                    f"""
                    copy (
                        select
                            species,
                            try_cast(decimallatitude as double) as decimalLatitude,
                            try_cast(decimallongitude as double) as decimalLongitude,
                            try_cast(minyear as bigint) as min_year,
                            try_cast(maxyear as bigint) as max_year,
                            try_cast(occurrences as bigint) as records
                        from read_csv(
                            '{cube_csv}',
                            delim='\t',
                            header=true,
                            auto_detect=true,
                            sample_size=-1,
                            parallel=true
                        )
                        where species is not null
                          and try_cast(decimallatitude as double) is not null
                          and try_cast(decimallongitude as double) is not null
                    ) to '{out}' (format parquet, compression zstd)
                    """
                )
                con.close()

            run_busy(
                f"Converting GBIF cube ({_fmt_bytes(input_size)} TSV → parquet)…",
                _convert,
                detail="This can take a long time; DuckDB will not report % complete for CSV scans.",
            )
            size_gb = os.path.getsize(out) / (1024 ** 3)
            logger.info("Wrote %s (%.2f GiB)", out, size_gb)
        return out

    def aggregate(self, force: bool = False):
        os.makedirs(self.temp_path, exist_ok=True)
        if "gbif" in self.sources:
            self.aggregate_gbif(force=force)
        if "obis" in self.sources:
            self.aggregate_obis(force=force)

    def aggregate_gbif(self, force: bool = False):
        out = os.path.join(self.temp_path, "gbif_h3.parquet")
        if os.path.isfile(out) and not force:
            logger.info("[dim]Using cached GBIF H3 aggregate %s[/dim]", out)
            return out

        cube = self.cache_gbif_cube(force=force)
        match_path = self.taxon.gbif_species_aphia_path
        resolution = self.grid.resolution

        with stage(f"Aggregate GBIF cube to H3 resolution {resolution}"):
            def _agg():
                con = connect(os.path.join(self.temp_path, "duckdb"))
                con.execute(
                    f"""
                    copy (
                        select
                            h3_latlng_to_cell_string(c.decimalLatitude, c.decimalLongitude, {resolution}) as cell,
                            m.scientificName as species,
                            m.AphiaID::bigint as AphiaID,
                            sum(c.records)::bigint as records,
                            min(c.min_year)::bigint as min_year,
                            max(c.max_year)::bigint as max_year
                        from read_parquet('{cube}') c
                        inner join read_parquet('{match_path}') m on c.species = m.species
                        where m.AphiaID is not null
                        group by 1, 2, 3
                    ) to '{out}' (format parquet, compression zstd)
                    """
                )
                con.close()

            run_busy(
                f"Aggregating GBIF → H3 r{resolution}…",
                _agg,
                detail=f"Joining {cube} with {match_path}",
            )
            con = connect(os.path.join(self.temp_path, "duckdb"))
            n = con.execute(f"select count(*) from read_parquet('{out}')").fetchone()[0]
            con.close()
            logger.info("Wrote %s H3×species rows → %s", f"{n:,}", out)
        return out

    def aggregate_obis(self, force: bool = False):
        out = os.path.join(self.temp_path, "obis_h3.parquet")
        if os.path.isfile(out) and not force:
            logger.info("[dim]Using cached OBIS H3 aggregate %s[/dim]", out)
            return out

        obis_path = self.sources["obis"]
        remap = self.taxon.aphia_remap_path
        resolution = self.grid.resolution
        predicates = ""
        if self.predicates:
            predicates = " and " + " and ".join(self.predicates)

        species_filter = ""
        if self.species_only:
            species_filter = "and interpreted.species is not null"

        parquet_glob = os.path.join(obis_path, "*.parquet")
        with stage(f"Aggregate OBIS to H3 resolution {resolution}"):
            def _agg():
                con = connect(os.path.join(self.temp_path, "duckdb"))
                con.execute(
                    f"""
                    copy (
                        select
                            h3_latlng_to_cell_string(
                                interpreted.decimalLatitude,
                                interpreted.decimalLongitude,
                                {resolution}
                            ) as cell,
                            r.species,
                            r.AphiaID::bigint as AphiaID,
                            count(*)::bigint as records,
                            min(interpreted.date_year)::bigint as min_year,
                            max(interpreted.date_year)::bigint as max_year
                        from read_parquet('{parquet_glob}', union_by_name=true) o
                        inner join read_parquet('{remap}') r
                            on o.interpreted.aphiaid = r.input_AphiaID
                        where interpreted.decimalLongitude is not null
                          and interpreted.decimalLatitude is not null
                          and absence is false
                          {species_filter}
                          {predicates}
                        group by 1, 2, 3
                    ) to '{out}' (format parquet, compression zstd)
                    """
                )
                con.close()

            run_busy(
                f"Aggregating OBIS → H3 r{resolution}…",
                _agg,
                detail=f"Scanning {parquet_glob} (remapping AphiaIDs via {remap})",
            )
            con = connect(os.path.join(self.temp_path, "duckdb"))
            n = con.execute(f"select count(*) from read_parquet('{out}')").fetchone()[0]
            con.close()
            logger.info("Wrote %s H3×species rows → %s", f"{n:,}", out)
        return out
