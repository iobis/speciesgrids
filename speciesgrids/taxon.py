"""WoRMS SQLite taxonomy export and GBIF scientific-name matching."""

import logging
import os
import sqlite3

import duckdb
import pandas as pd

from speciesgrids.progress import progress_bar, run_busy, stage


logger = logging.getLogger(__name__)

# Marine/brackish species usable as output taxa:
# - accepted (aphiaid = valid_aphiaid), or
# - valid_aphiaid IS NULL (e.g. alternative representation like Calanus helgolandicus)
_USABLE_MARINE_SPECIES_SQL = """
    (
        aphiaid = valid_aphiaid
        or valid_aphiaid is null
    )
    and json_extract(record, '$.rank') = 'Species'
    and (
        json_extract(record, '$.isMarine') = 1
        or json_extract(record, '$.isBrackish') = 1
    )
"""


class TaxonMatcher:
    """Export WoRMS taxonomy artefacts and match GBIF species names."""

    def __init__(self, worms_sqlite_path: str, cache_path: str):
        self.worms_sqlite_path = worms_sqlite_path
        self.cache_path = cache_path
        self.taxonomy_path = os.path.join(cache_path, "worms_taxonomy.parquet")
        self.aphia_remap_path = os.path.join(cache_path, "aphia_remap.parquet")
        self.gbif_species_aphia_path = os.path.join(cache_path, "gbif_species_aphia.parquet")

    def prepare(self, gbif_cube_parquet: str | None = None, force: bool = False):
        os.makedirs(self.cache_path, exist_ok=True)
        if force or not os.path.isfile(self.taxonomy_path):
            self.export_taxonomy()
        else:
            logger.info("[dim]Using cached taxonomy %s[/dim]", self.taxonomy_path)
        if force or not os.path.isfile(self.aphia_remap_path):
            self.export_aphia_remap()
        else:
            logger.info("[dim]Using cached AphiaID remap %s[/dim]", self.aphia_remap_path)
        if gbif_cube_parquet is not None:
            if force or not os.path.isfile(self.gbif_species_aphia_path):
                self.match_gbif_cube_names(gbif_cube_parquet)
            else:
                logger.info("[dim]Using cached GBIF name map %s[/dim]", self.gbif_species_aphia_path)

    def export_taxonomy(self):
        with stage("Export WoRMS taxonomy"):
            con = sqlite3.connect(self.worms_sqlite_path)
            df = pd.read_sql_query(
                f"""
                select
                    cast(aphiaid as integer) as AphiaID,
                    json_extract(record, '$.kingdom') as kingdom,
                    json_extract(record, '$.phylum') as phylum,
                    json_extract(record, '$.class') as class,
                    json_extract(record, '$.order') as "order",
                    json_extract(record, '$.family') as family,
                    json_extract(record, '$.genus') as genus,
                    coalesce(
                        nullif(json_extract(record, '$.scientificname'), ''),
                        canonical
                    ) as species
                from parsed
                where {_USABLE_MARINE_SPECIES_SQL}
                order by aphiaid
                """,
                con,
            )
            con.close()
            for col in ["kingdom", "phylum", "class", "order", "family", "genus", "species"]:
                df[col] = df[col].astype("string")
            df.to_parquet(self.taxonomy_path, index=False)
            logger.info("Wrote %s species → %s", f"{len(df):,}", self.taxonomy_path)

    def export_aphia_remap(self):
        """Map any AphiaID to a usable marine/brackish species AphiaID + name.

        Synonyms resolve via valid_aphiaid. When valid_aphiaid is NULL, the row's
        own aphiaid is used (covers alternative representations, etc.).
        """
        with stage("Export AphiaID → species remap"):
            con = sqlite3.connect(self.worms_sqlite_path)
            df = pd.read_sql_query(
                """
                select
                    cast(p.aphiaid as integer) as input_AphiaID,
                    cast(a.aphiaid as integer) as AphiaID,
                    coalesce(
                        nullif(json_extract(a.record, '$.scientificname'), ''),
                        a.canonical
                    ) as species
                from parsed p
                inner join parsed a
                    on a.aphiaid = coalesce(p.valid_aphiaid, p.aphiaid)
                where (
                    a.aphiaid = a.valid_aphiaid
                    or a.valid_aphiaid is null
                  )
                  and json_extract(a.record, '$.rank') = 'Species'
                  and (
                      json_extract(a.record, '$.isMarine') = 1
                      or json_extract(a.record, '$.isBrackish') = 1
                  )
                """,
                con,
            )
            con.close()
            df["species"] = df["species"].astype("string")
            df.to_parquet(self.aphia_remap_path, index=False)
            logger.info("Wrote %s remap rows → %s", f"{len(df):,}", self.aphia_remap_path)

    def match_names(self, names: list[str]) -> pd.DataFrame:
        """Match scientific names to marine/brackish species AphiaIDs.

        Rules (aligned with obis-qc match_with_sqlite):
        - Lookup by canonical name equality.
        - Accept if exactly one row, or multiple rows resolving to one ID.
        - Resolve to coalesce(valid_aphiaid, aphiaid); keep marine/brackish species
          that are accepted or have null valid_aphiaid.
        """
        names = [n for n in names if n is not None and str(n).strip() != ""]
        unique_names = sorted(set(names))
        if not unique_names:
            return pd.DataFrame(columns=["species", "AphiaID", "scientificName"])

        con = sqlite3.connect(self.worms_sqlite_path)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        with stage("Load usable marine species from SQLite"):
            cur.execute(
                f"""
                select cast(aphiaid as integer) as aphiaid,
                       coalesce(nullif(json_extract(record, '$.scientificname'), ''), canonical) as scientificname
                from parsed
                where {_USABLE_MARINE_SPECIES_SQL}
                """
            )
            accepted = {row["aphiaid"]: row["scientificname"] for row in cur.fetchall()}
            logger.info("Loaded %s usable marine/brackish species", f"{len(accepted):,}")

        batch_size = 500
        canonical_map: dict[str, list[dict]] = {}
        n_batches = (len(unique_names) + batch_size - 1) // batch_size
        with progress_bar() as progress:
            task = progress.add_task(
                f"Match {len(unique_names):,} names against WoRMS",
                total=n_batches,
            )
            for i in range(0, len(unique_names), batch_size):
                batch = unique_names[i : i + batch_size]
                placeholders = ",".join("?" * len(batch))
                cur.execute(
                    f"""
                    select canonical, aphiaid, valid_aphiaid
                    from parsed
                    where canonical in ({placeholders})
                    """,
                    batch,
                )
                for row in cur.fetchall():
                    canonical = row["canonical"]
                    canonical_map.setdefault(canonical, []).append(
                        {
                            "aphiaid": row["aphiaid"],
                            "valid_aphiaid": row["valid_aphiaid"],
                        }
                    )
                progress.advance(task)
        con.close()

        def _resolved_id(hit: dict) -> int | None:
            vid = hit["valid_aphiaid"]
            if vid is not None:
                return int(vid)
            if hit["aphiaid"] is not None:
                return int(hit["aphiaid"])
            return None

        rows = []
        matched = 0
        for name in unique_names:
            hits = canonical_map.get(name, [])
            if not hits:
                continue
            if len(hits) == 1:
                resolved_id = _resolved_id(hits[0])
            else:
                resolved_ids = {_resolved_id(h) for h in hits}
                resolved_ids.discard(None)
                if len(resolved_ids) != 1:
                    continue
                resolved_id = next(iter(resolved_ids))
            if resolved_id is None or resolved_id not in accepted:
                continue
            rows.append(
                {
                    "species": name,
                    "AphiaID": int(resolved_id),
                    "scientificName": accepted[resolved_id],
                }
            )
            matched += 1

        logger.info(
            "Matched [bold]%s[/bold] / %s names to usable marine species",
            f"{matched:,}",
            f"{len(unique_names):,}",
        )
        return pd.DataFrame(rows, columns=["species", "AphiaID", "scientificName"])

    def match_gbif_cube_names(self, gbif_cube_parquet: str):
        with stage("Extract distinct GBIF species names"):
            names: list[str] = []

            def _extract():
                con = duckdb.connect()
                con.execute("set enable_progress_bar=false")
                rows = con.execute(
                    f"""
                    select distinct species
                    from read_parquet('{gbif_cube_parquet}')
                    where species is not null and species != ''
                    """
                ).fetchall()
                con.close()
                names.extend(row[0] for row in rows)

            run_busy(
                "Scanning cube for distinct species names…",
                _extract,
                detail=gbif_cube_parquet,
            )
            logger.info("Found %s distinct species names", f"{len(names):,}")
        df = self.match_names(names)
        with stage("Write GBIF species → AphiaID map"):
            df.to_parquet(self.gbif_species_aphia_path, index=False)
            logger.info("Wrote %s → %s", f"{len(df):,}", self.gbif_species_aphia_path)
