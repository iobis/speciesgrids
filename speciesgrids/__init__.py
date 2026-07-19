import logging
import os

from speciesgrids.aggregate import Aggregator
from speciesgrids.grids import Grid, H3Grid
from speciesgrids.merge import Merger
from speciesgrids.progress import stage
from speciesgrids.taxon import TaxonMatcher


logger = logging.getLogger(__name__)


class DatasetBuilder(Aggregator, Merger):
    """Build an H3-gridded GeoParquet product from OBIS and/or a GBIF occurrence cube.

    All pipeline writes go under a single ``build_dir``::

        build/
          work/                 # intermediates (cube cache, taxon maps, H3 aggs, DuckDB spill)
          h3_7/data.parquet     # final product (name from grid resolution)
    """

    def __init__(
        self,
        sources: dict,
        grid: Grid = None,
        build_dir: str = "build",
        worms_sqlite_path: str = None,
        worms_redlist_path: str = None,
        predicates: list[str] = None,
        species_only: bool = True,
    ):
        if grid is None:
            grid = H3Grid(7)
        if not isinstance(grid, H3Grid):
            raise ValueError("Only H3Grid is supported by the DuckDB build path")
        if worms_sqlite_path is None:
            raise ValueError("worms_sqlite_path is required")

        self.sources = sources
        self.grid = grid
        self.build_dir = build_dir
        # One work tree for every intermediate; product sits beside it.
        self.work_path = os.path.join(build_dir, "work")
        self.cache_path = self.work_path
        self.temp_path = self.work_path
        self.output_path = os.path.join(build_dir, f"h3_{grid.resolution}")
        self.worms_sqlite_path = worms_sqlite_path
        self.worms_redlist_path = worms_redlist_path
        self.predicates = predicates or []
        self.species_only = species_only
        self.taxon = TaxonMatcher(worms_sqlite_path, self.work_path)

    def build(self, prepare=True, aggregate=True, merge=True, force=False):
        """Build the dataset.

        Args:
            prepare: export taxonomy / match GBIF names (uses cache when present)
            aggregate: aggregate OBIS and GBIF to H3
            merge: merge sources and write GeoParquet
            force: recompute cached artefacts
        """
        os.makedirs(self.work_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        logger.info(
            "Build dir [bold]%s[/bold] — work: %s — product: %s",
            self.build_dir,
            self.work_path,
            self.output_path,
        )

        with stage("Build speciesgrids dataset"):
            cube_parquet = None
            needs_cube = "gbif" in self.sources and (prepare or aggregate)
            if needs_cube:
                # Never force-rebuild the cube unless prepare/aggregate also run;
                # merge-only rebuilds should reuse build/work/gbif_cube.parquet.
                cube_parquet = self.cache_gbif_cube(force=force and (prepare or aggregate))

            if prepare:
                with stage("Prepare taxonomy artefacts"):
                    self.taxon.prepare(gbif_cube_parquet=cube_parquet, force=force)
            if aggregate:
                with stage("Aggregate sources to H3"):
                    self.aggregate(force=force)
            if merge:
                with stage("Merge and write output"):
                    self.merge(force=force)
