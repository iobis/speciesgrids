import tempfile
from speciesgrids.worms import WormsBuilder
from speciesgrids.index import Indexer
from speciesgrids.merge import Merger
from speciesgrids.lib import get_quadkeys
import os
from speciesgrids.grids import Grid


class DatasetBuilder(WormsBuilder, Merger, Indexer):
    """
    This class builds gridded datasets from species occurrence parquet files. Data sources
    can include OBIS and GBIF data, but for GBIF only species level output is supported.
    Supported grid systems include H3 and Geohash.

    Attributes:
        sources: parquet data source folders
        grid: grid system
        output_path: output path
        temp_path: temporary working directory path
        worms_taxon_path: WoRMS taxon file path
        worms_db_path: WoRMS sqlite file path
        worms_matching_path: WoRMS matching file path
        worms_profile_path: WoRMS profile file path
        worms_redlist_path: WoRMS red list file path
        worms_mapping_path: WoRMS mapping path
        worms_taxonomy_path: WoRMS taxonomy output path
        predicates: list of SQL predicates
        species: boolean indicating whether output should be restricted to species level
    """

    def __init__(self, sources: dict, grid: Grid, output_path: str = None, temp_path: str = None, worms_sqlite_path: str = None, worms_mapping_path: str = None, worms_taxonomy_path: str = None, worms_redlist_path: str = None, predicates: list[str] = [], species: bool = True):
        self.sources = sources
        self.grid = grid
        self.output_path = output_path
        self.temp_path = temp_path if temp_path else tempfile.TemporaryDirectory()
        self.worms_sqlite_path = worms_sqlite_path
        self.worms_mapping_path = worms_mapping_path
        self.worms_taxonomy_path = worms_taxonomy_path
        self.worms_redlist_path = worms_redlist_path
        self.quadkeys = get_quadkeys(self.grid.quadkey_level)
        self.predicates = predicates
        self.species = species

    def get_source_files(self, source_path):
        return [f for f in os.listdir(source_path) if not f.startswith(".")]

    def build(self, index=True, merge=True):
        """Build the dataset.

        Args:
            index: boolean indicating whether to build quadkey index
            merge: boolean indicating whether to merge indexed data
        """

        if index:
            self.index()
        if merge:
            self.merge()
