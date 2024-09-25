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
        worms_matching_path: WoRMS matching file path
        worms_profile_path: WoRMS profile file path
        worms_redlist_path: WoRMS red list file path
        worms_mapping_output_path: WoRMS mapping output path
        worms_taxonomy_output_path: WoRMS taxonomy output path
        predicates: list of SQL predicates
        species: boolean indicating whether outpuit should be restricted to species level
    """

    def __init__(self, sources: dict, grid: Grid, output_path: str = None, temp_path: str = None, worms_taxon_path: str = None, worms_matching_path: str = None, worms_profile_path: str = None, worms_redlist_path: str = None, worms_mapping_output_path: str = None, worms_taxonomy_output_path: str = None, predicates: list[str] = [], species: bool = True):
        self.sources = sources
        self.grid = grid
        self.output_path = output_path
        self.temp_path = temp_path if temp_path else tempfile.TemporaryDirectory()
        self.worms_taxon_path = worms_taxon_path
        self.worms_matching_path = worms_matching_path
        self.worms_profile_path = worms_profile_path
        self.worms_redlist_path = worms_redlist_path
        self.worms_mapping_output_path = worms_mapping_output_path
        self.worms_taxonomy_output_path = worms_taxonomy_output_path
        self.quadkeys = get_quadkeys(self.grid.quadkey_level)
        self.predicates = predicates
        self.species = species

    def get_source_files(self, source_path):
        return [f for f in os.listdir(source_path) if not f.startswith(".")]

    def build(self, worms=False, index=True, merge=True):
        """Build the dataset.

        Args:
            worms: boolean indicating whether to build the mapping from GBIF taxon ID to marine WoRMS accepted AphiaID
            index: boolean indicating whether to build quadkey index
            merge: boolean indicating whether to merge indexed data
        """

        if worms:
            self.worms_to_parquet()
        if index:
            self.index()
        if merge:
            self.merge()
