import tempfile
from worms import WormsBuilder
from index import Indexer
from merge import Merger
from lib import get_quadkeys
import os
from grids import Grid


class DatasetBuilder(WormsBuilder, Merger, Indexer):

    def __init__(self, sources: dict, grid: Grid, quadkey_level: int = 3, output_path: str = None, temp_path: str = None, worms_taxon_path: str = None, worms_matching_path: str = None, worms_profile_path: str = None, worms_redlist_path: str = None, worms_output_path: str = None):
        self.sources = sources
        self.grid = grid
        self.quadkey_level = quadkey_level
        self.output_path = output_path
        self.temp_path = temp_path if temp_path else tempfile.TemporaryDirectory()
        self.worms_taxon_path = worms_taxon_path
        self.worms_matching_path = worms_matching_path
        self.worms_profile_path = worms_profile_path
        self.worms_redlist_path = worms_redlist_path
        self.worms_output_path = worms_output_path
        self.quadkeys = get_quadkeys(self.quadkey_level)

    def get_source_files(self, source_path):
        return [f for f in os.listdir(source_path) if not f.startswith(".")]

    def build(self, worms=False, index=True, merge=True):
        if worms:
            self.worms_to_parquet()
        if index:
            self.index()
        if merge:
            self.merge()
