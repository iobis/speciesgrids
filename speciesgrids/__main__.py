import tempfile
from worms import WormsBuilder
from index import Indexer
from merge import Merger
from lib import get_quadkeys
import logging
import os


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


class DatasetBuilder(WormsBuilder, Merger, Indexer):

    def __init__(self, sources: dict, aggregation: dict, quadkey_level: int = 3, output_path: str = None, temp_path: str = None, worms_taxon_path: str = None, worms_matching_path: str = None, worms_profile_path: str = None):
        self.sources = sources
        self.aggregation = aggregation
        self.quadkey_level = quadkey_level
        self.output_path = output_path
        self.temp_path = temp_path if temp_path else tempfile.TemporaryDirectory()
        self.worms_taxon_path = worms_taxon_path
        self.worms_matching_path = worms_matching_path
        self.worms_profile_path = worms_profile_path
        self.quadkeys = get_quadkeys(self.quadkey_level)

    def get_source_files(self, source_path):
        return [f for f in os.listdir(source_path) if not f.startswith(".")]

    def build(self, worms=True, index=True, merge=True):
        if worms:
            self.worms_to_parquet()
        if index:
            self.index()
        if merge:
            self.merge()


def main():
    builder = DatasetBuilder(
        sources={
            "obis": "data/obis",
            "gbif": "data/gbif"
        },
        aggregation={
            "type": "h3",
            "resolution": 7
        },
        quadkey_level=3,
        output_path="output",
        worms_taxon_path="data/worms/WoRMS_OBIS/taxon.txt",
        worms_matching_path="data/worms/match-dataset-2011.tsv",
        worms_profile_path="data/worms/WoRMS_OBIS/speciesprofile.txt",
        temp_path="temp"
    )
    print(builder.__class__.__bases__)
    builder.build(worms=False, index=True, merge=False)


if __name__ == "__main__":
    main()
