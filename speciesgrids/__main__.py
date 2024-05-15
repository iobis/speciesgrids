import logging
from grids import GeohashGrid
from speciesgrids import DatasetBuilder


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def main():
    builder = DatasetBuilder(
        sources={
            "obis": "data/obis",
            "gbif": "data/gbif"
        },
        grid=GeohashGrid(4, 3),
        quadkey_level=3,
        output_path="geohash_4",
        worms_taxon_path="data/worms/WoRMS_OBIS/taxon.txt",
        worms_matching_path="data/worms/match-dataset-2011.tsv",
        worms_profile_path="data/worms/WoRMS_OBIS/speciesprofile.txt",
        worms_redlist_path="data/worms/redlist.parquet",
        worms_output_path="data/worms/worms_mapping.parquet",
        temp_path="temp_geohash"
    )
    builder.build(worms=True, index=True, merge=True)


if __name__ == "__main__":
    main()
