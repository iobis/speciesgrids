import logging
from grids import GeohashGrid, H3Grid
from speciesgrids import DatasetBuilder


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def main():
    builder = DatasetBuilder(
        sources={
            "obis": "/Volumes/acasis/occurrence",
            "gbif": "/Volumes/acasis/gbif"
        },
        grid=H3Grid(7, 3),
        output_path="h3_7",
        worms_matching_path="data/worms/match-dataset-2011.tsv",
        worms_db_path="data/worms/worms.db",
        worms_redlist_path="data/worms/redlist.parquet",
        worms_mapping_output_path="data/worms/worms_mapping.parquet",
        worms_taxonomy_output_path="data/worms/worms_taxonomy.parquet",
        temp_path="temp_h3_7"
    )
    builder.build(
        worms=True,  # TODO: use WoRMS straight from sqlite?
        index=True,
        merge=True
    )


if __name__ == "__main__":
    main()
