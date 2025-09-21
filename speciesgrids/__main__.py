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
        worms_sqlite_path="/Volumes/acasis/worms/worms_20250911.db",
        worms_mapping_path="data/worms/worms_mapping.parquet",
        worms_taxonomy_path="data/worms/worms_taxonomy.parquet",
        temp_path="temp_h3_7"
    )
    builder.build(
        index=True,
        merge=True
    )


if __name__ == "__main__":
    main()
