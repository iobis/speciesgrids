from speciesgrids import DatasetBuilder
from speciesgrids.grids import H3Grid
from speciesgrids.progress import configure_logging


def main():
    configure_logging()
    builder = DatasetBuilder(
        sources={
            "obis": "/Volumes/acasis/occurrence",
            "gbif": "/Volumes/acasis/gbif/0004795-260715120105164.csv",
        },
        grid=H3Grid(7),
        build_dir="build",
        worms_sqlite_path="/Volumes/acasis/worms/worms_draft_20260522.db",
    )
    builder.build(prepare=True, aggregate=True, merge=True)


if __name__ == "__main__":
    main()
