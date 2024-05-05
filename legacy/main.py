import logging
import os
from speciesgrids.index_to_quadkey import index_to_quadkey
from speciesgrids.worms_to_parquet import worms_to_parquet
from speciesgrids.merge_quadkeys import merge_quadkeys


logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


SOURCES = ["obis", "gbif"]
QUADKEY_LEVEL = 3
H3_RESOLUTION = 7


# worms_to_parquet()

# for dataset in SOURCES:
#     files = os.listdir(f"data/{dataset}")
#     for file in [f for f in files if not f.startswith(".")]:
#         path = os.path.join("data", dataset, file)
#         index_to_quadkey(path, QUADKEY_LEVEL, H3_RESOLUTION, overwrite=True)

# merge_quadkeys(H3_RESOLUTION, SOURCES)
