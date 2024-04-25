from speciesgrids.index_to_quadkey import index_to_quadkey
import logging
import os


logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


DATASETS = ["obis"]
QUADKEY_LEVEL = 3
H3_RESOLUTION = 7


for dataset in DATASETS:

    files = os.listdir(f"data/{dataset}")

    for file in files:
        path = os.path.join("data", dataset, file)
        index_to_quadkey(path, QUADKEY_LEVEL, H3_RESOLUTION)
