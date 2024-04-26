import pyarrow.parquet as pq
import os
import sys
import duckdb
from duckdb import InvalidInputException
import logging


DATA_DIR = "data"

logger = logging.getLogger(__name__)


def match_names(dataset: str):

    names = set()
    files = os.listdir(os.path.join("data", dataset))

    for file in files:
        logger.info(f"Reading names from {file}")
        path = os.path.join(DATA_DIR, dataset, file)
        try:
            res = duckdb.query(f"select species from read_parquet('{path}')").df()
            species = [sp for sp in list(res["species"]) if sp is not None]
            names.update(species)
        except InvalidInputException:
            logger.info(f"Skipping {file}")

    print(names)

if __name__ == "__main__":
    dataset = sys.argv[1]
    match_names(dataset)
