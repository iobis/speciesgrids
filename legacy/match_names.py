import pyarrow.parquet as pq
import os
import sys
import duckdb
from duckdb import InvalidInputException
import logging
import pickle
import pyworms
from retry import retry
from multiprocessing import Pool


DATA_DIR = "data"
NAMES_OUTPUT_DIR = "temp_names"
MATCHES_OUTPUT_DIR = "temp_matches"


logger = logging.getLogger(__name__)


def export_names(dataset: str):

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

    with open(os.path.join(NAMES_OUTPUT_DIR, f"{dataset}.pickle"), "wb") as outfile:
        pickle.dump(names, outfile)


@retry(tries=10, delay=60)
def match_batch(batch):
    matches = pyworms.aphiaRecordsByMatchNames(batch)
    return batch


def match_names(dataset: str):

    with open(os.path.join(NAMES_OUTPUT_DIR, f"{dataset}.pickle"), "rb") as infile:
        names = list(pickle.load(infile))

    logger.info(f"Matching {len(names)} names for {dataset}")

    names_batches = [names[i:i + 50] for i in range(0, len(names), 50)]  
    
    for i, batch in enumerate(names_batches):
        logger.info(f"Matching batch {i + 1}/{len(names_batches)}")
        out_path = os.path.join(MATCHES_OUTPUT_DIR, dataset, f"{i}.pickle")

        if os.path.exists(out_path):
            continue

        matches = match_batch(batch)

        with open(out_path, "wb") as outfile:
            pickle.dump(matches, outfile)
    

if __name__ == "__main__":
    dataset = sys.argv[1]
    match_names(dataset)
