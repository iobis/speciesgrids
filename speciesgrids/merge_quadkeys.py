import sys
import os
import logging
import glob


logger = logging.getLogger(__name__)
TEMP_DIR = "output"
OUTPUT_DIR = "output"


def merge_quadkeys():

    # get relevant files within datasets and subsets

    datasets = [dataset for dataset in os.listdir(TEMP_DIR) if os.path.isdir(os.path.join(TEMP_DIR, dataset))]

    for dataset in datasets:

        files = glob.glob(os.path.join(dataset, "**/\\d+"), recursive=True)
        for file in files:
            print(file)
    # merge
    # output


if __name__ == "__main__":
    merge_quadkeys()
