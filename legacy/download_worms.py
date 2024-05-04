import requests
import logging


logger = logging.getLogger(__name__)


def download_worms():

    offset = 0
    limit = 1000
    url = f"https://api.gbif.org/v1/species?datasetKey=2d59e5db-57ad-41ff-97d6-11f5fb264527&limit={limit}&offset={offset}"

    results = []

    while True:
        logger.info(f"Fetching offset {offset}")
        res = requests.get(url) 
        result = res.json()
        species = [r for r in result["results"] if "rank" in r and r["rank"] == "SPECIES"]
        results.extend(species)
        if result["endOfRecords"]:
            break
        offset += limit
