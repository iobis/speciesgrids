from pyquadkey2.quadkey import QuadKey
import itertools
import os
import logging


def get_quadkeys(level):
    """Get all quadkeys for a level."""

    quadkeys = list(itertools.chain(*[QuadKey(key).children(at_level=level) for key in ["0", "1", "2", "3"]]))
    return [str(quadkey) for quadkey in quadkeys]


def row_to_quadkey(row, level):
    """Get the quadkey for a geopandas row."""

    quadkey = QuadKey.from_geo((row["geometry"].y, row["geometry"].x), level)
    return quadkey.key


def clear_directory(path):
    logging.info(f"Clearing {path}")
    files = os.listdir(path)
    for file in files:
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
