from pyquadkey2.quadkey import QuadKey
import itertools


def get_quadkeys(level):
    """Get all quadkeys for a level."""

    quadkeys = list(itertools.chain(*[QuadKey(key).children(at_level=level) for key in ["0", "1", "2", "3"]]))
    return [str(quadkey) for quadkey in quadkeys]


def row_to_quadkey(row, level):
    """Get the quadkey for a geopandas row."""

    quadkey = QuadKey.from_geo((row["geometry"].y, row["geometry"].x), level)
    return quadkey.key
