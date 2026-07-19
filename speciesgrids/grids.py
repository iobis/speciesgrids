from abc import ABC, abstractmethod


class Grid(ABC):

    @abstractmethod
    def __init__(self, resolution: int):
        pass


class H3Grid(Grid):

    def __init__(self, resolution: int):
        self.resolution = resolution
        self.system = "h3"


class GeohashGrid(Grid):
    """Legacy grid type. The DuckDB build path only supports H3Grid."""

    def __init__(self, resolution: int):
        self.resolution = resolution
        self.system = "geohash"
