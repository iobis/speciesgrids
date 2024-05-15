from abc import ABC, abstractmethod
import geopandas
import pandas as pd
import h3pandas  # noqa: F401
import geohash2
from pyquadkey2.quadkey import QuadKey
from lib import row_to_quadkey


class Grid(ABC):

    @abstractmethod
    def __init__(self, resolution: int, quadkey_level: int):
        pass

    @abstractmethod
    def add_geometry(self, df: pd.DataFrame) -> geopandas.GeoDataFrame:
        pass

    @abstractmethod
    def summarize_occurrences(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class H3Grid(Grid):

    def __init__(self, resolution: int, quadkey_level: int):
        self.resolution = resolution
        self.quadkey_level = quadkey_level

    def add_geometry(self, df: pd.DataFrame) -> geopandas.GeoDataFrame:
        df.set_index("cell", inplace=True)
        df = df.h3.h3_to_geo()
        df["cell"] = df.index
        return df

    def summarize_occurrences(self, df: pd.DataFrame) -> pd.DataFrame:

        # calculate h3, drop coordinates

        df = df.h3.geo_to_h3(self.resolution, "decimalLatitude", "decimalLongitude", set_index=True)
        df["cell"] = df.index
        df.drop(["decimalLongitude", "decimalLatitude"], axis=1, inplace=True)

        # aggregate by h3

        df = df.groupby(["cell", "species", "AphiaID"], dropna=False).agg(
            records=("records", lambda x: x.sum()),
            min_year=("min_year", lambda x: x.min(skipna=True)),
            max_year=("max_year", lambda x: x.max(skipna=True))
        ).reset_index()

        # calculate center geometry and quadkey, drop geometry

        df.set_index("cell", inplace=True)
        df = df.h3.h3_to_geo()
        df["quadkey"] = df.apply(lambda row: row_to_quadkey(row, self.quadkey_level), axis=1)
        df = pd.DataFrame(df.drop(columns="geometry"))
        df.reset_index(inplace=True)

        return df


class GeohashGrid(Grid):

    def __init__(self, resolution: int, quadkey_level: int):
        self.resolution = resolution
        self.quadkey_level = quadkey_level

    def add_geometry(self, df: pd.DataFrame) -> geopandas.GeoDataFrame:
        df[["y", "x"]] = df["cell"].apply(lambda x: geohash2.decode_exactly(x)[0:2]).apply(pd.Series)
        df = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.x, df.y), crs="EPSG:4326")
        df.drop(["x", "y"], axis=1, inplace=True)
        return df

    def summarize_occurrences(self, df: pd.DataFrame) -> pd.DataFrame:

        # calculate geohash, drop coordinates

        df["cell"] = df.apply(lambda row: geohash2.encode(row["decimalLatitude"], row["decimalLongitude"], precision=self.resolution), axis=1)
        df.drop(["decimalLongitude", "decimalLatitude"], axis=1, inplace=True)

        # aggregate by geohash

        df = df.groupby(["cell", "species", "AphiaID"], dropna=False).agg(
            records=("records", lambda x: x.sum()),
            min_year=("min_year", lambda x: x.min(skipna=True)),
            max_year=("max_year", lambda x: x.max(skipna=True))
        ).reset_index()

        # calculate center geometry and quadkey, drop geometry

        df[["y", "x"]] = df["cell"].apply(lambda x: geohash2.decode_exactly(x)[0:2]).apply(pd.Series)
        df["quadkey"] = df.apply(lambda row: str(QuadKey.from_geo((row["y"], row["x"]), self.quadkey_level)), axis=1)
        df = pd.DataFrame(df.drop(columns=["x", "y"]))
        df.reset_index(inplace=True)

        return df
