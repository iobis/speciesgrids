# This script builds datasets for three depth layers to be used for WOA3.

import logging
from speciesgrids.grids import GeohashGrid, H3Grid
from speciesgrids import DatasetBuilder
import pyarrow.parquet as pq
import os
import pyarrow.compute as pc
import pyarrow as pa
import shutil


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


date = "20240723"
resolution = 5

datasets = [
    [f"data/obis_{date}", f"output/h3_{resolution}_{date}_200", f"output/temp_h3_{resolution}_{date}_200", ["bathymetry <= 200"]],
    [f"data/obis_{date}", f"output/h3_{resolution}_{date}_200_1000", f"output/temp_h3_{resolution}_{date}_200_1000", ["bathymetry > 200", "bathymetry <= 1000"]],
    [f"data/obis_{date}", f"output/h3_{resolution}_{date}_1000", f"output/temp_h3_{resolution}_{date}_1000", ["bathymetry > 1000"]]
]

for dataset in datasets:

    logging.info(dataset)

    builder = DatasetBuilder(
        sources={
            "obis": dataset[0],
        },
        grid=H3Grid(resolution, 3),
        quadkey_level=3,
        output_path=dataset[1],
        worms_taxon_path="data/worms/WoRMS_OBIS/taxon.txt",
        worms_matching_path="data/worms/match-dataset-2011.tsv",
        worms_profile_path="data/worms/WoRMS_OBIS/speciesprofile.txt",
        worms_redlist_path=None,#"data/worms/redlist.parquet",
        worms_output_path="data/worms/worms_mapping.parquet",
        temp_path=dataset[2],
        predicates=dataset[3],
        species=False
    )
    builder.build(worms=False, index=True, merge=True)
