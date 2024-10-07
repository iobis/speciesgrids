# This script builds a dataset for MWHS species lists

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


builder = DatasetBuilder(
    sources={
        "obis": f"data/obis_{date}",
        "gbif": "/Volumes/Samsung T7/gbif"
    },
    grid=H3Grid(resolution, 3),
    quadkey_level=3,
    output_path=f"output/h3_{resolution}_obisgbif_{date}",
    worms_taxon_path="data/worms/WoRMS_OBIS/taxon.txt",
    worms_matching_path="data/worms/match-dataset-2011.tsv",
    worms_profile_path="data/worms/WoRMS_OBIS/speciesprofile.txt",
    worms_redlist_path=None,
    worms_output_path="data/worms/worms_mapping.parquet",
    temp_path=f"output/temp_h3_{resolution}_{date}",
    species=True
)
builder.build(worms=False, index=False, merge=True)
