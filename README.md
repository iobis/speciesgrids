# speciesgrids

This Python package builds gridded datasets of WoRMS aligned marine species distributions as GeoParquet based on the OBIS and GBIF occurrence snapshots. The package currently supports Geohash and H3 grid output.

## Data usage
### Citing

When using this data product, please copy the citations provided below:

```
OBIS (2024). speciesgrids (version 0.1.0). https://github.com/iobis/speciesgrids
GBIF.org (1 May 2024) GBIF Occurrence Data https://doi.org/10.15468/dl.ubwn8z
OBIS (25 October 2023) OBIS Occurrence Snapshot. Ocean Biodiversity Information System. Intergovernmental Oceanographic Commission of UNESCO. https://obis.org.
World Register of Marine Species. Available from https://www.marinespecies.org at VLIZ. Accessed 2024-05-01. doi:10.14284/170.
IUCN. 2023. The IUCN Red List of Threatened Species. Version 2023-1. https://www.iucnredlist.org. Accessed on 13 May 2024.
Gearty W, Chamberlain S (2022). rredlist: IUCN Red List Client. R package version 0.7.1, https://CRAN.R-project.org/package=rredlist.
```

### Data access

A number of grids are available for download from S3:

```bash
aws s3 cp --recursive s3://obis-products/speciesgrids .
```

### Example: data exploration using geopandas

This example uses a local copy of the dataset to explore the distribution of a species.

```python
import geopandas

filters = [("AphiaID", "==", 141433)]

gdf = geopandas.read_parquet("../h3_7/", filters=filters)
gdf.explore(column="records", cmap="viridis", legend=True, tiles="CartoDB positron")
```

![screenshot](screenshot.png)

```python
gdf.set_index("h3_07", inplace=True)
gdf = gdf.h3.h3_to_geo_boundary()
gdf.explore(column="records", cmap="viridis", legend=True, tiles="CartoDB positron")
```

![screenshot](screenshot_grid.png)

### Example: regional species list in R

This example indexes a complex polygon and queries the GeoParquet dataset on AWS.

```r
library(readr)
library(h3jsr)
library(sf)
library(duckdb)
library(DBI)
library(dplyr)

sf_use_s2(FALSE)

# Read WKT from text file, convert to sf, and index to H3 resolution 7
# https://wktmap.com/?e6b28728

wkt <- read_file("wkt_21773.txt")
geom <- st_as_sfc(wkt, crs = 4326)
cells <- data.frame(cell = polygon_to_cells(geom, 7)[[1]])

# Set up duckdb connection and register cells table

con <- dbConnect(duckdb())
dbSendQuery(con, "install httpfs; load httpfs;")
duckdb_register(con, "cells", cells)

# Join cells list and gridded species dataset

species <- dbGetQuery(con, "
  select species, AphiaID
  from cells
  inner join read_parquet('s3://obis-products/speciesgrids/h3_7/*') h3 on cells.cell = h3.h3_07
  group by species, AphiaID
")

# Add WoRMS taxonomy

id_batches <- split(species$AphiaID, ceiling(seq_along(species$AphiaID) / 50))
taxa_batches <- purrr::map(id_batches, worrms::wm_record)

taxa <- bind_rows(taxa_batches) %>% 
  select(AphiaID, scientificname, phylum, class, order, family, genus, scientificName = scientificname)
```

### Notebooks

Other data usage examples are available as [notebooks](notebooks).

## For developers

### Data preparation

The following source datasets need to be prepared:

- OBIS occurrence snapshot
- GBIF occurrence snapshot
- WoRMS export (taxon and species profile tables)
- IUCN Red List export
- GBIF taxonomic backbone to WoRMS taxonomy from ChecklistBank

### Run

Adapt the file paths and grid configuration in `speciesgrids/__main__.py` and run:

```bash
python -m speciesgrids
```

### Upload to S3

```
aws s3 sync geohash_4 s3://obis-products/speciesgrids/geohash_4
aws s3 sync h3_7 s3://obis-products/speciesgrids/h3_7
```
