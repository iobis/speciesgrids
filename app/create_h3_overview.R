library(duckdb)
library(arrow)

con <- dbConnect(duckdb())

dbSendQuery(con, "INSTALL httpfs; LOAD httpfs;")
dbSendQuery(con, "INSTALL h3 FROM community; LOAD h3;")

S3 <- "read_parquet('s3://obis-products/speciesgrids/h3_7/*')"

# ── Overview map (H3 resolution 4) ───────────────────────────────────────────
overview <- dbGetQuery(con, sprintf("
  SELECT h3_cell_to_parent(cell, 4) AS cell,
         COUNT(DISTINCT species)::INTEGER AS total_species
  FROM %s
  GROUP BY h3_cell_to_parent(cell, 4)", S3))

write_ipc_file(overview, "app/static/overview_h3_4.arrow", compression = "uncompressed")

# ── Explore: Rare species (< 5 total records) ─────────────────────────────────
rare_species <- dbGetQuery(con, sprintf("
  SELECT species, AphiaID,
         SUM(records)::INTEGER  AS total_records,
         COUNT(*)::INTEGER      AS total_cells,
         MAX(max_year)::INTEGER AS last_seen
  FROM %s
  GROUP BY species, AphiaID
  HAVING SUM(records) < 5
  ORDER BY total_records ASC, species ASC", S3))

write_ipc_file(rare_species, "app/static/rare_species.arrow", compression = "uncompressed")

# ── Explore: Lost species (last seen <= 1965) ─────────────────────────────────
lost_species <- dbGetQuery(con, sprintf("
  SELECT species, AphiaID,
         MAX(max_year)::INTEGER AS last_seen,
         SUM(records)::INTEGER  AS total_records,
         COUNT(*)::INTEGER      AS total_cells
  FROM %s
  GROUP BY species, AphiaID
  HAVING MAX(max_year) <= 1965
  ORDER BY last_seen ASC, total_records DESC", S3))

write_ipc_file(lost_species, "app/static/lost_species.arrow", compression = "uncompressed")

# ── Explore: Blue whale cells ─────────────────────────────────────────────────
blue_whale <- dbGetQuery(con, sprintf("
  SELECT cell, records AS record_count, min_year, max_year
  FROM %s
  WHERE species = 'Balaenoptera musculus'
  ORDER BY records DESC", S3))

write_ipc_file(blue_whale, "app/static/blue_whale.arrow", compression = "uncompressed")

# ── Explore: Top 100 places (H3 res-4 by records) ────────────────────────────
top_places <- dbGetQuery(con, sprintf("
  SELECT h3_cell_to_parent(cell, 4)  AS cell_r4,
         SUM(records)::BIGINT         AS total_records,
         COUNT(DISTINCT species)::INT AS total_species
  FROM %s
  GROUP BY cell_r4
  ORDER BY total_records DESC
  LIMIT 100", S3))

write_ipc_file(top_places, "app/static/top_places.arrow", compression = "uncompressed")

# ── Gadus morhua records (Atlantic cod) ────────────────────────────
gadus <- dbGetQuery(con, sprintf("
  SELECT cell, records
  FROM %s
  WHERE species = 'Gadus morhua'", S3))

write_ipc_file(gadus, "app/static/gadus.arrow", compression = "uncompressed")


dbDisconnect(con)
message("Done. Files written to app/static/")
