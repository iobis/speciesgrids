library(dplyr)
library(arrow)
library(sf)
library(ggplot2)
library(viridis)
library(scales)
library(gsl)

# Current speciesgrids product (single H3-7 GeoParquet; depth splits are no longer published).
ds <- open_dataset("../build/h3_7/data.parquet")
df <- ds %>% collect()

calc <- function(df, esn = 50) {
  t1 <- df %>%
    group_by(cell, species) %>%
    summarize(ni = sum(records), .groups = "drop")
  t2 <- t1 %>%
    group_by(cell) %>%
    mutate(n = sum(ni))
  t3 <- t2 %>%
    group_by(cell, species) %>%
    mutate(
      hi = -(ni/n*log(ni/n)), si = (ni/n)^2, qi = ni/n,
      esi = case_when(
        n-ni >= esn ~ 1-exp(lngamma(n-ni+1)+lngamma(n-esn+1)-lngamma(n-ni-esn+1)-lngamma(n+1)),
        n >= esn ~ 1
      )
    )
  t4 <- t3 %>%
    group_by(cell) %>%
    summarize(n = sum(ni), sp = n(), shannon = sum(hi), simpson = sum(si), maxp = max(qi), es = sum(esi), .groups = "drop")
  result <- t4 %>%
    mutate(hill_1 = exp(shannon), hill_2 = 1/simpson, hill_inf = 1/maxp)
  return(result)
}

df_calc <- calc(df) %>%
  rename(species = sp)

df_sf <- df_calc %>%
  mutate(geom = h3jsr::cell_to_polygon(cell)) %>%
  st_as_sf() %>%
  st_wrap_dateline()

# plot

sf_use_s2(FALSE)
ocean <- sf::read_sf("../ne_10m_ocean/ne_10m_ocean.shp") %>% st_make_valid()
df_sf <- st_filter(df_sf, ocean, .predicate = st_intersects)

ggplot() +
  geom_sf(data = df_sf, aes(fill = n), color = NA) +
  scale_fill_viridis(option = "inferno", na.value = "white", trans = "log10", labels = comma) +
  theme_void()

# output

write_sf(df_sf, "ospar.gpkg", driver = "GPKG")
