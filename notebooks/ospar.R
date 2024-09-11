library(dplyr)
library(arrow)
library(sfarrow)
library(sf)
library(ggplot2)
library(viridis)
library(scales)
library(gsl)

ds_all <- open_dataset("../h3_4_2022_all")
ds_shallow <- open_dataset("../h3_4_2022_shallow")
ds_deep <- open_dataset("../h3_4_2022_deep")
df_all <- ds_all %>%
  collect()
df_shallow <- ds_shallow %>%
  collect()
df_deep <- ds_deep %>%
  collect()

calc <- function(df, esn = 50) {
  t1 <- df %>%
    group_by(cell, species) %>%
    summarize(ni = sum(records))
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
    summarize(n = sum(ni), sp = n(), shannon = sum(hi), simpson = sum(si), maxp = max(qi), es = sum(esi))
  result <- t4 %>%
    mutate(hill_1 = exp(shannon), hill_2 = 1/simpson, hill_inf = 1/maxp)
  return(result)
}

df_all_calc <- calc(df_all)
df_shallow_calc <- calc(df_shallow)
df_deep_calc <- calc(df_deep)

df_shallow_calc <- df_shallow_calc %>%
  rename(c(n_shallow = n, species_shallow = sp, shannon_shallow = shannon, simpson_shallow = simpson, maxp_shallow = maxp, es_shallow = es, hill_1_shallow = hill_1, hill_2_shallow = hill_2, hill_inf_shallow = hill_inf))
df_deep_calc <- df_deep_calc %>%
  rename(c(n_deep = n, species_deep = sp, shannon_deep = shannon, simpson_deep = simpson, maxp_deep = maxp, es_deep = es, hill_1_deep = hill_1, hill_2_deep = hill_2, hill_inf_deep = hill_inf))
df_all_calc <- df_all_calc %>%
  rename(c(species = sp))

df <- df_all_calc %>%
  left_join(df_shallow_calc, by = "cell") %>%
  left_join(df_deep_calc, by = "cell")

df_sf <- df %>% 
  mutate(geom = h3jsr::cell_to_polygon(cell)) %>%
  st_as_sf() %>%
  st_wrap_dateline()

# plot

sf_use_s2(FALSE)
ocean <- sf::read_sf("../ne_10m_ocean/ne_10m_ocean.shp") %>% st_make_valid()
df_sf = st_filter(df_sf, ocean, .predicate = st_intersects)

world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

ggplot() +
  geom_sf(data = df_sf, aes(fill = n), color = NA) +
  scale_fill_viridis(option = "inferno", na.value = "white", trans = "log10", labels = comma) +
  theme_void()

# output

write_sf(df_sf, "ospar.gpkg", driver = "GPKG")

# check expected totals

open_dataset("../data/obis_20220710.parquet") %>%
  filter(!is.na(species)) %>% 
  summarize(n()) %>% 
  collect()
