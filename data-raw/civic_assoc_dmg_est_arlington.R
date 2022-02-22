library(dplyr)
library(sf)
library(data.table)
library(tidyr)
library(stringr)

con <- get_db_conn()
va_arl_block_parcels_sf <- sf::st_read(con, c("dc_working", "va_arl_block_parcels"))
DBI::dbDisconnect(con)

# set as data.table to perform easy group by aggregation
setDT(va_arl_block_parcels_sf)

### block group parcel estimates ###

# get count of parcels per block group: group by block group (first 12 integers of Full_Block) and get count of units per block group (sum(Total_Units))
# remove rows where nchar(substr)!=12
arl_bg_prcel_cnt <- va_arl_block_parcels_sf[, .(cnt = sum(Total_Units)), substr(Full_Block, 1, 12)][nchar(substr)==12]

# update column names
colnames(arl_bg_prcel_cnt) <- c("bg_geoid", "prcl_cnt")

# set va_arl_block_parcels_sf back to sf for geo functions
va_arl_block_parcels_sf <- sf::st_as_sf(va_arl_block_parcels_sf)

# create block group geoid (to make merge easier)
va_arl_block_parcels_sf$bg_geoid <- substr(va_arl_block_parcels_sf$Full_Block, 1, 12)

# merge on bg_geoid
va_arl_block_parcels_cnts_sf <- merge(va_arl_block_parcels_sf, arl_bg_prcel_cnt, by = "bg_geoid")

# create parcel-level demographic multiplier by dividing Total_Units per parcel by total count of parcels in the block group (prcl_cnt)
va_arl_block_parcels_cnts_sf$mult <- va_arl_block_parcels_cnts_sf$Total_Units/va_arl_block_parcels_cnts_sf$prcl_cnt


con <- get_db_conn()
acs_data_bg <- sf::st_read(con, c("dc_common", "dcmdva_bg_acs_2013_2019_demographics_update"))
DBI::dbDisconnect(con)

acs_data_bg$cnty_fips <- substr(acs_data_bg$geoid, 1, 5)

acs_data_bg <- acs_data_bg %>%
  filter(cnty_fips == "51013",
         measure %in% c("pop_total",
                        "pop_white",
                        "pop_black",
                        "pop_native",
                        "pop_AAPI",
                        "pop_other",
                        "pop_two_or_more_races",
                        "pop_under_20",
                        "pop_20_64",
                        "pop_65_plus",
                        "pop_veteran",
                        "hh_limited_english")) %>%
  select(bg_geoid = geoid,
         name = region_name,
         year,
         measure,
         value) %>%
  as.data.frame()

# merge with ACS data and generate parcel demographic estimates by multiplying ACS estimate by parcel multipliers
va_arl_block_parcels_cnts_dmgs_sf <- merge(va_arl_block_parcels_cnts_sf,
                                           acs_data_bg,
                                           by = "bg_geoid",
                                           allow.cartesian = TRUE)

va_arl_block_parcels_cnts_dmgs_sf$prcl_estimate <- va_arl_block_parcels_cnts_dmgs_sf$mult * va_arl_block_parcels_cnts_dmgs_sf$value

# switch to data.table
va_arl_block_parcels_cnts_dmgs_dt <- data.table::as.data.table(va_arl_block_parcels_cnts_dmgs_sf)

# drop geometry column - huge because so many repeats and not needed here
va_arl_block_parcels_cnts_dmgs_dt$geometry <- NULL

# filter to needed columns
va_arl_block_parcels_cnts_dmgs_dt <- va_arl_block_parcels_cnts_dmgs_dt[, .(rpc_master = RPC_Master, geoid = Full_Block, year, measure = measure, value = prcl_estimate)]

# Cast long file to wide
va_arl_block_parcels_cnts_dmgs_dt_wide <- data.table::dcast(va_arl_block_parcels_cnts_dmgs_dt, rpc_master + geoid ~ measure + year, value.var = "value", fun.aggregate = sum)

# add back parcel geo
va_arl_parcel_geo <- unique(as.data.frame(va_arl_block_parcels_cnts_dmgs_sf[, c("RPC_Master")]))
colnames(va_arl_parcel_geo) <- c("rpc_master", "geometry")
va_arl_block_parcels_cnts_dmgs_dt_wide_geo <- merge(va_arl_block_parcels_cnts_dmgs_dt_wide, va_arl_parcel_geo, by = "rpc_master")

# back to sf
va_arl_bg_parcels_demographics_sf <- sf::st_as_sf(va_arl_block_parcels_cnts_dmgs_dt_wide_geo)


### tract parcel estimates ###
con <- get_db_conn()
va_arl_block_parcels_sf <- sf::st_read(con, c("dc_working", "va_arl_block_parcels"))
DBI::dbDisconnect(con)

# set as data.table to perform easy group by aggregation
setDT(va_arl_block_parcels_sf)

arl_tr_prcel_cnt <- va_arl_block_parcels_sf[, .(cnt = sum(Total_Units)), substr(Full_Block, 1, 11)][nchar(substr)==11]

# update column names
colnames(arl_tr_prcel_cnt) <- c("tr_geoid", "prcl_cnt")

# set va_arl_block_parcels_sf back to sf for geo functions
va_arl_block_parcels_sf <- sf::st_as_sf(va_arl_block_parcels_sf)

# create block group geoid (to make merge easier)
va_arl_block_parcels_sf$tr_geoid <- substr(va_arl_block_parcels_sf$Full_Block, 1, 11)

# merge on bg_geoid
va_arl_tr_parcels_cnts_sf <- merge(va_arl_block_parcels_sf, arl_tr_prcel_cnt, by = "tr_geoid")

# create parcel-level demographic multiplier by dividing Total_Units per parcel by total count of parcels in the block group (prcl_cnt)
va_arl_tr_parcels_cnts_sf$mult <- va_arl_tr_parcels_cnts_sf$Total_Units/va_arl_tr_parcels_cnts_sf$prcl_cnt


con <- get_db_conn()
acs_data_tr <- sf::st_read(con, c("dc_common", "dcmdva_tr_acs_2009_2019_demographics_update"))
DBI::dbDisconnect(con)

acs_data_tr$cnty_fips <- substr(acs_data_tr$geoid, 1, 5)

acs_data_tr <- acs_data_tr %>%
  filter(cnty_fips == "51013",
         measure == "pop_hispanic_or_latino",
         year %in% c(2013:2019)) %>%
  select(tr_geoid = geoid,
         year,
         measure,
         value) %>%
  as.data.frame()

# merge with ACS data and generate parcel demographic estimates by multiplying ACS estimate by parcel multipliers
va_arl_tr_parcels_cnts_dmgs_sf <- merge(va_arl_tr_parcels_cnts_sf,
                                        acs_data_tr,
                                        by = "tr_geoid",
                                        allow.cartesian = TRUE)

va_arl_tr_parcels_cnts_dmgs_sf$prcl_estimate <- va_arl_tr_parcels_cnts_dmgs_sf$mult * va_arl_tr_parcels_cnts_dmgs_sf$value

# switch to data.table
va_arl_tr_parcels_cnts_dmgs_dt <- data.table::as.data.table(va_arl_tr_parcels_cnts_dmgs_sf)

# drop geometry column - huge because so many repeats and not needed here
va_arl_tr_parcels_cnts_dmgs_dt$geometry <- NULL

# filter to needed columns
va_arl_tr_parcels_cnts_dmgs_dt <- va_arl_tr_parcels_cnts_dmgs_dt[, .(rpc_master = RPC_Master,
                                                                     geoid = Full_Block,
                                                                     year,
                                                                     measure = measure,
                                                                     value = prcl_estimate)]

# Cast long file to wide
va_arl_tr_parcels_cnts_dmgs_dt_wide <- data.table::dcast(va_arl_tr_parcels_cnts_dmgs_dt,
                                                         rpc_master + geoid ~ measure + year,
                                                         value.var = "value",
                                                         fun.aggregate = sum)

# add back parcel geo
va_arl_parcel_geo <- unique(as.data.frame(va_arl_tr_parcels_cnts_dmgs_sf[, c("RPC_Master")]))
colnames(va_arl_parcel_geo) <- c("rpc_master", "geometry")
va_arl_tr_parcels_cnts_dmgs_dt_wide_geo <- merge(va_arl_tr_parcels_cnts_dmgs_dt_wide,
                                                 va_arl_parcel_geo,
                                                 by = "rpc_master")

# back to sf before writing to DB
va_arl_tr_parcels_demographics_sf <- sf::st_as_sf(va_arl_tr_parcels_cnts_dmgs_dt_wide_geo)
va_arl_tr_parcels_demographics_sf$geometry <- NULL
va_arl_tr_parcels_demographics_sf <- va_arl_tr_parcels_demographics_sf %>% select(-geoid)

# combine block group and tract parcel estimates
va_arl_bg_parcels_demographics_sf <- va_arl_bg_parcels_demographics_sf %>%
  inner_join(va_arl_tr_parcels_demographics_sf, by = "rpc_master")


civ_assoc <- st_read("/project/biocomplexity/sdad/projects_data/mc/data_commons/Civic_Association_Polygons/Civic_Association_Polygons.shp")
#plot(st_geometry(civ_assoc))
civ_assoc_wgs84 <- st_transform(civ_assoc, 4326)
#plot(st_geometry(civ_assoc_wgs84))

civ_assoc_wgs84$area <- as.numeric(st_area(civ_assoc_wgs84))

civ_assoc_areas <- civ_assoc_wgs84 %>% select(CIVIC, area) %>% as.data.frame()
civ_assoc_areas$geometry <- NULL

# intersect with parcels to assign parcels to civic associations
int <- st_intersects(civ_assoc_wgs84, va_arl_bg_parcels_demographics_sf)


va_arl_bg_parcels_demographics_sf$civ_assoc <- NA
for (i in 1:length(int)) {
  cv <- civ_assoc_wgs84[i,]$CIVIC
  va_arl_bg_parcels_demographics_sf$civ_assoc[int[[i]]] <- cv
}

va_arl_bg_parcels_demographics_sf <- va_arl_bg_parcels_demographics_sf[!is.na(va_arl_bg_parcels_demographics_sf$civ_assoc),]

# remove geometry
va_arl_bg_parcels_demographics_sf$geometry <- NULL

civ_assoc_demographics <- va_arl_bg_parcels_demographics_sf %>%
  select(-c(1:2)) %>%
  group_by(civ_assoc) %>%
  summarise_all(sum) %>%
  mutate(perc_20_64_2013 = pop_20_64_2013 / pop_total_2013 * 100,
         perc_20_64_2014 = pop_20_64_2014 / pop_total_2014 * 100,
         perc_20_64_2015 = pop_20_64_2015 / pop_total_2015 * 100,
         perc_20_64_2016 = pop_20_64_2016 / pop_total_2016 * 100,
         perc_20_64_2017 = pop_20_64_2017 / pop_total_2017 * 100,
         perc_20_64_2018 = pop_20_64_2018 / pop_total_2018 * 100,
         perc_20_64_2019 = pop_20_64_2019 / pop_total_2019 * 100,
         perc_65_plus_2013 = pop_65_plus_2013 / pop_total_2013 * 100,
         perc_65_plus_2014 = pop_65_plus_2014 / pop_total_2014 * 100,
         perc_65_plus_2015 = pop_65_plus_2015 / pop_total_2015 * 100,
         perc_65_plus_2016 = pop_65_plus_2016 / pop_total_2016 * 100,
         perc_65_plus_2017 = pop_65_plus_2017 / pop_total_2017 * 100,
         perc_65_plus_2018 = pop_65_plus_2018 / pop_total_2018 * 100,
         perc_65_plus_2019 = pop_65_plus_2019 / pop_total_2019 * 100,
         perc_AAPI_2013 = pop_AAPI_2013 / pop_total_2013 * 100,
         perc_AAPI_2014 = pop_AAPI_2014 / pop_total_2014 * 100,
         perc_AAPI_2015 = pop_AAPI_2015 / pop_total_2015 * 100,
         perc_AAPI_2016 = pop_AAPI_2016 / pop_total_2016 * 100,
         perc_AAPI_2017 = pop_AAPI_2017 / pop_total_2017 * 100,
         perc_AAPI_2018 = pop_AAPI_2018 / pop_total_2018 * 100,
         perc_AAPI_2019 = pop_AAPI_2019 / pop_total_2019 * 100,
         perc_black_2013 = pop_black_2013 / pop_total_2013 * 100,
         perc_black_2014 = pop_black_2014 / pop_total_2014 * 100,
         perc_black_2015 = pop_black_2015 / pop_total_2015 * 100,
         perc_black_2016 = pop_black_2016 / pop_total_2016 * 100,
         perc_black_2017 = pop_black_2017 / pop_total_2017 * 100,
         perc_black_2018 = pop_black_2018 / pop_total_2018 * 100,
         perc_black_2019 = pop_black_2019 / pop_total_2019 * 100,
         perc_native_2013 = pop_native_2013 / pop_total_2013 * 100,
         perc_native_2014 = pop_native_2014 / pop_total_2014 * 100,
         perc_native_2015 = pop_native_2015 / pop_total_2015 * 100,
         perc_native_2016 = pop_native_2016 / pop_total_2016 * 100,
         perc_native_2017 = pop_native_2017 / pop_total_2017 * 100,
         perc_native_2018 = pop_native_2018 / pop_total_2018 * 100,
         perc_native_2019 = pop_native_2019 / pop_total_2019 * 100,
         perc_other_2013 = pop_other_2013 / pop_total_2013 * 100,
         perc_other_2014 = pop_other_2014 / pop_total_2014 * 100,
         perc_other_2015 = pop_other_2015 / pop_total_2015 * 100,
         perc_other_2016 = pop_other_2016 / pop_total_2016 * 100,
         perc_other_2017 = pop_other_2017 / pop_total_2017 * 100,
         perc_other_2018 = pop_other_2018 / pop_total_2018 * 100,
         perc_other_2019 = pop_other_2019 / pop_total_2019 * 100,
         perc_two_or_more_races_2013 = pop_two_or_more_races_2013 / pop_total_2013 * 100,
         perc_two_or_more_races_2014 = pop_two_or_more_races_2014 / pop_total_2014 * 100,
         perc_two_or_more_races_2015 = pop_two_or_more_races_2015 / pop_total_2015 * 100,
         perc_two_or_more_races_2016 = pop_two_or_more_races_2016 / pop_total_2016 * 100,
         perc_two_or_more_races_2017 = pop_two_or_more_races_2017 / pop_total_2017 * 100,
         perc_two_or_more_races_2018 = pop_two_or_more_races_2018 / pop_total_2018 * 100,
         perc_two_or_more_races_2019 = pop_two_or_more_races_2019 / pop_total_2019 * 100,
         perc_under_20_2013 = pop_under_20_2013 / pop_total_2013 * 100,
         perc_under_20_2014 = pop_under_20_2014 / pop_total_2014 * 100,
         perc_under_20_2015 = pop_under_20_2015 / pop_total_2015 * 100,
         perc_under_20_2016 = pop_under_20_2016 / pop_total_2016 * 100,
         perc_under_20_2017 = pop_under_20_2017 / pop_total_2017 * 100,
         perc_under_20_2018 = pop_under_20_2018 / pop_total_2018 * 100,
         perc_under_20_2019 = pop_under_20_2019 / pop_total_2019 * 100,
         perc_veteran_2013 = pop_veteran_2013 / pop_total_2013 * 100,
         perc_veteran_2014 = pop_veteran_2014 / pop_total_2014 * 100,
         perc_veteran_2015 = pop_veteran_2015 / pop_total_2015 * 100,
         perc_veteran_2016 = pop_veteran_2016 / pop_total_2016 * 100,
         perc_veteran_2017 = pop_veteran_2017 / pop_total_2017 * 100,
         perc_veteran_2018 = pop_veteran_2018 / pop_total_2018 * 100,
         perc_veteran_2019 = pop_veteran_2019 / pop_total_2019 * 100,
         perc_white_2013 = pop_white_2013 / pop_total_2013 * 100,
         perc_white_2014 = pop_white_2014 / pop_total_2014 * 100,
         perc_white_2015 = pop_white_2015 / pop_total_2015 * 100,
         perc_white_2016 = pop_white_2016 / pop_total_2016 * 100,
         perc_white_2017 = pop_white_2017 / pop_total_2017 * 100,
         perc_white_2018 = pop_white_2018 / pop_total_2018 * 100,
         perc_white_2019 = pop_white_2019 / pop_total_2019 * 100,
         perc_hispanic_or_latino_2013 = pop_hispanic_or_latino_2013 / pop_total_2013 * 100,
         perc_hispanic_or_latino_2014 = pop_hispanic_or_latino_2014 / pop_total_2014 * 100,
         perc_hispanic_or_latino_2015 = pop_hispanic_or_latino_2015 / pop_total_2015 * 100,
         perc_hispanic_or_latino_2016 = pop_hispanic_or_latino_2016 / pop_total_2016 * 100,
         perc_hispanic_or_latino_2017 = pop_hispanic_or_latino_2017 / pop_total_2017 * 100,
         perc_hispanic_or_latino_2018 = pop_hispanic_or_latino_2018 / pop_total_2018 * 100,
         perc_hispanic_or_latino_2019 = pop_hispanic_or_latino_2019 / pop_total_2019 * 100) %>%
  as.data.frame()

civ_assoc_demographics <- civ_assoc_demographics %>%
  left_join(civ_assoc_areas, by = c("civ_assoc" = "CIVIC")) %>%
  mutate(density_2013 = pop_total_2013 / area,
         density_2014 = pop_total_2014 / area,
         density_2015 = pop_total_2015 / area,
         density_2016 = pop_total_2016 / area,
         density_2017 = pop_total_2017 / area,
         density_2018 = pop_total_2018 / area,
         density_2019 = pop_total_2019 / area) %>%
  as.data.frame()

civ_assoc_demo_long <- civ_assoc_demographics %>%
  relocate(area, .after = civ_assoc) %>%
  pivot_longer(hh_limited_english_2016:density_2019,
               names_to = "measure",
               values_to = "value") %>%
  mutate(year = str_sub(measure, -4),
         measure = str_sub(measure, 1, -6)) %>%
  as.data.frame()

con <- get_db_conn()
civ_assoc_names <- sf::st_read(con, c("dc_geographies", "va_013_arl_2020_civic_assoc_geo_names"))
DBI::dbDisconnect(con)

civ_assoc_full_demo_long <- civ_assoc_demo_long %>%
  rename(region_name = civ_assoc) %>%
  select(-area) %>%
  left_join(civ_assoc_names, by = "region_name") %>%
  mutate(measure_type = rep(c(rep("count", 88), rep("ratio", 84)), 62)) %>%
  select(geoid, region_type, region_name, year, measure, value, measure_type) %>%
  as.data.frame()

# save to DB
con <- get_db_conn()
dc_dbWriteTable(con,
                "dc_common",
                "va013_ca_sdad_2013_2019_civ_assoc_demographics",
                civ_assoc_full_demo_long)
DBI::dbDisconnect(con)

