## ----------------------------------------------------------------
## 
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: August 3rd, 2026
## 
## Description: 
## 
# "SUBSECTION D1: Load Combined Batch Results",
#       demonstrating the read-in for these complex DuckDB results.
## 
## NOTE: Under the Data Use Agreements (DUAs) with Data Axle and the USPS API 
##       license, raw data cannot be publicly distributed and is stored locally 
##       in "~/KEEP LOCAL" directories. Some code or results may also be 
##       restricted. All publicly distributed results are summarized, and 
##       publicly distributed code has been constructed to avoid referencing 
##       individual-level data. Executing the code below requires access to the 
##       raw data and results.
## 
##       API keys are user-specific and, where applicable, instructions have 
##       been provided to help users obtain their own and configure them locally 
##       or on a High Performance Computer (HPC).
## 
## NOTE: In Spring 2026, the pipeline developed for the Summer 2025 symposium
##       prototype was lightly refactored for clarity and rerun to process all
##       entries not covered in the initial pass. Core methods remained 
##       consistent with the prototype. An updated dataset delivered in May 2026 
##       prompted further expansion of the pipeline to support two designated 
##       format variations: the 2023 Format and the 2026 Format.
## 
##       This script contains the revised pipeline developed and represents the 
##       current recommended workflow.
## 
##       The steps for the 2023 Format and the 2026 Format are no longer in the 
##       same processing sequence, and are therefore not directly comparable. 
##       Adjustments were made to:
## 
##            1. Implement changes based on the findings in "Process Data Update.R",
##               which documents key differences between the two formats.
## 
##            2. Refactor the workflow to resolve errors encountered during 
##               prototype development and to improve computational performance.
## 
##            3. Increase use of the High Performance Computing (HPC) environment, 
##               which was not available during initial development and 
##               previously required more discrete steps to accommodate local 
##               limitations.
## 
##            4. Account for updated USPS API terms of use which will now
##               incur costs to users.
## 
## USPS API Keys:
## To query the USPS database, a client key and secret must be configured to
## generate an OAuth token for database access. These credentials CANNOT be
## shared and must remain private to each user. They should be kept untracked
## by Git and stored locally, and must never be published to GitHub.
## 
## Follow the steps below to set up your credentials and environment.
## 
## 1. Register for a USPS developer account by following the "Getting Started"
##    instructions on the USPS Developer Portal:
##    https://developers.usps.com/
## 
## 2. In the "Apps" section of the developer portal, create a new app to
##    generate your personal API credentials. Be sure to include a project
##    description. This will provide two credentials: a "Consumer Key" and a
##    "Consumer Secret".
## 
##    As noted above, these credentials must NOT be hard-coded into the script.
##    They must remain private to each user.
## 
## 3. In the project root directory, create a ".Renviron" file if one does not
##    already exist. Add your credentials as shown below, with no extra spaces
##    or hidden characters:
## 
##       USPS_CONSUMER_KEY="your_consumer_key"
##       USPS_CONSUMER_SECRET="your_consumer_secret"
## 
##    These variables will be loaded in the script below using sys.getenv().
## 
## 4. Ensure that the ".Renviron" file is listed in your ".gitignore" file and
##    is not being tracked by Git.
## 
## 
## NOTE: This script requires GDAL to run. Verify that GDAL is installed on
##       your device using the following commands:
##
##       Check for the TIGER driver (returns one row if installed)
##          sf::st_drivers() |> subset(name == "TIGER")
##
##       Confirm the GDAL version (this script was developed using v3.5.3)
##          sf::sf_extSoftVersion()["GDAL"]
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Variable Characteristics and Missingness Impact
##        * SUBSECTION A1: Uniqueness, Missingness, and Key Variable Characteristics
##        * SUBSECTION A2: Distribution of Characteristics Requiring Removal
##        * SUBSECTION A3: Set Geocoder Search Priorities
##        * SUBSECTION A4: Build Precompiled TIGER/Line GeoPackages
## 
##    - PART B: ALGORITHM TO CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA
## 
##    - PART C: Recompile Results from the HPC
##        * SUBSECTION C1: Batch Array 18850425
##        * SUBSECTION C2: Batch Array 20823868
##        * SUBSECTION C3: Combining the Batches
## 
##    - PART D: Assess Overall Performance
##        * SUBSECTION D1: Load Combined Batch Results
##        * SUBSECTION D2: Confirm Complete ABI Coverage
##        * SUBSECTION D3: Plot the Distribution of Quality Check Outcomes
##        * SUBSECTION D4: At-Point-of-Evaluation Quality Checks

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("openxlsx")         # Read/write Excel workbooks (.xlsx) with multiple sheets
  library("DBI")              # Standard database interface for R (dbConnect, dbWriteTable, dbGetQuery)
  library("duckdb")           # DuckDB database engine + DBI backend (local .duckdb files, in-memory DBs)
  library("arrow")            # Parquet/Feather & fast I/O (Arrow)
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("dbplyr")           # Data manipulation and transformation for DuckDB
  library("stringr")          # String operations
  library("ggplot2")          # Graphics and visualization
  library("patchwork")        # Combine multiple ggplot objects into a single layout
  library("scales")           # Scale/label helpers for plots (percent/number/date formatting, breaks)
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests for APIs (GET/POST, headers, auth)
  library("jsonlite")         # Parse/write JSON (fromJSON/toJSON)
  library("future.apply")     # Parallel processing
  library("stringdist")       # Measuring string distances
  library("progress")         # Progress bars
  library("data.table")       # High-performance data manipulation
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
})


# Set up the plan for parallel processing
plan(multisession, workers = 4)

# Load in the functions
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 2_2026 Format.R")
source("./Code/Support Functions/For Step 2_Compile Decennial Data.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))

# Define the "if else" for null options operation
"%||%" <- function(a, b) if (!is.null(a)) a else b

# Cache TIGRIS shapefiles locally to avoid re-downloading each session
options(tigris_use_cache = TRUE)

# Use S2 spherical geometry engine for correct lon/lat distance calculations
sf::sf_use_s2(TRUE)




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Compiled results for this step are saved to
# "~/From Clean Raw Data/Step 2_2026 Format/Compiled by Batches/"
# and can be loaded directly from that location. The code for loading these
# pre-prepared files can be found in "PART D: Assess Overall Performance".
# 
# NOTE: The file sizes are large enough that loading both the raw data and the
#       newly produced files simultaneously may cause problems. Prioritize
#       which files are loaded based on the comparisons or outcomes being
#       assessed in a given session.


# Load standardized and converted data.
church_2026_form    <- read_parquet("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")
church_2026_form_dt <- as.data.table(church_2026_form)  # Convert for efficient data manipulation
setorder(church_2026_form_dt, state, abi)  # Organize the table by state to increase census boundary efficiency then abi

# Load the coded representation of variables from "Process Data Update.R".
core_fields <- read_csv("./Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")

# Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
uscities_df <- read_csv("./Data/Raw/simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)




## ----------------------------------------------------------------
## PART A: Variable Characteristics and Missingness Impact

# Prior to running the cleaning and validation algorithm, it is important
# to assess the overall characteristics and missingness of the variables
# being processed. This section reviews these aspects and reports on them.
# 
# Note that the algorithm was designed with the explicit intention of
# retaining as much of the original raw data as possible. Only ABIs filing
# under addresses outside of the United States were excluded prior to
# evaluation.

## --------------------
## SUBSECTION A1: Uniqueness, Missingness, and Key Variable Characteristics

# Count the number of unique ABIs.
church_2026_form_dt[
  ,
  .(n = .N, n_distinct = uniqueN(abi))
]

# Confirm that years represented for each ABI is unique (no duplications).
dup_check <- church_2026_form_dt[
  ,
  .(n = .N, n_distinct = uniqueN(archive_version_year)),
  by = abi
]

# Identify ABIs with repeated years.
violations <- dup_check[n != n_distinct]

# Result is empty, validating that no duplicate year entries exist.
nrow(violations)
violations

# It is interesting to see how many ABIs filed at multiple addresses, which
# may translate into moves outside a community of impact.
num_addresses <- church_2026_form_dt[
  ,
  .(n_unique_addresses = data.table::uniqueN(combined_address)),
  by = abi
]

# The vast majority of entries did not have any change of address (70%), with
# 20% changing address only once. The remaining 10% moved more than twice,
# with less than 1% moving more than five times.
result <- table(num_addresses$n_unique_addresses, useNA = "ifany") %>%
  (\(x) {
    data.frame(
      n_unique_addresses = names(x),
      count              = as.integer(x),
      row.names          = NULL
    ) %>%
      mutate(percent = round(100 * count / sum(count), 2))
  })() %>%
  mutate(n_unique_addresses = as.character(n_unique_addresses)) %>%
  pivot_longer(c(count, percent), names_to = "metric", values_to = "value") %>%
  pivot_wider(names_from = n_unique_addresses, values_from = value) %>%
  mutate(across(-metric, ~ ifelse(metric == "count", as.character(as.integer(.x)), .x)))


# Some characteristics are neither verifiable nor reconcilable for subsequent
# analyses, including PO Boxes, missing address_line_1, and missing
# geocoordinates that cannot be recovered via address-based validation.
problematic_entries <- church_2026_form_dt[
  ,
  .(
    n = .N,
    poBox                = any(!is.na(address_line_1) & grepl("\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b", address_line_1, ignore.case = TRUE)),
    any_na_address       = any(is.na(address_line_1)),
    any_na_city          = any(is.na(city)),
    any_na_state         = any(is.na(state)),
    any_na_zipcode       = any(is.na(zipcode)),
    any_na_zip4          = any(is.na(zip4)),
    empty_non_na_address = any(!is.na(address_line_1) & trimws(address_line_1) == ""),
    na_lon               = any(is.na(longitude)),
    na_lat               = any(is.na(latitude))
  ),
  by = abi
]

# Among all ABIs: 13.74% filed under a PO Box at least once, 2.64% had at
# least one record missing address_line_1 (NA only; no blank strings observed).
round(prop.table(table(problematic_entries$poBox, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(problematic_entries$any_na_address, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(problematic_entries$empty_non_na_address, useNA = "ifany"))*100, digits = 2)

# Only 0.24% of ABIs have both a PO Box and a missing address_line_1 entry.
round(prop.table(table(
  "PO Box" = problematic_entries$poBox,
  "NA Address" = problematic_entries$any_na_address,
  useNA = "ifany"
))*100, digits = 2)

# No entries are missing state or city; 0.001% lack a zip code and ~22% lack
# the four-digit zip extension. These missing values pose minimal geocoding
# risk, except for the 8 entries without a zip code; should address validation
# fail SimpleMaps secondary validation would not be possible.
round(prop.table(table(problematic_entries$any_na_city, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(problematic_entries$any_na_state, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(problematic_entries$any_na_zipcode, useNA = "ifany"))*100, digits = 3)
round(prop.table(table(problematic_entries$any_na_zip4, useNA = "ifany"))*100, digits = 2)

# All ABIs with missing geocoordinates are missing both longitude and latitude.
round(prop.table(table(
  "NA Lon" = problematic_entries$na_lon,
  "NA Lat" = problematic_entries$na_lat,
  useNA = "ifany"
))*100, digits = 2)

# Minimal overlap exists among the three problematic characteristics;
# 83.51% of ABIs are unaffected by any of them.
round(prop.table(table(
  "NA Lon" = problematic_entries$na_lon,
  "PO Box" = problematic_entries$poBox,
  "NA Address" = problematic_entries$any_na_address,
  useNA = "ifany"
))*100, digits = 2)

# All character columns with an expected fixed size (state acronym, ZIP
# code, and extension) are consistent across all ABIs.
church_2026_form_dt[
  ,
  .(
    n = .N,
    
    # max observed character length (excluding NA)
    max_nchar_state   = max(nchar(as.character(state)),   na.rm = TRUE),
    max_nchar_zipcode = max(nchar(as.character(zipcode)), na.rm = TRUE),
    max_nchar_zip4    = max(nchar(as.character(zip4)),    na.rm = TRUE),
    
    # any non-NA values with unexpected length (state=2, zipcode=5, zip4=4)
    any_badlen_state   = any(!is.na(state)   & nchar(as.character(state))   != 2L),
    any_badlen_zipcode = any(!is.na(zipcode) & nchar(as.character(zipcode)) != 5L),
    any_badlen_zip4    = any(!is.na(zip4)    & nchar(as.character(zip4))    != 4L)
  )
]


## --------------------
## SUBSECTION A2: Distribution of Characteristics Requiring Removal

# PO Box usage and missing address_line_1 values require removal of all 
# associated ABI entries before metric calculation, potentially affecting up to 
# 16% of ABI. The following plots assess their spatial distribution by city, as 
# impact varies by region.
#
# Missing geocoordinates (<1% of ABIs) prevent spatial joining of census
# boundaries and will be reconciled via address-based geocoding; results
# will be assessed following analysis.


# -- Import City-Level TIGER/Line Shapefiles ---------

# Places polygons for all states + DC; skip any state that errors
pl_sf <- do.call(rbind, lapply(c(state.abb, "DC"), \(st)
                               tryCatch(tigris::places(state = st, cb = TRUE, year = 2023), error = \(e) NULL)
)) %>% st_make_valid()

# Representative in-polygon points (compute in CONUS Albers, return lon/lat)
pl_ll <- pl_sf %>%
  st_transform(5070) %>%
  st_point_on_surface() %>%
  st_transform(4326)

# (STATE, CITY) -> (LON, LAT) lookup (uppercased for consistent joins)
coords <- st_coordinates(pl_ll)
places_dt <- as.data.table(st_drop_geometry(pl_ll))[
  , .(state = toupper(STUSPS), city = toupper(NAME),
      lon = coords[,1], lat = coords[,2])
][, unique(.SD), by = .(state, city)]

# Contiguous US + DC state polygons (for masking/plotting)
contig <- setdiff(c(state.abb, "DC"), c("AK","HI"))
us_states_sf <- tigris::states(cb = TRUE, year = 2023, class = "sf") %>%
  (\(x) x[x$STUSPS %in% contig, ])() %>%
  st_make_valid()


#' @description
#' Codebook for `us_states_sf`, an `sf` simple feature collection of U.S. state
#' and DC boundaries used as a basemap layer, based on the 2023 cartographic
#' boundaries.
#'
#' @field STATEFP Two-digit state FIPS code (character).
#' @field STATENS GNIS feature identifier for the state (character).
#' @field GEOIDFQ Fully qualified geographic identifier (character).
#' @field GEOID State geographic identifier (typically the state FIPS code as character).
#' @field STUSPS USPS state abbreviation (e.g., "CA") (character).
#' @field NAME State name (character).
#' @field LSAD Legal/statistical area description code (character).
#' @field ALAND Land area in square meters (numeric/integer).
#' @field AWATER Water area in square meters (numeric/integer).
#' @field geometry State boundary geometry as MULTIPOLYGON.

# Save result.
st_write(
  us_states_sf,
  dsn = "./Data/Results/From Clean Raw Data/Step 2_2026 Format/us_states_sf.shp",
  delete_dsn = TRUE
)


# -- Generate Event Counts by City -------------------

abi_city_flag <- church_2026_form_dt[
  ,
  .(
    poBox          = any(!is.na(address_line_1) & grepl("\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b", address_line_1, ignore.case = TRUE)),
    any_na_address = any(is.na(address_line_1)),
    any_na_city    = any(is.na(city)),
    any_na_state   = any(is.na(state)),
    any_na_zipcode = any(is.na(zipcode))
  ),
  by = .(state, city, zipcode, abi)
]


# -- Distribution of ABI Characteristics by City -----

# 1) NA Addresses
na_by_city <- abi_city_flag[
  , .(
    n_abi = uniqueN(abi),
    n_abi_any_na_address_line_1   = sum(any_na_address),
    pct_abi_any_na_address_line_1 = round(100 * mean(any_na_address), 2)
  ),
  by = .(state, city, zipcode)
][
  n_abi_any_na_address_line_1 > 0
][
  order(-n_abi_any_na_address_line_1)
][
  , `:=`(
    state   = toupper(state),
    city    = toupper(city),
    zipcode = sprintf("%05s", zipcode)
  )
]

summary(na_by_city[["pct_abi_any_na_address_line_1"]])

# 2) PO Boxes
po_by_city <- abi_city_flag[
  , .(
    n_abi = uniqueN(abi),
    n_abi_poBox = sum(poBox),
    pct_abi_poBox = round(100 * mean(poBox), 2)
  ),
  by = .(state, city, zipcode)
][
  n_abi_poBox > 0
][
  order(-n_abi_poBox)
][
  , `:=`(
    state   = toupper(state),
    city    = toupper(city),
    zipcode = sprintf("%05s", zipcode)
  )
]

summary(po_by_city[["pct_abi_poBox"]])


# -- Distribution of ABI Characteristics by City -----

na_joined <- join_places_with_zip_fix(na_by_city, places_dt, zip_city_lookup)
po_joined <- join_places_with_zip_fix(po_by_city, places_dt, zip_city_lookup)

# Following the join and SimpleMaps zip-to-city supplementation, ~11% and ~13%
# of listed cities could not be matched to a TIGER/Line Shapefile feature.
round((na_joined[is.na(lon) | is.na(lat), .N]/nrow(na_joined))*100, digits = 2)
round((po_joined[is.na(lon) | is.na(lat), .N]/nrow(po_joined))*100, digits = 2)


#' @description
#' Codebook for the summary output fields produced by the evaluation. Separate
#' datasets are created for missing address_line_1 and PO Box entries.
#'
#' @field state/city/zip_code Address elements associated with each entry.
#'
#' @field n_abi Number of unique ABIs represented in that city or zip code.
#'
#' @field `[n|pct]_abi_any_na_address_line_1` Count and percent of ABIs with at 
#'                                            least one missing address_line_1.
#'
#' @field `[n|pct]_abi_poBox` Count and percent of ABIs with at least one PO 
#'                            Box entry.
#'
#' @field lon/lat TIGER/Line Shapefile geocoordinates associated with the city.

# # Save result.
# write.csv(na_joined, file = "./Data/Results/From Clean Raw Data/Step 2_2026 Format/ABI with NA Addresses by City_08.07.2026.csv")
# write.csv(po_joined, file = "./Data/Results/From Clean Raw Data/Step 2_2026 Format/ABI with PO Boxes by City_08.07.2026.csv")


# -- Distribution of ABI Characteristics by City -----

# Plot 1: Missing address line 1
dt_na <- na_joined[
  state %in% setdiff(c(state.abb, "DC"), c("AK","HI")) & !is.na(lon) & !is.na(lat)
]

sf_na <- st_as_sf(dt_na, coords = c("lon", "lat"), crs = 4326) |>
  st_transform(5070)

p_na <- ggplot() +
  geom_sf(data = us_states_sf, fill = "grey95", color = "grey35", linewidth = 0.6) +
  geom_sf(
    data = sf_na,
    aes(
      color = pct_abi_any_na_address_line_1,
      size  = n_abi_any_na_address_line_1
    ),
    alpha = 0.85
  ) +
  scale_color_viridis_c(
    option = "magma", end = 0.90,
    name   = "% of ABIs",
    labels = scales::label_number(suffix = "%")
  ) +
  scale_size_continuous(
    name   = "N ABIs",
    labels = scales::label_comma()
  ) +
  coord_sf(crs = st_crs(5070)) +
  labs(
    title    = "ABIs with Any Missing Address Line 1 by City",
    subtitle = "Color = % of ABIs; size = N ABIs"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    panel.grid    = element_blank(),
    axis.title    = element_blank(),
    axis.text     = element_blank(),
    axis.ticks    = element_blank(),
    plot.title    = element_text(size = 14),
    plot.subtitle = element_text(size = 11),
    legend.title  = element_text(size = 11),
    legend.text   = element_text(size = 10)
  )


# Plot 2: PO Boxes
dt_po <- po_joined[
  state %in% setdiff(c(state.abb, "DC"), c("AK","HI")) & !is.na(lon) & !is.na(lat)
]

sf_po <- st_as_sf(dt_po, coords = c("lon", "lat"), crs = 4326) |>
  st_transform(5070)

p_po <- ggplot() +
  geom_sf(data = us_states_sf, fill = "grey95", color = "grey35", linewidth = 0.6) +
  geom_sf(
    data = sf_po,
    aes(
      color = pct_abi_poBox,
      size  = n_abi_poBox
    ),
    alpha = 0.85
  ) +
  scale_color_viridis_c(
    option = "magma", end = 0.90,
    name   = "% of ABIs",
    labels = scales::label_number(suffix = "%")
  ) +
  scale_size_continuous(
    name   = "N ABIs",
    labels = scales::label_comma()
  ) +
  coord_sf(crs = st_crs(5070)) +
  labs(
    title    = "ABIs using PO Boxes by City",
    subtitle = "Color = % of ABIs; size = N ABIs"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    panel.grid    = element_blank(),
    axis.title    = element_blank(),
    axis.text     = element_blank(),
    axis.ticks    = element_blank(),
    plot.title    = element_text(size = 14),
    plot.subtitle = element_text(size = 11),
    legend.title  = element_text(size = 11),
    legend.text   = element_text(size = 10)
  )


p_na / p_po


## --------------------
## SUBSECTION A3: Set Geocoder Search Priorities

# The U.S. Census Geocoder API supports multiple benchmarks and vintages for
# geolocation searches. The following lines identify the available options
# and construct a prioritized search sequence that cycles through benchmark/
# vintage combinations if an initial search returns no result.

# Display available benchmarks and vintages
census_geo_show_options(filter_benchmark_name = "Public_AR_", max_benchmarks = 50)

# Construct a prioritized table of benchmark/vintage search criteria,
# where earlier rows take precedence.
spec <- tibble::tibble(
  benchmark_name = c("Public_AR_Census2020", "Public_AR_Census2020",
                     "Public_AR_Current",  "Public_AR_ACS2025"),
  vintage_name   = c("Census2020_Census2020", "Census2010_Census2020",
                     "Current_Current",       "Current_ACS2025")
)

# Construct the "tries" input for `validate_geolocation()`
geocoder_census_tries <- census_geo_make_tries(spec)


## --------------------
## SUBSECTION A4: Build Precompiled TIGER/Line GeoPackages

# When processing the 2023 Format, the tigris R package was used to retrieve 
# relevant decennial data from the U.S. Census Bureau's TIGER/Line Shapefiles 
# API. This approach had difficulty scaling, failed to associate 2020 decennial
# data as reliably as desired, and experienced timeouts due to slow query
# responses.
#
# To address these issues, state block- and block group-level shapefiles were 
# individually downloaded and precompiled into GeoPackage (*.gpkg) files 
# containing all desired metadata (block, tract, county, and state) as layers 
# by decennial year. The 2026 Format additionally incorporated Core Based 
# Statistical Areas and ZIP Code Tabulation Areas (ZCTA), which were downloaded a
# nd included in the annotation process. Further details can be found in the 
# ~/Census Bureau TIGER Line Shapefiles/ directory under ./Data/Raw or 
# ./Data/Results in the project GitHub: 
# https://github.com/SOCAH-Lab/Church-Closures-Dashboard/
#
# This section generates the compiled shapefiles used in the algorithm. To
# regenerate any existing files, delete the relevant GeoPackages in
# Data/Results and rerun the script.
#
# NOTE: A warning is occasionally thrown during execution. If this occurs,
#       delete the generated files and restart the R session before retrying.


# Preflight TIGER inputs/outputs for the requested geography.
# This checks BOTH:
# - raw inputs: required core-area ZIPs and per-state TIGER/Line ZIPs (2000/2010/2020),
#   including a lightweight ZIP readability test; and
# - rendered outputs: expected GeoPackage files and required layers.
#
# Only state folders with at least one matching ZIP are treated as “detected” and
# are checked/reported.
# 
# NOTE: This may take a few minutes to run.

# Optionally choose to process block- or block group-level shapefiles. Block-level
# files take longer to handle, but do contain block group-level details.
status <- preflight_tiger_outputs(geography = "block groups")


# Set the root directories
raw_root_raw <- "Data/Raw/Census Bureau TIGER Line Shapefiles"
out_root_raw <- "Data/Results/Census Bureau TIGER Line Shapefiles"

# Generate GeoPackages for any source files detected in "~/Raw/Census Bureau
# TIGER Line Shapefiles/" that are missing from "~/Results/Census Bureau TIGER
# Line Shapefiles/", based on the preceding status check.

if (status$core_areas_ok && status$states_ok) {
  
  message("All done. Outputs are in: ", out_root_raw)
  
} else {
  
  # Assume:
  # status <- preflight_tiger_outputs(geography = "blocks")  # or "block groups"
  geography <- status$geography
  prefix    <- if (geography == "blocks") "blocks" else "bg"
  
  # Convert paths to absolute paths so results do not depend on the working directory
  raw_root <- normalizePath(raw_root_raw, winslash = "/", mustWork = TRUE)
  out_root <- normalizePath(out_root_raw, winslash = "/", mustWork = FALSE)
  
  # Create the output folder tree if it does not already exist
  dir.create(out_root, recursive = TRUE, showWarnings = FALSE)
  
  # --- Build missing state GPKGs only when state outputs are incomplete --------
  if (!status$states_ok) {
    
    state_dirs <- find_state_dirs(raw_root, geography = geography)
    message("Found ", length(state_dirs), " state directories in ", raw_root_raw)
    print(basename(state_dirs))
    
    purrr::walk(state_dirs, function(sd) {
      
      z <- find_state_block_zips(sd, geography = geography)
      
      # Determine statefp by reading as little as possible (try 2020, then 2010, then 2000)
      x20 <- standardize_geo_layer(read_zip_sf(z$zip2020), year = 2020, geography = geography)
      st <- unique(x20$statefp)
      
      if (length(st) != 1) {
        x10 <- standardize_geo_layer(read_zip_sf(z$zip2010), year = 2010, geography = geography)
        st <- unique(x10$statefp)
      } else {
        x10 <- NULL
      }
      
      if (length(st) != 1) {
        x00 <- standardize_geo_layer(read_zip_sf(z$zip2000), year = 2000, geography = geography)
        st <- unique(x00$statefp)
      } else {
        x00 <- NULL
      }
      
      if (length(st) != 1) stop("Could not determine unique statefp for: ", sd)
      st <- st[1]
      
      final_path <- file.path(
        out_root,
        paste0(prefix, "_statefp_", st, "_2000_2010_2020.gpkg")
      )
      
      if (file.exists(final_path)) {
        message("Skipping (already exists): ", basename(final_path))
        return(invisible(NULL))
      }
      
      # Load any layers not already read during statefp detection
      if (is.null(x00)) x00 <- standardize_geo_layer(read_zip_sf(z$zip2000), year = 2000, geography = geography)
      if (is.null(x10)) x10 <- standardize_geo_layer(read_zip_sf(z$zip2010), year = 2010, geography = geography)
      if (is.null(x20)) x20 <- standardize_geo_layer(read_zip_sf(z$zip2020), year = 2020, geography = geography)
      
      write_state_gpkg(
        x00, x10, x20,
        final_path,
        geography = geography,
        use_tmp_then_copy = TRUE
      )
      message("Wrote state GPKG: ", basename(final_path))
      
      invisible(gc())
    })
  }
  
  # --- Build missing core-areas GPKG only when it is missing/incomplete --------
  if (!status$core_areas_ok) {
    
    z <- find_core_area_zips(raw_root)
    
    message("Found required core-area ZIPs:")
    print(basename(unlist(z)))
    
    target_crs <- 4326
    final_path <- file.path(out_root, "core_areas.gpkg")  # keep in sync with preflight_tiger_outputs()
    
    # Guard against race conditions
    if (file.exists(final_path)) {
      message("Skipping (already exists): ", basename(final_path))
    } else {
      
      # ---- Build CBSA/CSA layers (2007/2010/2020) ----
      core_2007 <- build_cbsa_csa(z$zip_cbsa_2007, z$zip_csa_2007, 2007, target_crs = target_crs)
      core_2010 <- build_cbsa_csa(z$zip_cbsa_2010, z$zip_csa_2010, 2010, target_crs = target_crs)
      core_2020 <- build_cbsa_csa(z$zip_cbsa_2020, z$zip_csa_2020, 2020, target_crs = target_crs)
      
      # ---- Build ZCTA layers (2000/2010/2020) ----
      # Updated build_zcta() can (optionally) annotate ZCTAs with area_states if
      # matching state boundaries are supplied. We pass the state ZIPs here so
      # the output includes area_states consistently.
      zcta_2000 <- build_zcta(
        z$zip_zcta_2000, 2000, target_crs = target_crs,
        zip_state = z$zip_state_2000
      )
      zcta_2010 <- build_zcta(
        z$zip_zcta_2010, 2010, target_crs = target_crs,
        zip_state = z$zip_state_2010
      )
      zcta_2020 <- build_zcta(
        z$zip_zcta_2020, 2020, target_crs = target_crs,
        zip_state = z$zip_state_2020
      )
      
      write_core_areas_gpkg(
        cbsa_csa = list("2007" = core_2007, "2010" = core_2010, "2020" = core_2020),
        zcta     = list("2000" = zcta_2000, "2010" = zcta_2010, "2020" = zcta_2020),
        final_path = final_path,
        use_tmp_then_copy = TRUE
      )
      
      message("Wrote core-areas GPKG: ", basename(final_path))
      invisible(gc())
    }
  }
  
  message(
    "All done. State ", if (geography == "blocks") "Block" else "Block Group",
    " and core-areas GPKGs are in: ", out_root_raw
  )
}


#' @description
#' Codebook for the block and block-group level shapefiles created for each
#' state. Both will result in similar standardized outcomes, only reflecting
#' different levels of census boundary detail. Files for each state contain
#' three layers: one for the 2000, 2010, and 2020 decennial years, prefaced
#' as, for example, "bg_2000" for the block-group 2000 decennial year layer.
#' 
#' When these results are imported into the HPC algorithm, each state's
#' shapefile layers are stored as a list element indexed by its state acronym.
#'
#' @field decennial_year The decennial census year associated with the boundary
#'                       definition (2000, 2010, or 2020).
#'                       
#' @field geoid Full concatenated census geographic identifier, combining the
#'              state, county, tract, and block-group FIPS codes into a unique 
#'              identifier for each boundary unit.
#'              
#' @field statefp Two-digit state Federal Information Processing Series (FIPS)
#'                code identifying the state.
#'                
#' @field countyfp Three-digit county FIPS code identifying the county within
#'                 the state.
#'                 
#' @field tractce Six-digit census tract code identifying the census tract
#'                within the county.
#'                
#' @field blkgrpce Single-digit block-group code identifying the block group
#'                 within the census tract.
#'                 
#' @field geom Spatial geometry column containing the boundary polygons for
#'             each census unit, in the form of an `sf` geometry object.


#' @description
#' Codebook for the core area shapefiles, encompassing Metropolitan and
#' Micropolitan Statistical Areas (CBSA) and Combined Statistical Areas (CSA).
#' All area types were compiled together, with separate layers representing 
#' three decennial years: 2000 (approximated by 2007, the earliest available), 
#' 2010, and 2020.
#'
#' @field vintage_year The decennial census year associated with the boundary
#'                     definition (2007, 2010, or 2020).
#'
#' @field area_type    Classification of the area as either a Core-Based
#'                     Statistical Area (CBSA) or a Combined Statistical
#'                     Area (CSA).
#'                     
#' @field area_code    Official Census Bureau numeric code uniquely identifying
#'                     the CBSA or CSA.
#'                     
#' @field area_level   Hierarchical classification of the CBSA, indicating
#'                     whether the area is Metropolitan or Micropolitan. CSA
#'                     entries are `NA`.
#'                     
#' @field area_name    Full official name of the statistical area as designated
#'                     by the Census Bureau (e.g., "Atlanta-Sandy Springs-
#'                     Alpharetta, GA").
#'                     
#' @field area_states  Concatenated string of state abbreviations for all
#'                     states encompassed by the statistical area boundary.
#'
#' @field geom         Spatial geometry column containing the boundary polygons
#'                     for each census unit, in the form of an `sf` geometry
#'                     object.


#' @description
#' Codebook for the core are shapefiles, encompassing ZIP Code Tabulation Areas
#' (ZCTA). Each shapefile contains separate layers representing three decennial
#' years: 2000, 2010, and 2020.
#'
#' @field vintage_year The decennial census year associated with the boundary
#'                     definition (2000, 2010, or 2020).
#'
#' @field area_type    Classification of the area as ZCTA.
#'
#' @field area_code    Official Census Bureau numeric code uniquely identifying
#'                     the ZCTA.
#'
#' @field area_states  Concatenated string of state abbreviations derived from
#'                     a spatial join with the state-level TIGER/Line Shapefile
#'                     for the respective decennial year.
#'
#' @field geom         Spatial geometry column containing the boundary polygons
#'                     for each census unit, in the form of an `sf` geometry
#'                     object.




## ----------------------------------------------------------------
## PART B: ALGORITHM TO CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA

# The complete algorithm for cleaning and validation is stored in
# "Code/Clean Raw Data_Step 2 HPC v2_2026 Format.R". It is configured to run
# either locally or on Yale's High Performance Computing (HPC) cluster. This
# algorithm completes all necessary validation and cleaning outlined in this
# step of the process and generates relevant quality control outputs at the
# point of computation.

# Algorithm Parts:
#     - LOOP PART A: Isolate Unique Candidate Addresses
#     - LOOP PART B: Consolidate and Verify the Addresses
#     - LOOP PART B.i.: Validate Addresses with USPS Database
#     - LOOP PART B.ii.: Resolve Records with No Address Match Found
#     - LOOP PART B.iii.: Agnostically Resolve Record Heterogeneity
#     - LOOP PART C: Verify Geolocation with the US Census Bureau’s Geocoder Database
#     - LOOP PART D: Point-in-Polygon Spatial Assignment of Census Information
#     - LOOP PART E: Add Back to Main Dataset
#     - LOOP PART F: Quality Checks — Address Validation and Consolidation Results
#     - LOOP PART G: Quality Checks — Variation with Geolocation
#     - LOOP PART H: Quality Checks — Variation with Census Information
#     - LOOP PART I: Commit Results

# After running the algorithm, the subsets will need to be compiled within
# a given batch run and, if multiple batches were run, across batches as
# well. This can be a nuanced process. PART C outlines how this was
# accomplished for the Summer 2026 process run.


## ----------------------------------------------------------------
## PART C: Recompile Results from the HPC

# Compute resources for batches deployed on Yale's HPC cluster are managed 
# using SLURM. Due to these resource constraints, only 25 arrays could be 
# processed concurrently.
# 
# As a result, not all arrays were processed before the USPS API transitioned
# from version 3.2.3 to 3.3.1 on August 1st. Several errors also arose during
# processing:
#   - The USPS API message-catching mechanism did not correctly handle output
#     within the context of the function interfacing with the API.
#   - Entries associated with US territory addresses were not filtered out
#     prior to validation, causing runs to halt due to failed spatial joins.
#   - Four arrays exceeded the maximum compute time allotment of one week.
# 
# Two batches were run. The subsections below describe the relevant settings
# and considerations for each. Results from both batches are then combined to
# produce the final, clean, and validated dataset.
# 
# NOTE: Normally, the data dictionary is provided at the time results are
#       written. However, because the compilation process is complex, this
#       information is instead provided in "SUBSECTION D1: Load Combined
#       Batch Results", which demonstrates the read-in process for these
#       complex DuckDB results.

# Directory containing all batch results.
data_root <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"


## --------------------
## SUBSECTION C1: Batch Array 18850425

# The dataset was partitioned into 217 arrays of up to 5,000 unique ABIs each.
# The SLURM shell script was configured to run 218 arrays with a maximum of
# 75 concurrent runs.
# 
# Script configurations enabled USPS API validation, with the maximum address
# threshold set high enough to qualify all addresses for validation. However,
# API rate limits restricted access, preventing many addresses from being
# validated despite these settings.

qc_df_18 <- compile_parquet_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_18850425/Results/Verified Result/"),
  church_dt = church_2026_form_dt,
  filter_states = FALSE,
  us_states = c(state.abb, "DC")
)

# Of the 217 queued arrays, 188 completed successfully. All expected ABIs
# within these indices were successfully processed.
table(qc_df_18$qc$abi_check$qc_pass, useNA = "ifany")

# The batch logs revealed two additional columns that are not required and can
# be removed prior to saving: usps_status and usps_status_detail. All
# validation QC columns are available in the respective QC table loaded below.
qc_df_18$data <- qc_df_18$data %>% select(-usps_status, -usps_status_detail)

# As shown in the qc_address_18 table assessment below, NA address_verified
# outcomes are attributed to missing address_line_1 entries. This case can
# be clarified for both address_verified and address_matched accordingly.
qc_df_18$data <- qc_df_18$data %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    ),
    address_verified = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_verified
    )
  ) %>%
  select(-address_prefix) %>% 
  arrow_table()


qc_address_18 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_18850425/Results/Address QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = FALSE,
  us_states     = c(state.abb, "DC")
)

# Unique reported address and year combinations are checked for correspondence
# to the raw data. No induced duplications were detected after address
# verification and matching.
table(qc_address_18$qc1$`New vs Old differ`, useNA = "ifany")

# Column names with capitalization and spaces are not easily read by DuckDB.
# These are converted here for consistent reading.
qc_address_18$qc1 <- qc_address_18$qc1 %>%
  rename(
    abi                    = ABI,
    allow_usps_api         = `Allow USPS API`,
    api_used               = `API Used`,
    verification_attempted = `Verification Attempted`,
    match_attempted        = `Match Attempted`,
    duplicates_induced     = `Duplicates Induced`,
    any_addresses_line1_na = `Any Addresses Line 1 NA`,
    new_not_in_old_addr_yr = `New not in Old addr yr`,
    old_not_in_new_addr_yr = `Old not in New addr yr`,
    new_vs_old_differ      = `New vs Old differ`
  )

# The match_attempted column was intended to reflect the presence of the
# matched_address column during analysis, indicating whether address matching
# was attempted. Instead, it was mistakenly assigned to geo_matched_address,
# which indicates whether the address found through address-based geocoding
# matched the one queried.
#
# If any ABI is present in qc3, matching was attempted; otherwise, it was not.
# Replace this column with the correct results.

qc_address_18$qc1 <- qc_address_18$qc1 %>%
  mutate(match_attempted = abi %in% unique(qc_address_18$qc3$abi))

# The initial settings for interpreting USPS API response codes did not
# correctly classify successful queries. Additionally, some shorthand
# descriptions were later determined to be misleading and were revised.
table(qc_address_18$qc2$usps_status_detail, qc_address_18$qc2$address_verified, useNA = "ifany")

# All NAs in both columns correspond and, as shown below, all NA outcomes
# correspond to a missing address_line_1. This case can be clarified in the
# address_verified column; NA in the USPS API columns is expected, as the
# address could not be queried.
qc_address_18$qc2 %>%
  filter(is.na(usps_status_detail) | is.na(address_verified)) %>%
  mutate(
    `Address is NA`              = is.na(reported_address),
    `Address starts with "NA"`   = str_starts(reported_address, "NA"),
    `Address starts with "NA,"`  = str_starts(reported_address, "NA,")
  ) %>%
  count(`Address is NA`, `Address starts with "NA"`, `Address starts with "NA,"`)

# As indicated above, "Other unanticipated errors" correspond exactly to
# cases where the address was successfully verified. The corresponding
# status values can be updated to reflect this.
table(qc_address_18$qc2$usps_status_detail, qc_address_18$qc2$usps_status, useNA = "ifany")

# Revise nomenclature and clarify vague NA cases.
qc_address_18$qc2 <- qc_address_18$qc2 %>%
  mutate(
    address_verified = case_when(
      is.na(address_verified) ~ "No address_line_1",
      TRUE                    ~ address_verified
    ),
    usps_status_detail = case_when(
      usps_status_detail == "Other unanticipated errors" ~ "200 Successful operation",
      usps_status_detail == "403 Forbidden"              ~ "403 Access denied",
      TRUE                                               ~ usps_status_detail
    ),
    usps_status = case_when(
      usps_status_detail == "200 Successful operation" ~ 200,
      TRUE                                             ~ usps_status
    )
  )

# Revise nomenclature and clarify vague NA cases.
qc_address_18$qc3 <- qc_address_18$qc3 %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    )
  ) %>%
  select(-address_prefix)


# Remove the column indicating which ABI column was used for evaluation, as
# this is redundant.
qc_address_18$qc_import <- qc_address_18$qc_import %>% select(-abi_col)


qc_census_18 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_18850425/Results/Census QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = FALSE,
  us_states     = c(state.abb, "DC")
)

# Some column names erroneously included "census" or "code". Remove for clarity.
# Additionally, the same categorizations of the "_any_match" column were not
# applied at the time of data generation. Apply them here.
qc_census_18$qc1 <- qc_census_18$qc1 %>%
  rename(
    tract_any_match = census_tract_any_match,
    block_any_match = census_block_any_match,
    fips_any_match = fips_code_any_match,
    county_any_match = county_code_any_match
  ) %>%
  mutate(
    block_match_2000 = str_detect(block_vintages, "2000"),
    block_match_2010 = str_detect(block_vintages, "2010"),
    block_match_2020 = str_detect(block_vintages, "2020"),
    block_any_match = dplyr::case_when(
      is.na(census_block)                                        ~ "Uncheckable",
      str_to_lower("block groups") != str_to_lower(block_type)   ~ "Not the expected dimensions",
      (block_match_2000 | block_match_2010 | block_match_2020)   ~ "Matched",
      TRUE                                                       ~ "None"
    )
  ) %>%
  select(-block_match_2000, -block_match_2010, -block_match_2020)

# Some column names erroneously included "code". Remove for clarity.
qc_census_18$qc2 <- qc_census_18$qc2 %>%
  rename(
    csa_any_match = csa_code_any_match,
    csa_vintages = csa_code_vintages,
    csa_vintages_aligned = csa_code_vintages_aligned
  )


qc_geo_18 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_18850425/Results/Geo QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = FALSE,
  us_states     = c(state.abb, "DC")
)

# Move query QC columns into their correct position — missed in initial ordering.
qc_geo_18$qc1 <- qc_geo_18$qc1 %>%
  relocate(matched_address_similar, .after = matched_address_same)

# Error code capture was not optimized for conciseness. These can be adjusted
# in-place to clarify outcomes.
qc_geo_18$qc1$query_statuses <- qc_geo_18$qc1$query_statuses %>%
  str_replace_all(
    "(?s)error: lexical error: invalid char in json text\\..*?\\^\\n\\s*",
    "vintage_lookup_html_response"
  ) |>
  str_replace_all(
    "(?s)error: Timeout was reached \\[geocoding\\.geo\\.census\\.gov\\]:.*?seconds\\s*",
    "vintage_lookup_timeout"
  ) %>%
  str_replace_all("vintage_lookup_http_504", "504") %>%
  str_replace_all("vintage_lookup_network_error", "network_error") %>%
  str_replace_all("vintage_lookup_html_response", "html_not_json") %>%
  str_replace_all("vintage_lookup_timeout", "request_timed_out")

# As indicated above, "Other unanticipated errors" correspond exactly to
# cases where the address was successfully verified. The corresponding
# status values can be updated to reflect this.
qc_geo_18$qc1 <- qc_geo_18$qc1 %>%
  mutate(
    address_verified = case_when(
      is.na(address_verified) ~ "No address_line_1",
      TRUE                    ~ address_verified
    )
  )

# Revise nomenclature and clarify vague NA cases.
qc_geo_18$qc1 <- qc_geo_18$qc1 %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    )
  ) %>%
  select(-address_prefix)

# The actual test compared max - min > 0.02. Update the column name to reflect
# this.
qc_geo_18$qc3 <- qc_geo_18$qc3 %>%
  rename(
    lat_spread_gt_02 = lat_spread_gt_002,
    lon_spread_gt_02 = lon_spread_gt_002
  )


## --------------------
## SUBSECTION C2: Batch Array 20823868

# The dataset was partitioned into 145 arrays of up to 1,000 unique ABIs each,
# containing only ABIs that were not successfully processed in the previous
# batch. The SLURM shell script was configured to run 146 arrays with a
# maximum of 75 concurrent runs.
# 
# USPS API validation was disabled in this batch, as the API had transitioned
# to an updated system beginning August 1st, and validation was turned off to
# avoid incurring additional costs.


# -- Determining the Arrays to Run -------------------

# Indices for batch_array_18850425
processed_indices <- sprintf(
  "%d to %d",
  seq(1, 1080001, by = 5000),
  c(seq(5000, 1080000, by = 5000), 1080764)
)

array_num <- as.integer(qc_df_18$qc$abi_check$array)

# Save the array indices as numeric values for reference in "SUBSECTION B1: 
# Index Queue" of "Clean Raw Data_Step 2 HPC v2_2026 Format.R".
array_num <- c(
  1:19, 21:24, 29:38, 40:45, 48:55, 57, 59:91, 93:105, 107:121, 123:168, 
  171:182, 186, 189:190, 193, 195, 199:201, 203, 205:206, 208:217
)

# Isolate the indices that were not successfully run and extract the
# dimensions of their corresponding arrays.
miss_chr <- processed_indices[-array_num]

mat <- stringr::str_match(miss_chr, "^\\s*(\\d+)\\s*to\\s*(\\d+)\\s*$")
miss_df <- data.frame(
  from = as.integer(mat[,2]),
  to   = as.integer(mat[,3])
)

# Define the new span of indices to be run. Reducing the index range aims to
# help the arrays that exceeded the time limit complete within the allotted
# time period.
by <- 1000

missing_subranges_1000 <- unlist(Map(function(a, b) {
  starts <- seq(a, b, by = by)
  ends   <- pmin(starts + by - 1L, b)
  sprintf("%d to %d", starts, ends)
}, miss_df$from, miss_df$to))

# The updated indices of unique ABIs to run.
missing_subranges_1000


# -- Import Results ----------------------------------

qc_df_20 <- compile_parquet_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_20823868/Results/Verified Result/"),
  church_dt = church_2026_form_dt,
  filter_states = TRUE,
  us_states = c(state.abb, "DC")
)

# Of the 145 queued arrays, 142 completed successfully. All expected ABIs
# within 135 of these indices were successfully processed. The missing indices
# or missing ABIs were the result of filtering out ABIs containing at least one 
# address in a US territory, as these were not prioritized for processing in 
# this iteration.
table(qc_df_20$qc$abi_check$qc_pass, useNA = "ifany")

# As shown in the qc_address_18 assessment above, NA address_verified outcomes
# are caused by missing address_line_1 entries. In this batch, however, no
# addresses were verified via the USPS API, so this correction is not needed.
# Only the address_matched needs to be updated accordingly.
qc_df_20$data <- qc_df_20$data %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    )
  ) %>%
  select(-address_prefix) %>% 
  arrow_table()


qc_address_20 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_20823868/Results/Address QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = TRUE,
  us_states     = c(state.abb, "DC")
)

# Column names with capitalization and spaces are not easily read by DuckDB.
# These are converted here for consistent reading.
qc_address_20$qc1 <- qc_address_20$qc1 %>%
  rename(
    abi                    = ABI,
    allow_usps_api         = `Allow USPS API`,
    api_used               = `API Used`,
    verification_attempted = `Verification Attempted`,
    match_attempted        = `Match Attempted`,
    duplicates_induced     = `Duplicates Induced`,
    any_addresses_line1_na = `Any Addresses Line 1 NA`,
    new_not_in_old_addr_yr = `New not in Old addr yr`,
    old_not_in_new_addr_yr = `Old not in New addr yr`,
    new_vs_old_differ      = `New vs Old differ`
  )

# The match_attempted column was intended to reflect the presence of the
# matched_address column during analysis, indicating whether address matching
# was attempted. Instead, it was mistakenly assigned to geo_matched_address,
# which indicates whether the address found through address-based geocoding
# matched the one queried.
#
# If any ABI is present in qc3, matching was attempted; otherwise, it was not.
# Replace this column with the correct results.

qc_address_20$qc1 <- qc_address_20$qc1 %>%
  mutate(match_attempted = abi %in% unique(qc_address_20$qc3$abi))

# Revise nomenclature and clarify vague NA cases.
qc_address_20$qc3 <- qc_address_20$qc3 %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    )
  ) %>%
  select(-address_prefix)


qc_census_20 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_20823868/Results/Census QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = TRUE,
  us_states     = c(state.abb, "DC")
)

# Some column names erroneously included "census" or "code". Remove for clarity.
# Additionally, the same categorizations of the "_any_match" column were not
# applied at the time of data generation. Apply them here.
qc_census_20$qc1 <- qc_census_20$qc1 %>%
  rename(
    tract_any_match = census_tract_any_match,
    block_any_match = census_block_any_match,
    fips_any_match = fips_code_any_match,
    county_any_match = county_code_any_match
  ) %>%
  mutate(
    block_match_2000 = str_detect(block_vintages, "2000"),
    block_match_2010 = str_detect(block_vintages, "2010"),
    block_match_2020 = str_detect(block_vintages, "2020"),
    block_any_match = dplyr::case_when(
      is.na(census_block)                                        ~ "Uncheckable",
      str_to_lower("block groups") != str_to_lower(block_type)   ~ "Not the expected dimensions",
      (block_match_2000 | block_match_2010 | block_match_2020)   ~ "Matched",
      TRUE                                                       ~ "None"
    )
  ) %>%
  select(-block_match_2000, -block_match_2010, -block_match_2020)

# Some column names erroneously included "code". Remove for clarity.
qc_census_20$qc2 <- qc_census_20$qc2 %>%
  rename(
    csa_any_match = csa_code_any_match,
    csa_vintages = csa_code_vintages,
    csa_vintages_aligned = csa_code_vintages_aligned
  )


qc_geo_20 <- compile_duckdb_folder(
  subset_dir    = file.path(getwd(), data_root, "batch_array_20823868/Results/Geo QC/"),
  abi_ref       = unique(church_2026_form_dt$abi),
  church_dt     = church_2026_form_dt,
  filter_states = TRUE,
  us_states     = c(state.abb, "DC")
)

# Move query QC columns into their correct position — missed in initial ordering.
qc_geo_20$qc1 <- qc_geo_20$qc1 %>%
  relocate(matched_address_similar, .after = matched_address_same)

# Error code capture was not optimized for conciseness. These can be adjusted
# in-place to clarify outcomes.
qc_geo_20$qc1$query_statuses <- qc_geo_20$qc1$query_statuses %>%
  str_replace_all(
    "(?s)error: lexical error: invalid char in json text\\..*?\\^\\n\\s*",
    "vintage_lookup_html_response"
  ) |>
  str_replace_all(
    "(?s)error: Timeout was reached \\[geocoding\\.geo\\.census\\.gov\\]:.*?seconds\\s*",
    "vintage_lookup_timeout"
  ) %>%
  str_replace_all("vintage_lookup_http_504", "504") %>%
  str_replace_all("vintage_lookup_network_error", "network_error") %>%
  str_replace_all("vintage_lookup_html_response", "html_not_json") %>%
  str_replace_all("vintage_lookup_timeout", "request_timed_out")

# Revise nomenclature and clarify vague NA cases.
qc_geo_20$qc1 <- qc_geo_20$qc1 %>%
  collect() %>%
  mutate(
    address_prefix = str_extract(address, "^[^,]+"),
    address_matched = if_else(
      address_prefix == "NA",
      "No address_line_1",
      address_matched
    )
  ) %>%
  select(-address_prefix)


# The actual test compared max - min > 0.02. Update the column name to reflect
# this.
qc_geo_20$qc3 <- qc_geo_20$qc3 %>%
  rename(
    lat_spread_gt_02 = lat_spread_gt_002,
    lon_spread_gt_02 = lon_spread_gt_002
  )


## --------------------
## SUBSECTION C3: Combining the Batches

# The next step combines results across batches alongside their respective
# batch import QC outputs. This is memory-intensive and may cause issues on
# some machines.
# 
# To ensure results are written cleanly, run this step with minimal other
# objects in the global environment after a recent session restart. Writing
# confirmations are included here for less consistent processes; confirmations
# for more consistent processes are in "PART D: Assess Overall Performance".


# -- Combine Main Dataset ----------------------------

# Define the output folder for compiled batch artifacts and ensures it exists.
out_dir <- normalizePath(file.path(data_root, "Compiled by Batches"), winslash = "/", mustWork = FALSE)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# Define the DuckDB database file path (created/overwritten on write).
out_db <- normalizePath(file.path(out_dir, "church_2026_form_validated_08.03.2026.db"), winslash = "/", mustWork = FALSE)

# Create parquet staging directories for each batch and normalizes paths.
p18 <- normalizePath({p <- file.path(out_dir, "main_df_18_parquet"); dir.create(p, recursive=TRUE, showWarnings=FALSE); p},
                     winslash="/", mustWork=FALSE)
p20 <- normalizePath({p <- file.path(out_dir, "main_df_20_parquet"); dir.create(p, recursive=TRUE, showWarnings=FALSE); p},
                     winslash="/", mustWork=FALSE)

# Write large datasets to parquet to avoid binding in RAM; DuckDB reads from 
# parquet directly.
arrow::write_dataset(qc_df_18$data, p18, format = "parquet")
arrow::write_dataset(qc_df_20$data, p20, format = "parquet")

# Open a writable DuckDB connection to the target database file.
con <- dbConnect(duckdb::duckdb(), dbdir = out_db, read_only = FALSE)

# Build/refresh "data" by unioning parquet batches (align by name) and sorting 
# by ABI then archive_version_year (asc; NULLs last).
dbExecute(con, sprintf("
  CREATE OR REPLACE TABLE data AS
  SELECT *
  FROM (
    SELECT * FROM read_parquet('%s/**/*.parquet')
    UNION ALL BY NAME
    SELECT * FROM read_parquet('%s/**/*.parquet')
  )
  ORDER BY
    abi ASC,
    archive_version_year ASC NULLS LAST;
", p18, p20))

# Write QC outputs as separate tables named qc_df_18__* and qc_df_20__* (skips 
# NULLs without templates).
write_qc_groups(
  con,
  list(
    qc_df_18 = qc_df_18$qc,
    qc_df_20 = qc_df_20$qc
  ),
  prefixes = c(qc_df_18 = "import_qc_18", qc_df_20 = "import_qc_20"),
  on_missing = "skip",
  verbose    = TRUE
)

# Closes the connection and persists changes to disk.
dbDisconnect(con, shutdown = TRUE)


# Confirm that the DuckDB file was written correctly by:
#    1) connecting to the file
#    2) listing tables
#    3) counting rows
#    4) previewing the data table

# Open a writable DuckDB connection to the target database file.
con <- dbConnect(duckdb::duckdb(), dbdir = out_db, read_only = TRUE)

# Print the attached database(s) and file path(s) to confirm the connection 
# points to the expected file.
dbGetQuery(con, "PRAGMA database_list;")

# Lists all tables in the database. Expect one "data" table containing all
# results and six import QC tables per batch. Batches are differentiated by
# the first two digits of the batch number.
tabs <- dbListTables(con)
print(tabs)

# Computes row counts for each table (quick check that tables exist and are not 
# unexpectedly empty).
counts <- bind_rows(lapply(tabs, function(t) {
  dbGetQuery(con, sprintf(
    "SELECT %s AS table_name, COUNT(*) AS n FROM %s;",
    dbQuoteString(con, t),
    dbQuoteIdentifier(con, t)
  ))
}))

# Prints the per-table row counts.
print(counts)

# Confirm the compiled dataset table exists and contains rows (n = 10,157,989).
dbGetQuery(con, "SELECT COUNT(*) AS n FROM data;")

# Display a small preview of the compiled dataset.
dbGetQuery(con, "SELECT * FROM data LIMIT 5;")

# Close the connection cleanly and shuts down DuckDB so the file is fully released.
dbDisconnect(con, shutdown = TRUE)
try(duckdb::duckdb_shutdown(), silent = TRUE)

# If all checks look correct, any temporary write/staging directories (e.g., 
# parquet folders), any intermediate database files (e.g., "combined.duckdb"), 
# and any DuckDB WAL sidecar files (e.g., "*.db.wal") can be safely deleted.


# -- Combine Address QC ------------------------------

required_bind <- c("qc1", "qc2", "qc3")
required_keep <- "qc_import"

# Ensure required names exist (missing become NULL).
for (nm in c(required_bind, required_keep)) {
  if (!nm %in% names(qc_address_18)) qc_address_18[[nm]] <- NULL
  if (!nm %in% names(qc_address_20)) qc_address_20[[nm]] <- NULL
}

# Build output list: bind algorithm qc1-3, keep qc_import split by batch.
qc_address_out <- list()

for (nm in required_bind) {
  qc_address_out[[nm]] <- rbind_qc(qc_address_18[[nm]], qc_address_20[[nm]])
}

qc_address_out[["import_qc_18"]] <- qc_address_18[["qc_import"]]
qc_address_out[["import_qc_20"]] <- qc_address_20[["qc_import"]]

# Ensure each element is a data.frame (DuckDB tables need tabular) .
qc_address_out_tbls <- lapply(qc_address_out, function(x) {
  if (is.null(x)) data.frame(.empty = character())[0, , drop = FALSE] else as.data.frame(x)
})

# Write combined QC outputs as unified tables and batch import QC outputs as
# separate tables named import_qc_*.
out_db <- file.path(data_root, "Compiled by Batches", "address_qc_08.05.2026.db")
write_list_to_duckdb(
  lst = qc_address_out_tbls,
  path = out_db,
  table_names = names(qc_address_out_tbls),
  overwrite = TRUE
)

# Any temporary intermediate database files (e.g., "combined.duckdb") or 
# DuckDB WAL sidecar files (e.g., "*.db.wal") can be safely deleted.


# -- Combine Census QC -------------------------------

required_bind <- c("qc1", "qc2")
required_keep <- "qc_import"

# Ensure required names exist (missing become NULL).
for (nm in c(required_bind, required_keep)) {
  if (!nm %in% names(qc_census_18)) qc_census_18[[nm]] <- NULL
  if (!nm %in% names(qc_census_20)) qc_census_20[[nm]] <- NULL
}

# Build output list: bind algorithm qc1-2, keep qc_import split by batch.
qc_census_out <- list()
for (nm in required_bind) {
  qc_census_out[[nm]] <- rbind_qc(qc_census_18[[nm]], qc_census_20[[nm]])
}

qc_census_out[["import_qc_18"]] <- qc_census_18[["qc_import"]]
qc_census_out[["import_qc_20"]] <- qc_census_20[["qc_import"]]

# Ensure each element is a data.frame (DuckDB tables need tabular) .
qc_census_out_tbls <- lapply(qc_census_out, function(x) {
  if (is.null(x)) data.frame(.empty = character())[0, , drop = FALSE] else as.data.frame(x)
})

# Write combined QC outputs as unified tables and batch import QC outputs as
# separate tables named import_qc_*.
out_db <- file.path(data_root, "Compiled by Batches", "census_qc_08.06.2026.db")
write_list_to_duckdb(
  lst = qc_census_out_tbls,
  path = out_db,
  table_names = names(qc_census_out_tbls),
  overwrite = TRUE
)

# Any temporary intermediate database files (e.g., "combined.duckdb") or 
# DuckDB WAL sidecar files (e.g., "*.db.wal") can be safely deleted.


# -- Combine Geo QC ----------------------------------

required_bind <- c("qc1", "qc2", "qc3")
required_keep <- "qc_import"

# Ensure required names exist (missing become NULL).
for (nm in c(required_bind, required_keep)) {
  if (!nm %in% names(qc_geo_18)) qc_geo_18[[nm]] <- NULL
  if (!nm %in% names(qc_geo_20)) qc_geo_20[[nm]] <- NULL
}

# Build output list: bind algorithm qc1-2, keep qc_import split by batch.
qc_geo_out <- list()
for (nm in required_bind) {
  qc_geo_out[[nm]] <- rbind_qc(qc_geo_18[[nm]], qc_geo_20[[nm]])
}

qc_geo_out[["import_qc_18"]] <- qc_geo_18[["qc_import"]]
qc_geo_out[["import_qc_20"]] <- qc_geo_20[["qc_import"]]

# Ensure each element is a data.frame (DuckDB tables need tabular) .
qc_geo_out_tbls <- lapply(qc_geo_out, function(x) {
  if (is.null(x)) data.frame(.empty = character())[0, , drop = FALSE] else as.data.frame(x)
})

# Write combined QC outputs as unified tables and batch import QC outputs as
# separate tables named import_qc_*.
out_db <- file.path(data_root, "Compiled by Batches", "geo_qc_08.06.2026.db")
write_list_to_duckdb(
  lst = qc_geo_out_tbls,
  path = out_db,
  table_names = names(qc_geo_out_tbls),
  overwrite = TRUE
)

# Any temporary intermediate database files (e.g., "combined.duckdb") or 
# DuckDB WAL sidecar files (e.g., "*.db.wal") can be safely deleted.



## ----------------------------------------------------------------
## PART D: Assess Overall Performance

# If results were recently compiled in "PART C: Recompile Results from the HPC"
# then it is possible the available compute RAM is limited. Before proceeding
# with this section, restart the session and import only the datasets required.
# 
# NOTE: In "SUBSECTION D2: Confirm Complete ABI Coverage" the tests require
#       both the raw data and validated data imported simultaneously. These
#       are both very large and the validation may max available RAM. Results
#       are reported in the comments for those who cannot run these locally.

# Directory containing all batch results.
data_root <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"


## --------------------
## SUBSECTION D1: Load Combined Batch Results

# Only import the complete validated dataset if you plan to run the tests in
# "SUBSECTION D2: Confirm Complete ABI Coverage". All other QC results have
# been precompiled in their respective macro-algorithm cleaning and validation
# steps below: address, census, and geo.

#' @description
#' Codebook for new output fields produced during the data cleaning and
#' validation step. All other fields were present in the Step 1 form of
#' the data.
#'
#' @field address Best-available address, derived by using verified_address 
#'                (trimmed; excluding "No address match found"), else 
#'                matched_address, else combined_address.
#'
#' @field address_verified If the given address was verified against the USPS
#'                         API, this field contains the verified result. If no
#'                         match is found, the value is reported as NA.
#'
#' @field address_matched If agnostic address matching was performed, this
#'                        field contains the result if the match succeeded via 
#'                        either "Exact matching" or "Fuzzy matching". "Only
#'                        one address" represents cases where matching was 
#'                        attempted but only one address was reported. "No
#'                        address_line_1" represents cases where matching was
#'                        not possible, otherwise NA.
#'
#' @field reported_address The original reported address, prior to any cleaning 
#'                         or verification.
#'
#' @field longitude/latitude Best-available geocoordinates, using verified
#'                           coordinates when available, otherwise falling back
#'                           to averaged raw coordinates.
#'                           
#' @field geolocation_verified 
#'
#' @field geoid_[2000|2010|2020] Full concatenated census GEOID assigned to the
#'                               record via point-in-polygon spatial join with 
#'                               the block-group level TIGER/Line  Shapefile for 
#'                               the respective decennial year.
#'
#' @field geoid_match Summary outcome of the GEOID spatial assignment across all 
#'                    decennial years. One of: "Matched" (all decennial years 
#'                    assigned), "Some matches not found" (partial assignment), 
#'                    "Matches not found" (no years assigned), or "Not enough 
#'                    info" (insufficient coordinate data to attempt assignment).
#'
#' @field cbsa_code_[2007|2010|2020] Official Census Bureau numeric code 
#'                                   identifying the CBSA assigned to the 
#'                                   record via point-in-polygon spatial join 
#'                                   for the respective vintage year.
#'
#' @field cbsa_level_[2007|2010|2020] Hierarchical classification of the 
#'                                    assigned CBSA, indicating whether the 
#'                                    area is Metropolitan or Micropolitan, for 
#'                                    the respective vintage year.
#'                                    
#' @field csa_code_[2007|2010|2020] Official Census Bureau numeric code 
#'                                  identifying the CSA assigned to the record
#'                                  via point-in-polygon spatial join for the
#'                                  respective vintage year.
#'
#' @field zcta_[2000|2010|2020] Five-digit ZCTA code assigned to the record via
#'                              point-in-polygon spatial join with the 
#'                              ZCTA-level TIGER/Line Shapefile for the 
#'                              respective decennial year.

church_2026_form_validated <- import_church_db(
  db_path = file.path(data_root, "Compiled by Batches", "church_2026_form_validated_08.03.2026.db"),
  import_data = "data"
)


# Otherwise, import the QC results, which cover algorithm cleaning and
# validation as well as high-level batch result checks. List elements
# containing "import" pertain to the latter.

#' @description
#' The main dataset was compiled from multiple HPC batches. At the time of
#' import, a series of quality checks were run to confirm ABI coverage against
#' the Step 1 data and summarize key validation outcomes.
#'
#' This codebook documents the list items produced by the quality check
#' algorithm across the following fields: \code{address_verified},
#' \code{address_matched}, \code{geoid_match}, \code{geolocation_verified},
#' and the distribution of \code{NA}s across all census boundary columns.
#' Each batch's import results are annotated with the first two digits of
#' the batch number (\code{<XX>}).
#'
#' @name import_qc_<XX>$abi_check
#'
#' @field array The batch array number the results come from.
#' @field file The file name the results come from.
#' @field from/to The ABI search space index range defined under "SUBSECTION B1: 
#'                Index Queue" in \file{Clean Raw Data_Step 2 HPC v2_2026 Format.R}.
#' @field n_expected_unique The number of unique ABIs expected in the Step 1
#'                          data for the given index range.
#' @field n_actual_unique The number of unique ABIs present in the batch results 
#'                        for the given index range.
#' @field n_missing The number of ABIs absent from the batch results, computed 
#'                  as the length of \code{setdiff()} between the expected and 
#'                  actual ABIs.
#' @field qc_pass Boolean. TRUE if \code{n_missing == 0}, otherwise FALSE.
#' 
#' 
#' @name import_qc_<XX>$[address_matched|address_verified|geoid_match|geolocation_verified]
#' 
#' @field array The batch array number the results come from.
#' @field value The unique outcome observed for that column (e.g., 
#'              \code{"Exact match"}).
#' @field n The number of records with that outcome.
#' @field n_addr The total number of addresses in that array.
#' @field pct The percentage of addresses with that outcome.
#' 
#' 
#' @name import_qc_<XX>$na_census_boundaries
#' 
#' @field array The batch array number the results come from.
#' @field column The variable being checked for \code{NA} values.
#' @field n_addr The total number of addresses in that array.
#' @field n_na The number of \code{NA} entries observed.
#' @field pct_na The percentage of addresses with a \code{NA} for that census 
#'               boundary column.

church_2026_form_validated_import_qc <- import_church_db(
  db_path = file.path(data_root, "Compiled by Batches", "church_2026_form_validated_08.03.2026.db"),
  import_data = "qc"
)


# Import the QC datasets generated by the cleaning and validation algorithms
# at the time of processing. The import framework and import metrics are
# provided here, but the quality assessment for the cleaning and validation
# method is documented in the separate "Preparation Step 2 QC_2026 Format"
# PDF report.

#' @description
#' The three quality control datasets — \code{address_qc}, \code{census_qc},
#' and \code{geo_qc} — collected at the time of import were compiled from
#' multiple HPC batches. A series of quality checks were run to confirm ABI
#' coverage against the Step 1 data. Each batch's results are annotated with
#' the first two digits of the batch number (\code{<XX>}).
#'
#' @name import_qc_<XX>
#'
#' @field array The batch array number the results come from.
#' @field file The file name the results come from.
#' @field key The quality check list (e.g., \code{qc1}, \code{qc2}) within
#'            that file the results come from.
#' @field from/to The ABI search space index range defined under "SUBSECTION B1:
#'                Index Queue" in \file{Clean Raw Data_Step 2 HPC v2_2026 Format.R}.
#' @field n_expected_unique The number of unique ABIs expected in the Step 1
#'                          data for the given index range.
#' @field n_actual_unique The number of unique ABIs present in the batch results
#'                        for the given index range.
#' @field n_missing The number of ABIs absent from the batch results, computed
#'                  as the length of \code{setdiff()} between the expected and
#'                  actual ABIs.
#' @field qc_pass Boolean. TRUE if \code{n_missing == 0}, otherwise FALSE.


#' @description
#' Three quality checks were conducted over the address validation and matching
#' process. QC1: General indicator whether API verification was allowed, which 
#' ABIs were verified, whether any addresses were matched, and two checks for 
#' duplications or other introductions/loss of results. QC2: Summarizes all
#' quality check outcomes from address validation using the USPS API over unique
#' ABIs and addresses. QC3: Summarizes all quality check outcomes from address
#' matching over unique ABIs and addresses.
#'
#' @name qc1
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field allow_usps_api Boolean. TRUE if any USPS API usage was allowed during
#'                       that array run via \code{verify_addresses}, otherwise
#'                       FALSE.
#'                       
#' @field api_used Boolean. TRUE if the entries for that ABI are configured
#'                 for USPS API usage via \code{do_api}, otherwise FALSE.
#'                 
#' @field verification_attempted Boolean. TRUE if \code{verified_address}
#'                               is present. Results reiterate \code{do_api}
#'                               and should match.
#'                               
#' @field match_attempted Boolean. TRUE if agnostic matching was attempted
#'                        for any addresses for that ABI, indicated by the
#'                        presence of the \code{matched_address} column.
#'                        
#' @field duplicates_induced Boolean. TRUE if any duplicates over
#'                           \code{combined_address}, \code{anchor_year_min},
#'                           and \code{anchor_year_max} are detected, otherwise
#'                           FALSE.
#'                           
#' @field any_addresses_line1_na Boolean. TRUE if any \code{address_line_1}
#'                               for that ABI is \code{NA}, otherwise FALSE.
#'                               
#' @field new_not_in_old_addr_yr The function \code{compare_tabs()} checks whether
#'                               any unique \code{combined_address} and
#'                               \code{archive_version_year} outcomes between the
#'                               cleaned and Step 1 data are the same. This field
#'                               indicates whether the cleaned data has any
#'                               outcomes not present in the Step 1 standardized 
#'                               version.
#'                               
#' @field old_not_in_new_addr_yr The inverse of \code{new_not_in_old_addr_yr}.
#'                               Indicates whether the Step 1 standardized data
#'                               has any outcomes not present in the cleaned data.
#'                               
#' @field new_vs_old_differ Boolean. TRUE if the processed data differed
#'                          at all from the Step 1 standardized form, otherwise
#'                          FALSE.
#' 
#' 
#' @name qc2
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field reported_address The original reported address, prior to any cleaning
#'                         or verification.
#'                         
#' @field address_verified If the given address was verified against the USPS
#'                         API, this field is TRUE. If it is unverified but a
#'                         match is found, the value indicates the mode of
#'                         string matching: \code{"Override"} or \code{"Exact"}.
#'                         If no \code{address_line_1} was present for matching
#'                         this is indicated with \code{"No address_line_1"}.
#'                         
#' @field ver_geolocation_test Geolocation test for matching attempts to verified
#'                             addresses. \code{"Override"} if \code{"Exact"}
#'                             matched, TRUE/FALSE if \code{"Fuzzy"} matched.
#'                             \code{NA} if no match was attempted.
#'                             
#' @field usps_status Numeric summarizing the API query interaction
#'                    (e.g. \code{200}, \code{400}).
#'                    
#' @field usps_status_detail Status description for the above code (e.g.
#'                           \code{"200 Successful operation"}).
#'                           
#' @field attempt_succeeded Indicates if the verification attempt succeeded on
#'                          the first try or via the fallback, or if the attempt
#'                          failed entirely.
#'                          
#' @field verified_address The verified address result from the USPS API query.
#'  
#'  
#' @name qc3
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using verified_address 
#'                (trimmed; excluding "No address match found"), else 
#'                matched_address, else combined_address.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field reported_address The original reported address, prior to any cleaning 
#'                         or verification.
#'                         
#' @field address_matched If agnostic address matching was performed, this
#'                        field contains the result if the match succeeded via 
#'                        either "Exact matching" or "Fuzzy matching". "Only
#'                        one address" represents cases where matching was 
#'                        attempted but only one address was reported. "No
#'                        address_line_1" represents cases where matching was
#'                        not possible, otherwise NA.
#'                        
#' @field match_geolocation_test Geolocation test for agnostic matching attempts.
#'                               \code{"Override"} if \code{"Exact"} matched,
#'                               \code{PASS}/\code{FAIL} if \code{"Fuzzy"}
#'                               matched. If \code{PASS} includes \code{"no Lon"}, 
#'                               \code{"no Lat"}, or \code{"no Lon/Lat"}, this 
#'                               indicates one pair was missing that geolocation. 
#'                               \code{NA} if no match was attempted.
#'                               
#' @field matched_address The matched address result.

address_qc <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "address_qc_08.05.2026.db"))


#' @description
#' Two quality checks were conducted over the census boundary verification
#' process. QC1 and QC2 apply the same test over different columns, verifying
#' whether the reported census boundaries correspond with any of the spatial
#' join results, and whether the matched vintages align with the dates that
#' address was filed.
#'
#' @name qc1
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field census_[block|tract]/[county|fips]_code The original reported census
#'                                                boundaries recorded for that
#'                                                address.
#'                                                
#' @field n_address The number of times that ABI and address combination were
#'                  recorded.
#'                  
#' @field geoid_[2000|2010|2020] Full concatenated census GEOID assigned to the
#'                               record via point-in-polygon spatial join with
#'                               the block-group level TIGER/Line Shapefile for
#'                               the respective decennial year.
#'                               
#' @field `[fips|county|tract|block]_code_any_match` Checks if any of the original
#'                                                   values correspond with any
#'                                                   of the decennial outcomes
#'                                                   annotated using spatial
#'                                                   joining. \code{"Matched"} if
#'                                                   any do, \code{"Uncheckable"}
#'                                                   if there was no original
#'                                                   value to compare, \code{"None"}
#'                                                   if no matches.
#'                                                   
#' @field `[fips|county|tract|block]_vintages` Lists the vintages that were
#'                                             matched. \code{"None"} if none
#'                                             matched, or \code{"Not reported"}
#'                                             if no original value was reported.
#'                                             
#' @field `[fips|county|tract|block]_vintages_aligned` Uses the function
#'                                                     \code{check_alignment()}
#'                                                     to assess if the vintages
#'                                                     matched align with the
#'                                                     dates that address was
#'                                                     reported. \code{TRUE} if
#'                                                     so, otherwise \code{FALSE}.
#'                                                     \code{NA} if no comparison
#'                                                     was available.
#'                                                     
#' @field block_type Identifies the level of the block census boundary reported:
#'                   \code{"None reported"} if missing, \code{"Blocks"} if four
#'                   digits, \code{"Block groups"} if one digit, or
#'                   \code{"Not the expected dimensions"} if neither.
#' 
#' 
#' @name qc2
#' 
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field n_address The number of times that ABI and address combination were
#'                  recorded.
#'                                 
#' @field cbsa_[level|code]/csa_code The original reported census boundaries 
#'                                   recorded for that address.
#' 
#' @field cbsa_code_[2007|2010|2020] Official Census Bureau numeric code 
#'                                   identifying the CBSA assigned to the 
#'                                   record via point-in-polygon spatial join 
#'                                   for the respective vintage year.
#'
#' @field cbsa_level_[2007|2010|2020] Hierarchical classification of the 
#'                                    assigned CBSA, indicating whether the 
#'                                    area is Metropolitan or Micropolitan, for 
#'                                    the respective vintage year.
#'                                    
#' @field csa_code_[2007|2010|2020] Official Census Bureau numeric code 
#'                                  identifying the CSA assigned to the record
#'                                  via point-in-polygon spatial join for the
#'                                  respective vintage year.
#'
#' @field `cbsa_[level|code]_code_any_match/csa_code_code_any_match` 
#'                  Checks if any of the original values correspond with any
#'                  of the decennial outcomes annotated using spatial joining. 
#'                  \code{"Matched"} if any do, \code{"Uncheckable"} if there 
#'                  was no original value to compare, \code{"None"} if no matches.
#'                                                   
#' @field `cbsa_[level|code]_vintages/csa_code_vintages` 
#'                  Lists the vintages that were matched. \code{"None"} if none
#'                  matched, or \code{"Not reported"} if no original value was 
#'                  reported.
#'                                             
#' @field `cbsa_[level|code]_vintages_aligned/csa_code_vintages_aligned`  
#'                  Uses the function \code{check_alignment()} to assess if the 
#'                  vintages matched align with the dates that address was
#'                  reported. \code{TRUE} if so, otherwise \code{FALSE}. 
#'                  \code{NA} if no comparison was available.

census_qc <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "census_qc_08.06.2026.db"))


#' @description
#' Three quality checks were conducted over the geocoordinate verification 
#' process: QC1: Summarizes all quality check outcomes from address-based 
#' geocoding validation using the US Census Bureau over unique ABIs and 
#' addresses. QC2: Assesses the difference between the reported and validated 
#' geocoordinates. QC3: Examines the spread of geocoordinates among all records 
#' sharing the same ABI and address, and indicates whether this summary includes 
#' records where the address was verified or agnostically matched.
#'
#' @name qc1
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field n_address The number of times that ABI and address combination were
#'                  recorded.
#'                  
#' @field enough_geo Boolean. TRUE if both longitude and latitude were reported
#'                   otherwise FALSE.
#'                   
#' @field geolocation_verified Boolean. TRUE if an address-based geocoding
#'                             match was retrieved from the US Census Bureau
#'                             Geocoder API, otherwise FALSE.
#'                             
#' @field n_attempts The number of API query attempts made. Each uses a different
#'                   benchmark and vintage for the query URL.
#'                   
#' @field query_statuses Summary of the query statuses returned. \code{200}
#'                       indicates the interaction was successful, even if no
#'                       match was found. Each attempt is separated by a
#'                       vertical bar.
#'                       
#' @field all_200 Boolean. TRUE if all benchmark and vintage pairs returned a
#'                status of \code{200}, indicating no match could be found but
#'                the API interaction succeeded, otherwise FALSE.
#'               
#' @field geo_matched_address The resulting matched address found in the database.
#' 
#' @field matched_address_same Uses \code{find_similar_addresses(threshold = 0)}
#'                             to check if the addresses are exactly the same.
#'                             
#' @field matched_address_similar Uses \code{find_similar_addresses(threshold = 0.15)}
#'                                to check if the addresses are similar.
#'                                
#' @field benchmark/vintage_input The specific reference database used for
#'                                searching for an address match during geocoding.
#'                                
#' @field address_verified If the given address was verified against the USPS
#'                         API, this field is TRUE. If it is unverified but a
#'                         match is found, the value indicates the mode of
#'                         string matching: \code{"Override"} or \code{"Exact"}.
#'                         If no \code{address_line_1} was present for matching
#'                         this is indicated with \code{"No address_line_1"}.
#'                         
#' @field ver_geolocation_test Geolocation test for matching attempts to verified
#'                             addresses. \code{"Override"} if \code{"Exact"}
#'                             matched, TRUE/FALSE if \code{"Fuzzy"} matched.
#'                             \code{NA} if no match was attempted.
#'                             
#' @field address_matched If agnostic address matching was performed, this
#'                        field contains the result if the match succeeded via 
#'                        either "Exact matching" or "Fuzzy matching". "Only
#'                        one address" represents cases where matching was 
#'                        attempted but only one address was reported. "No
#'                        address_line_1" represents cases where matching was
#'                        not possible, otherwise NA.
#'                        
#' @field match_geolocation_test Geolocation test for agnostic matching attempts.
#'                               \code{"Override"} if \code{"Exact"} matched,
#'                               \code{PASS}/\code{FAIL} if \code{"Fuzzy"}
#'                               matched. If \code{PASS} includes \code{"no Lon"}, 
#'                               \code{"no Lat"}, or \code{"no Lon/Lat"}, this 
#'                               indicates one pair was missing that geolocation. 
#'                               \code{NA} if no match was attempted.
#' 
#' 
#' @name qc2
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field archive_versions_present String listing all the years that address
#'                                 was observed.
#'                                 
#' @field `[lat|lon]_abs_diff` The absolute difference between the reported
#'                             geocoordinate and the validated one.
#'                             
#' @field `[lat|lon]_gt_002` Boolean. TRUE if the absolute difference exceeds
#'                           0.002 degrees, otherwise FALSE. NA if there were
#'                           no geocoordinates to compare.
#' 
#' 
#' @name qc3
#'
#' @field i Results produced by the for loop are saved as lists. \code{i}
#'          indicates which iteration of the loop the results are from.
#'          
#' @field abi The business ID the results relate to.
#' 
#' @field address Best-available address, derived by using \code{verified_address}
#'                (trimmed; excluding "No address match found"), else
#'                \code{matched_address}, else \code{combined_address}.
#'                
#' @field n_address The number of times that ABI and address combination were
#'                  recorded.
#'                                 
#' @field `[lat|lon]_[min|q1|median|mean|q3|max]` Summary statistics describing
#'                                                 the spread of geocoordinates
#'                                                 reported for a unique address.
#'                                                 
#' @field `[lat|lon]_spread_gt_02` Boolean. TRUE if the spread of geocoordinates
#'                                 exceeds 0.02 degrees, otherwise FALSE. NA if
#'                                 there were no geocoordinates to compare.
#'                                 
#' @field any_address_verified Boolean. TRUE if any of the addresses reflected
#'                             in these results came from a verified address.
#'                             NA if no validation was used.
#'                             
#' @field any_address_matched Boolean. TRUE if any of the addresses reflected
#'                            in these results came from an agnostically matched
#'                            address. NA if no matching was applied.

geo_qc <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "geo_qc_08.06.2026.db"))


## --------------------
## SUBSECTION D2: Confirm Complete ABI Coverage

# All ABIs in the validated dataset are present in the original raw dataset.
all(unique(church_2026_form_validated$data$abi) %in% unique(church_2026_form$abi))

# However, not all ABIs in the raw dataset are retained after validation.
all(unique(church_2026_form$abi) %in% unique(church_2026_form_validated$data$abi))

# During validation, only ABIs located in the US, excluding US territories,
# were processed. We can confirm that all "missing" ABIs correspond exactly
# to ABIs containing at least one address outside of the US.
missing_abi <- unique(church_2026_form$abi)[unique(church_2026_form$abi) %!in% unique(church_2026_form_validated$data$abi)]
us_states   <- c(state.abb, "DC")

# Confirm that every business missing from the validation dataset has, at some 
# point, an address with a non-U.S. state value. Results confirm this is true.
church_2026_form_dt[abi %chin% missing_abi,
                    .(ok = all(state %chin% us_states)),
                    by = .(abi),
                    drop = FALSE
][, table(ok, useNA = "ifany")]  # all FALSE

# Confirm that every business present in both datasets never had an address with 
# a non-U.S. state value. Results confirm this is true.
church_2026_form_dt[!(abi %chin% missing_abi),
                    .(ok = all(state %chin% us_states)),
                    by = .(abi),
                    drop = FALSE
][, table(ok, useNA = "ifany")]  # all TRUE


# Earlier it was identified that missing geocoordinates might be recovered
# through address-based geocoding or aggregation or matching to an address 
# with valid geocoordinates. This quantifies the proportion of results that 
# still have NAs for both geocoordinates.

# Need to make compute space to generate a table form of the validated dataset.
rm(church_2026_form)
rm(church_2026_form_dt)

church_2026_form_validated_dt <- as.data.table(church_2026_form_validated$data)  # Convert for efficient data manipulation
setorder(church_2026_form_validated_dt, abi)  # Organize the table by abi

# Quantify the proportion of missing geocoordinates. 0.03% were identified as
# missing. This is a reduction from 0.39% before completing the data cleaning
# and validation.
church_2026_form_validated_dt[
  ,
  .(
    n = .N,
    na_lon               = any(is.na(longitude)),
    na_lat               = any(is.na(latitude))
  ),
  by = abi
][, round(prop.table(table(na_lon, na_lat, useNA = "ifany"))*100, digits = 2)]


## --------------------
## SUBSECTION D3: Plot the Distribution of Quality Check Outcomes

# A limited set of quality check columns reflecting the salient results from
# each stage of the algorithm were saved in the main dataset and summarized
# into results by batch array at import.

import_qc_18 <- church_2026_form_validated_import_qc$import_qc_18
import_qc_20 <- church_2026_form_validated_import_qc$import_qc_20


# All expected ABIs are present. Note that this assessment considers all ABIs
# in the raw data, parsed by ABI across each array.
table(import_qc_18$abi_check$qc_pass, useNA = "ifany")

# Scatterplots summarizing outcome distributions across each batch array.
p_address_18     <- flag_boxplot(import_qc_18$address_verified, "Address Verified", x_levels = c("TRUE", "Exact match", "Fuzzy match", "FALSE"))
p_match_18       <- flag_boxplot(import_qc_18$address_matched, "Address Matched", x_levels = c("Only one address", "Exact match", "Fuzzy match", "FALSE"))
p_geolocation_18 <- flag_boxplot(import_qc_18$geolocation_verified, "Geolocation Verified", x_levels = c("TRUE", "FALSE", "No address_line_1"))
p_geoid_18       <- flag_boxplot(import_qc_18$geoid_match, "GeoID Match", x_levels = c("Matched", "Some matches not found", "Matches not found", "Not enough info"))

p_combined_18 <- (p_address_18 | p_match_18 | p_geolocation_18 | p_geoid_18) +
  plot_annotation(
    title    = "QC Flag Distributions Across Arrays",
    subtitle = "Batch 18850425: 87% ABI coverage (allowed address verification)",
    theme    = theme(
      plot.title    = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(size = 11, colour = "grey40")
    )
  )

p_combined_18

# Convert to data.table for fast in-place mutation (does not modify `df` in caller)
d <- as.data.table(import_qc_18$na_census_boundaries)
x_levels = c(
  "address", "geoid_2000", "geoid_2010", "geoid_2020", 
  "cbsa_code_2007", "cbsa_level_2007", "cbsa_code_2010", "cbsa_level_2010", 
  "cbsa_code_2020", "cbsa_level_2020", "csa_code_2007", "csa_code_2010", 
  "csa_code_2020", "zcta_2000", "zcta_2010", "zcta_2020"
)

d[, value := column]
d[, pct   := pct_na]
d[, value := data.table::fifelse(is.na(value), "NA", as.character(value))]

if (!is.null(x_levels)) {
  d[, value := factor(value, levels = c(x_levels, "NA"))]
}

# Scatterplots summarizing missingness across all census boundaries and batch arrays.
d %>%
  filter(column %!in% "address") %>%
  ggplot(aes(x = value, y = pct, fill = value)) +
    geom_boxplot(alpha = 0.6, outlier.shape = NA, na.rm = FALSE) +
    geom_jitter(aes(colour = value), width = 0.15, size = 2, alpha = 0.8, na.rm = FALSE) +
    scale_fill_viridis_d(drop = FALSE) +
    scale_colour_viridis_d(drop = FALSE, guide = "none") +
    scale_y_continuous(breaks = seq(0, 100, 10)) +
    coord_cartesian(ylim = c(0, 100)) +
    labs(
      title    = "Spatial Joining Matches Across Arrays",
      subtitle = "Batch 18850425: 87% ABI coverage (allowed address verification)",
      x = "Census Boundary and Decennial Period",
      y = "% with No Matches"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      legend.position = "none",
      plot.title      = element_text(face = "bold", size = 13, margin = margin(b = 4)),
      plot.subtitle   = element_text(size = 11, colour = "grey30", margin = margin(b = 10)),
      axis.text.x     = element_text(angle = 25, hjust = 1)
    )


# All expected ABIs are present. Note that this assessment considers a subset
# of ABIs in the raw data, parsed by ABI across each array, filtered to ABIs
# where all addresses were located in the US.
table(import_qc_20$abi_check$qc_pass, useNA = "ifany")

# Scatterplots of outcome distributions across each batch array; no address
# verification was performed.
p_match_20       <- flag_boxplot(import_qc_20$address_matched, "Address Matched", x_levels = c("Only one address", "Exact match", "Fuzzy match", "FALSE"))
p_geolocation_20 <- flag_boxplot(import_qc_20$geolocation_verified, "Geolocation Verified", x_levels = c("TRUE", "FALSE", "No address_line_1"))
p_geoid_20       <- flag_boxplot(import_qc_20$geoid_match, "GeoID Match", x_levels = c("Matched", "Some matches not found", "Matches not found", "Not enough info"))

p_combined_20 <- (p_match_20 | p_geolocation_20 | p_geoid_20) +
  plot_annotation(
    title    = "QC Flag Distributions Across Arrays",
    subtitle = "Batch 20823868: Remaining ABI not covered in batch 18850425 (no address verification)",
    theme    = theme(
      plot.title    = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(size = 11, colour = "grey40")
    )
  )

p_combined_20


# Convert to data.table for fast in-place mutation (does not modify `df` in caller)
d <- as.data.table(import_qc_20$na_census_boundaries)
x_levels = c(
  "address", "geoid_2000", "geoid_2010", "geoid_2020", 
  "cbsa_code_2007", "cbsa_level_2007", "cbsa_code_2010", "cbsa_level_2010", 
  "cbsa_code_2020", "cbsa_level_2020", "csa_code_2007", "csa_code_2010", 
  "csa_code_2020", "zcta_2000", "zcta_2010", "zcta_2020"
)

d[, value := column]
d[, pct   := pct_na]
d[, value := data.table::fifelse(is.na(value), "NA", as.character(value))]

if (!is.null(x_levels)) {
  d[, value := factor(value, levels = c(x_levels, "NA"))]
}

d %>%
  filter(column %!in% "address") %>%
  ggplot(aes(x = value, y = pct, fill = value)) +
    geom_boxplot(alpha = 0.6, outlier.shape = NA, na.rm = FALSE) +
    geom_jitter(aes(colour = value), width = 0.15, size = 2, alpha = 0.8, na.rm = FALSE) +
    scale_fill_viridis_d(drop = FALSE) +
    scale_colour_viridis_d(drop = FALSE, guide = "none") +
    scale_y_continuous(breaks = seq(0, 100, 10)) +
    coord_cartesian(ylim = c(0, 100)) +
    labs(
      title    = "Spatial Joining Matches Across Arrays",
      subtitle = "Batch 20823868: Remaining ABI not covered in batch 18850425 (no address verification)",
      x = "Census Boundary and Decennial Period",
      y = "% with No Matches"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      legend.position = "none",
      plot.title      = element_text(face = "bold", size = 13, margin = margin(b = 4)),
      plot.subtitle   = element_text(size = 11, colour = "grey30", margin = margin(b = 10)),
      axis.text.x     = element_text(angle = 25, hjust = 1)
    )


## --------------------
## SUBSECTION D4: At-Point-of-Evaluation Quality Checks

# In this section, the at-point-of-evaluation quality check datasets, which
# store the comprehensive results at each macro stage of the algorithm, are
# assessed. Their coverage at import is also reviewed.


# -- Address QC --------------------------------------

# The most comprehensive quality check (QC1) contains all ABIs; the remaining
# checks may show missing ABIs due to records being skipped during validation
# or matching. Common reasons include NA values for address_line_1 or no
# addresses remaining after USPS API validation. Additionally, approximately
# 70% of ABIs are expected to be ineligible for agnostic matching; without
# prior validation, these ABIs will not be captured by this metric.
table(address_qc$import_qc_18$key, "Pass Missingness Test" = address_qc$import_qc_18$qc_pass, useNA = "ifany")
table(address_qc$import_qc_20$key, "Pass Missingness Test" = address_qc$import_qc_20$qc_pass, useNA = "ifany")

# At most 12% of ABIs did not qualify for either QC2 or QC3 evaluation, though
# the average disqualification between arrays was 1.6%.
address_qc$import_qc_18 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/5000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/5000)*100, digits = 2),
    "Max" = round((max(n_missing)/5000)*100, digits = 2)
  )

# A little more ABIs did not qualify for QC3, where QC2 is completely not
# represented in this batch. This is because address validation was not
# allowed. Similar to the previous batch, on average 1.7% of ABIs were
# disqualified.
address_qc$import_qc_18 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/1000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/1000)*100, digits = 2),
    "Max" = round((max(n_missing)/1000)*100, digits = 2)
  )


# 87% of addresses were eligible for the API, and all eligible addresses used
# the API validation algorithm as expected.
round(prop.table(table(address_qc$qc1$allow_usps_api, useNA = "ifany"))*100, digits = 2)
round(prop.table(table("Allow" = address_qc$qc1$allow_usps_api, "Used" = address_qc$qc1$api_used, useNA = "ifany"))*100, digits = 2)

# Of the records that underwent API validation, approximately 1.5% did not
# attempt verification — verified_address is absent from the dataset for
# these records. All such cases correspond to a NA value in address_line_1,
# indicating that no valid address was available for the business and the
# validation step was therefore skipped.
round(prop.table(table("Allow Verification" = address_qc$qc1$allow_usps_api, "Attempted" = address_qc$qc1$verification_attempted, useNA = "ifany"))*100, digits = 2)
round(prop.table(table("Any NA Addresses" = address_qc$qc1$any_addresses_line1_na, "Attempted" = address_qc$qc1$verification_attempted, "Allow Verification" = address_qc$qc1$allow_usps_api, useNA = "ifany"))*100, digits = 2)

# We can verify this directly by isolating the ABIs suspected of filing
# exclusively with NA values in address_line_1.
test_abi <- address_qc$qc1 %>%
  filter(allow_usps_api == TRUE &
           verification_attempted == FALSE & 
           any_addresses_line1_na == TRUE) %>%
  pull(abi)

# As expected, all of these ABIs correspond to records where no addresses
# were valid — i.e., all address_line_1 values are NA.
church_2026_form_dt[
  abi %in% test_abi,
  .(all_unique_address_line_1_na = all(is.na(unique(address_line_1)))),
  by = abi
][
  , .N, by = all_unique_address_line_1_na
]

# Almost all ABIs (~98%) were processed through agnostic matching. No duplicates
# were detected following this step, and no address-year combinations differed
# between the raw and validated forms of the data, confirming that no data loss
# occurred.
round(prop.table(table(address_qc$qc1$match_attempted, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(address_qc$qc1$duplicates_induced, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(address_qc$qc1$new_vs_old_differ, useNA = "ifany"))*100, digits = 2)


# Set categorical outcome ordering for easier reading.
address_ver_order <- c(TRUE, "Exact match", "Fuzzy match", FALSE, "No address_line_1")
address_geo_order <- c(TRUE, FALSE, "Override")
address_attempt_order <- c("First try", "With city correction by zip5", "None")

# The only values that failed the geolocation test occurred when verification
# failed and no match was found.
round(prop.table(table(
  "Geolocation Test" = factor(address_qc$qc2$ver_geolocation_test, levels = address_geo_order, ordered = TRUE),
  "Verified Address" = factor(address_qc$qc2$address_verified, levels = address_ver_order, ordered = TRUE), 
  useNA = "ifany"
), margin = 2)*100, digits = 2)

# Some matching attempts coincided with API query errors such as "400 Bad Request".
# Most failures to verify an address were associated with rate limit errors.
round(prop.table(table(
  "USPS Status Detail" = address_qc$qc2$usps_status_detail,
  "Verified Address" = factor(address_qc$qc2$address_verified, levels = address_ver_order, ordered = TRUE), 
  useNA = "ifany"
), margin = 2)*100, digits = 2)

# The vast majority of successful verification attempts happened on the first try,
# with less than 1% verified using the secondary fallback strategy. NOTE: All
# NAs correspond to outcomes where `attempt_succeeded == "No address_line_1"`.
round(prop.table(table(
  "Geolocation Test" = address_qc$qc2$usps_status_detail, 
  "Attempt Succeeded" = factor(address_qc$qc2$attempt_succeeded, levels = address_attempt_order, ordered = TRUE)
), margin = 1)*100, digits = 2)


# Set categorical outcome ordering for easier reading.
address_match_order <- c("Exact match", "Fuzzy match", "Only one address", "No address_line_1")
address_geo_order <- c("PASS", 'PASS; no Lon/Lat', "FAIL", "Override")

# Most fuzzy-matched attempts (86%) passed the geolocation test; less than 1%
# passed with an incomplete comparison due to one record missing geocoordinates.
round(prop.table(table(
  "Matched Address" = factor(address_qc$qc3$address_matched, levels = address_match_order, ordered = TRUE), 
  "Geolocation Test" = factor(address_qc$qc3$match_geolocation_test, levels = address_geo_order, ordered = TRUE),
  useNA = "ifany"
), margin = 1)*100, digits = 2)


# -- Geo QC ------------------------------------------

# The vast majority of arrays contained ABIs that were not processed through
# the geolocation tests. This is expected, since QC1 and QC2 restricted their
# assessments to only those ABI records where an address was available for
# address-based geocoding. QC2 additionally filtered by non-missing verified
# geocoordinates. QC3 focused only on ABI records where more than one valid
# pair of geocoordinates was available for generating the summary tables.
table(geo_qc$import_qc_18$key, "Pass Missingness Test" = geo_qc$import_qc_18$qc_pass, useNA = "ifany")
table(geo_qc$import_qc_20$key, "Pass Missingness Test" = geo_qc$import_qc_20$qc_pass, useNA = "ifany")

# As many as 45% of ABIs are not reported in QC2, with QC1 showing the most
# coverage (average 1.6%). However, arrays present with as many as 12% ABI
# not represented on average.
geo_qc$import_qc_18 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/5000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/5000)*100, digits = 2),
    "Max" = round((max(n_missing)/5000)*100, digits = 2)
  )

# Lack of representation is even higher for the second batch run, with maxima
# as high as 98% for QC2. However, the averages excluded still tend to be
# comparable with the previous batch. This is consistent with the results seen
# in earlier plots, where it was shown that this batch resulted in some arrays
# with high degrees of no validation.
geo_qc$import_qc_20 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/1000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/1000)*100, digits = 2),
    "Max" = round((max(n_missing)/1000)*100, digits = 2)
  )


# Set categorical outcome ordering for easier reading.
geolocation_verified_order <- c("TRUE", "FALSE", "No address_line_1")

# A higher proportion of addresses with valid geocoordinates (non-NA) were
# verified through address-based geocoding. ~80% of entries without valid
# geocoordinates failed to verify in comparison to ~22% of those with valid
# geocoordinates.
round(prop.table(table(
  "Geo Verified" = factor(geo_qc$qc1$geolocation_verified, levels = geolocation_verified_order, ordered = TRUE), 
  "Enough Valid Geo" = geo_qc$qc1$enough_geo, 
  useNA = "ifany"
), margin = 2)*100, digits = 2)

# "N Addresses" counts the number of unique addresses were aggregated and had 
# valid geocoordinates geocoordinates. In general, we see an increase in success 
# as more records are included, but this drops down again with more than 7 
# records, with 8 having a 50/50 match rate.
round(prop.table(table(
  "Geo Verified" = factor(geo_qc$qc1$geolocation_verified, levels = geolocation_verified_order, ordered = TRUE), 
  "N Address" = geo_qc$qc1$n_address, 
  useNA = "ifany"
), margin = 2)*100, digits = 2)

# As would be expected, any query where the number of attempts falls below the
# max number of benchmark/vintage tries listed successfully verified
# geocoordinates. ~98% of queries were successful with the first try. The next
# query tries with some successes was the third set, with very little verifying
# using the fourth set.
round(prop.table(table(
  "# Attempts" = geo_qc$qc1$n_attempts,
  "Geo Verified" = factor(geo_qc$qc1$geolocation_verified, levels = geolocation_verified_order, ordered = TRUE), 
  useNA = "ifany"
), margin = 2)*100, digits = 2)

# Recall the benchmark and vintage pairing tries listed under
# "SUBSECTION A3: Set Geocoder Search Priorities". "[Public_AR|Current]_Current" 
# at this time are expected to be the same as "[Public_AR|Census2020]_Census2020".
spec

# Of outcomes that reached four tries, 45 different combinations of API query
# responses were received, with 99.5% having all 200 status codes, indicating
# a successful query interaction. This confirms that failures to match largely
# were a result of no match being available in the respective benchmark/vintage
# try pairings databases used.
geo_qc$qc1 %>% 
  filter(n_attempts == 4) %>%
  (\(x) {
    round(prop.table(table(
      "All Statuses 200" = x$query_statuses, 
      "# Attempts" = x$n_attempts,
      useNA = "ifany"
    ), margin = 2)*100, digits = 2)
  })() %>%
  length()


# A string comparison was done between the queried address and the matched
# address in the US Census Bureau database. The algorithm processing JSON
# responses handled multiple possible matches through best match triaging.
# This assessment quickly identifies if the given address and best match
# chosen from the JSON response are exactly the same or similar (delta = 0.15).
#
# Results show most JSON results were the exact same (78%). Almost 19% of
# responses that were not exactly the same were similar. Only 3% were neither
# similar nor the exact same.
round(prop.table(table(
  "Address Similar" = geo_qc$qc1$matched_address_similar, 
  "Address Same" = geo_qc$qc1$matched_address_same
))*100, digits = 2)


# It is important to assess if verifying addresses or agnostically matching
# them results in any changes with address-based geocoding.
#
# If results where a verification was retained (directly or through matching)
# are considered, then we see no obvious differences between those that had
# geocoordinates verified and those that did not.
geo_qc$qc1 %>%
  mutate(
    address_verified = case_when(
      address_verified %in% c(TRUE, "Exact match", "Fuzzy match") ~ "TRUE",
      address_verified %in% c(FALSE) ~ "FALSE",
      TRUE ~ "Other"
    )
  ) %>%
  count(geolocation_verified, address_verified, .drop = FALSE) %>%
  pivot_wider(names_from = address_verified, values_from = n, values_fill = 0) %>%
  mutate(
    across(c(`FALSE`, Other, `TRUE`), \(x) round(100 * x / (`FALSE` + Other + `TRUE`), 2))
  )

# Similarly, reducing matching outcomes together indicates agnostic matching
# may have negatively impacted matching with the geocoding database.
geo_qc$qc1 %>%
  mutate(
    address_matched = case_when(
      address_matched %in% c("Exact match", "Fuzzy match") ~ "TRUE",
      address_matched %in% c(NA) ~ "FALSE",
      TRUE ~ "Other"
    )
  ) %>%
  count(geolocation_verified, address_matched, .drop = FALSE) %>%
  pivot_wider(names_from = address_matched, values_from = n, values_fill = 0) %>%
  mutate(
    across(c(`FALSE`, Other, `TRUE`), \(x) round(100 * x / (`FALSE` + Other + `TRUE`), 2))
  )

# For both verified addresses and agnostic matching, those compressed via
# fuzzy matching saw an attenuation in getting a geocode match, whereas exact
# matching did not seem to change results when matched to a directly verified
# address or in comparison to results with one address (no matches attempted).
round(prop.table(table(
  "Address Verified" = factor(geo_qc$qc1$address_verified, levels = address_ver_order, ordered = TRUE), 
  "Geo Verified" = factor(geo_qc$qc1$geolocation_verified, levels = geolocation_verified_order, ordered = TRUE), 
  useNA = "ifany"
), margin = 1)*100, digits = 2)

round(prop.table(table(
  "Address Matched" = factor(geo_qc$qc1$address_matched, levels = address_match_order, ordered = TRUE), 
  "Geo Verified" = factor(geo_qc$qc1$geolocation_verified, levels = geolocation_verified_order, ordered = TRUE), 
  useNA = "ifany"
), margin = 1)*100, digits = 2)


# It would be interesting to assess how well the reported values aligned to
# the verified match from the Census Bureau Geocoder database. It was observed
# that 21% had a difference between the respective geocoordinates of more
# than 0.002 degrees (about 2 city blocks). 12% had a difference between
# both.
round(prop.table(table(
  "Lat Diff > 0.002" = geo_qc$qc2$lat_gt_002, 
  "Lon Diff > 0.002" = geo_qc$qc2$lon_gt_002, 
  useNA = "ifany"
))*100, digits = 2)

# When looking only at those where the differences exceeded 0.002 degrees,
# the mean difference is 0.0197 degrees and the max is 22.54 degrees
# in latitude.
geo_qc$qc2 %>%
  filter(lat_gt_002 == TRUE) %>%
  summarize(
    "Min" = round(min(lat_abs_diff), digits = 4),
    "Mean" = round(mean(lat_abs_diff), digits = 4),
    "Max" = round(max(lat_abs_diff), digits = 4)
  )

# When looking only at those where the differences exceeded 0.002 degrees,
# the mean difference is 0.0233 degrees and the max is 75.72 degrees
# in longitude.
geo_qc$qc2 %>%
  filter(lon_gt_002 == TRUE) %>%
  summarize(
    "Min" = round(min(lon_abs_diff), digits = 4),
    "Mean" = round(mean(lon_abs_diff), digits = 4),
    "Max" = round(max(lon_abs_diff), digits = 4)
  )


# It would be interesting to assess how well the reported values aligned within
# the same reported address. Based on limited manual assessment, it was noted
# that these varied. Most addresses were within 0.02 degrees longitude or
# latitude, about 2 kilometers or 1.4 miles. About 5%, however, had some
# variation, either in longitude, latitude, or both.
round(prop.table(table(
  "Lat Spread > 0.02" = geo_qc$qc3$lat_spread_gt_02, 
  "Lon Spread > 0.02" = geo_qc$qc3$lon_spread_gt_02, 
  useNA = "ifany"
))*100, digits = 2)

# One concern is that address validation or matching increased the amount of
# geolocation variation. However, based on the results from the previous
# quality check, this may be moot since the given coordinates have a large
# margin of error.
# 
# Looking at the latitude for verified addresses, there does not appear to be 
# any strong effect on the max or average difference of geolocation amongst the 
# same address. For context, the number of results represented for each stratum 
# is included. These run a little on the high end.
geo_qc$qc3 %>%
  filter(lat_spread_gt_02 == TRUE) %>%
  group_by(any_address_verified, .drop = FALSE) %>%
  summarise(
    n = n(),
    "Max Diff"  = round(max(lat_max, na.rm = TRUE) - min(lat_min, na.rm = TRUE), 4),
    "Mean Diff" = round(mean(lat_max, na.rm = TRUE) - mean(lat_min, na.rm = TRUE), 4),
    "Avg N Geo"     = round(mean(n_geo, na.rm = TRUE), 0),
    .groups = "drop"
  )

# Similarly, for agnostically matched addresses, there does not appear to be a
# strong discernible influence on the max or average difference of geolocation
# amongst the same address.
geo_qc$qc3 %>%
  filter(lat_spread_gt_02 == TRUE) %>%
  group_by(any_address_matched, .drop = FALSE) %>%
  summarise(
    n = n(),
    "Max Diff"  = round(max(lat_max, na.rm = TRUE) - min(lat_min, na.rm = TRUE), 4),
    "Mean Diff" = round(mean(lat_max, na.rm = TRUE) - mean(lat_min, na.rm = TRUE), 4),
    "Avg N Geo"     = round(mean(n_geo, na.rm = TRUE), 0),
    .groups = "drop"
  )

# Looking again but for longitude, there also does not appear to be a strong
# influence caused by verifying the address or agnostically matching them.
geo_qc$qc3 %>%
  filter(lon_spread_gt_02 == TRUE) %>%
  group_by(any_address_verified, .drop = FALSE) %>%
  summarise(
    n = n(),
    "Max Diff"  = round(max(lon_max, na.rm = TRUE) - min(lon_min, na.rm = TRUE), 4),
    "Mean Diff" = round(mean(lon_max, na.rm = TRUE) - mean(lon_min, na.rm = TRUE), 4),
    "Avg N Geo"     = round(mean(n_geo, na.rm = TRUE), 0),
    .groups = "drop"
  )

geo_qc$qc3 %>%
  filter(lon_spread_gt_02 == TRUE) %>%
  group_by(any_address_matched, .drop = FALSE) %>%
  summarise(
    n = n(),
    "Max Diff"  = round(max(lon_max, na.rm = TRUE) - min(lon_min, na.rm = TRUE), 4),
    "Mean Diff" = round(mean(lon_max, na.rm = TRUE) - mean(lon_min, na.rm = TRUE), 4),
    "Avg N Geo"     = round(mean(n_geo, na.rm = TRUE), 0),
    .groups = "drop"
  )


# -- Census QC ---------------------------------------

# The vast majority of arrays contained ABIs that were not processed through
# the QC1 census tests assessing GEOIDs, where a higher proportion of arrays
# completely represented ABIs when assessing the CBSA/CSA/ZCTA boundaries.
# This is expected, since QC1 restricted its assessments to only ABI records
# where enough geocoordinates were available for spatial joining. QC1 also
# filters by entries where a GEOID match was made after joining.
table(census_qc$import_qc_18$key, "Pass Missingness Test" = census_qc$import_qc_18$qc_pass, useNA = "ifany")
table(census_qc$import_qc_20$key, "Pass Missingness Test" = census_qc$import_qc_20$qc_pass, useNA = "ifany")

# As many as 16% of ABIs were not represented in QC1, with the average
# exclusion sitting under 3%.
census_qc$import_qc_18 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/5000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/5000)*100, digits = 2),
    "Max" = round((max(n_missing)/5000)*100, digits = 2)
  )

# Lack of representation is slightly higher for the second batch run, with maxima
# as high as 19% for QC1. However, the averages excluded still tend to be
# comparable with the previous batch. This is consistent with the results seen
# in earlier plots, where it was shown that this batch resulted in some arrays
# with high degrees of NA after spatial joining.
census_qc$import_qc_20 %>%
  group_by(key) %>%
  summarize(
    "Min" = round((min(n_missing)/1000)*100, digits = 2),
    "Mean" = round((mean(n_missing)/1000)*100, digits = 2),
    "Max" = round((max(n_missing)/1000)*100, digits = 2)
  )


# The big questions we want to answer are these: was the raw data annotated
# with the correct census boundaries, and if not, what is the extent of the
# error; do the matched boundaries by decennial year correspond to the years
# the address was filed under.
#
# The following function helps to construct generalized tables showing this
# information over multiple columns for different boundaries and over multiple
# alignment checks for these. Percentages are then calculated over the outcomes
# for a given census boundary.

make_pct_table <- function(data, cols, levels = NULL, row_labels = NULL) {
  data <- as.data.frame(data)
  
  tab <- sapply(cols, \(nm) {
    x <- data[[nm]]
    if (!is.null(levels)) {
      x <- factor(x, levels = levels)
    } else {
      x <- factor(x)
    }
    round(prop.table(table(x, useNA = "ifany")) * 100, digits = 2)
  }, simplify = "matrix")
  
  result <- t(tab)
  
  if (!is.null(row_labels)) {
    rownames(result) <- row_labels
  }
  
  result
}

# Order the possible combinations of decennial years observed.
vintage_levels <- c(
  "2000", "2010", "2020", "2000, 2010", "2000, 2020", "2010, 2020",
  "2000, 2010, 2020", "None", "Not reported", "Uncheckable"
)

# A quick check can be run to see if each unique address is consistently
# associated with the same census metadata. This could be skewed due to the
# matching processes done. In general, we have reason to believe the same
# census boundaries were not applied to the same address over the years.
n_distinct(census_qc$qc1$address) == nrow(distinct(census_qc$qc1, address, census_block))
n_distinct(census_qc$qc1$address) == nrow(distinct(census_qc$qc1, address, census_tract))
n_distinct(census_qc$qc1$address) == nrow(distinct(census_qc$qc1, address, county_code))
n_distinct(census_qc$qc1$address) == nrow(distinct(census_qc$qc1, address, fips_code))

# Generate the tables over GEOID boundaries and three alignment checks.
any_match <- c( # Set 1: Confirms if any vintages matched the raw values
  fips   = "fips_any_match",
  county = "county_any_match",
  tract  = "tract_any_match",
  block  = "block_any_match"
)
listed_vintages <- c( # Set 2: Vintages matched
  fips   = "fips_vintages",
  county = "county_vintages",
  tract  = "tract_vintages",
  block  = "block_vintages"
)
any_aligned <- c( # Set 3: Boolean if the matched vintages correspond to the years open
  fips   = "fips_vintages_aligned",
  county = "county_vintages_aligned",
  tract  = "tract_vintages_aligned",
  block  = "block_vintages_aligned"
)

# Generate all three tables
make_pct_table(census_qc$qc1, any_match)
make_pct_table(census_qc$qc1, listed_vintages, levels = vintage_levels) %>% t()
make_pct_table(census_qc$qc1, any_aligned)


# A quick check can be run to see if each unique address is consistently
# associated with the same census metadata. This could be skewed due to the
# matching processes done. In general, we have reason to believe the same
# census boundaries were not applied to the same address over the years.
n_distinct(census_qc$qc2$address) == nrow(distinct(census_qc$qc2, address, cbsa_level))
n_distinct(census_qc$qc2$address) == nrow(distinct(census_qc$qc2, address, cbsa_code))
n_distinct(census_qc$qc2$address) == nrow(distinct(census_qc$qc2, address, csa_code))

# Generate the tables over GEOID boundaries and three alignment checks.
any_match <- c( # Set 1: Confirms if any vintages matched the raw values
  cbsa_code  = "cbsa_code_any_match",
  cbsa_level = "cbsa_level_any_match",
  csa        = "csa_any_match"
)
listed_vintages <- c( # Set 2: Vintages matched
  cbsa_code  = "cbsa_code_vintages",
  cbsa_level = "cbsa_level_vintages",
  csa        = "csa_vintages"
)
any_aligned <- c( # Set 3: Boolean if the matched vintages correspond to the years open
  cbsa_code  = "cbsa_code_vintages_aligned",
  cbsa_level = "cbsa_level_vintages_aligned",
  csa        = "csa_vintages_aligned"
)

# Generate all three tables
make_pct_table(census_qc$qc2, any_match)
make_pct_table(census_qc$qc2, listed_vintages, levels = vintage_levels) %>% t()
make_pct_table(census_qc$qc2, any_aligned)


# Assess matching
# An attenuation of matching is observed as the boundary becomes smaller, with
# near perfect matching for FIPS and county. Only 0.01% of tract and block
# entries were reported as NA, and are consequentially uncheckable. CBSA code 
# yielded a high match rate at 85%, CSA at 65%, and CBSA level at the lowest, 
# with matches at 3%. Many CBSA level results were uncheckable too, at 12%.
# 
# Assess vintages
# As would be expected, FIPS and county generally matched with GEOIDs across
# decennial periods (over 97%). Tract and block saw 44% and 35%, respectively.
# The next big category of matching was the 2000 decennial period, but nearly
# the same proportion of records match over the 2000, 2010 and 2010, 2020
# decennial periods. This indicates that the given GEOID boundaries do not
# clearly follow a pattern of annotating information from any one decennial
# period.
# 
# Most CBSA code, CBSA level, and CSA records did not match a decennial year,
# with a high level reporting as NA. The NA values arise from entries failing
# to annotate with that boundary during spatial joining, which is expected since
# these represent urban areas and will not cover the whole contiguous US. The
# vintage with the most matches was the 2020 vintage (as high as 14%), with all
# other vintages ranking well below 1%.
# 
# Assess align with years-open
# The variability in census boundaries indicated earlier may legitimately be
# a manifestation of census boundaries getting correctly applied when that
# address was filed. However, we see that this assumption only holds for the
# most macro-level boundaries, FIPS and county. High adherence is also observed for
# CBSA codes, despite most matching with the 2020 decennial year.
#
# Granular boundaries like tract and block showed progressively increasing
# lack of alignment, with as high as 15% being unable to assess. CSA had
# just more than half align, but users should note that this proportion may be
# misleading. We would expect that there is a high degree of failed comparisons
# since CSA is not applied for all parts of the US. Therefore, the real
# proportion of candidates is expected to be higher.


