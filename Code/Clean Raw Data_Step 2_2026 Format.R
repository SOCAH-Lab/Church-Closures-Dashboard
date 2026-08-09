## ----------------------------------------------------------------
## 
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: August 3rd, 2026
## 
## Description: 
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
##    - PART A: 
##        * SUBSECTION A1: 
##        * SUBSECTION A2: Set Geocoder Search Priorities
##        * SUBSECTION A3: Build Precompiled TIGER/Line GeoPackages
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
## PART A: 

## --------------------
## SUBSECTION A1: 

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
#' @field n_abi_any_na_address_line_1/pct_abi_any_na_address_line_1 Count and
#'              percent of ABIs with at least one missing address_line_1.
#'
#' @field n_abi_poBox/pct_abi_poBox Count and percent of ABIs with at least
#'                                  one PO Box entry.
#'
#' @field lon/lat TIGER/Line Shapefile geocoordinates associated with the city.

# Save result.
write.csv(na_joined, file = "./Data/Results/From Clean Raw Data/Step 2_2026 Format/ABI with NA Addresses by City_08.07.2026.csv")
write.csv(po_joined, file = "./Data/Results/From Clean Raw Data/Step 2_2026 Format/ABI with PO Boxes by City_08.07.2026.csv")


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
## SUBSECTION A2: Set Geocoder Search Priorities

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
## SUBSECTION A3: Build Precompiled TIGER/Line GeoPackages

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




## ----------------------------------------------------------------
## PART B: ALGORITHM TO CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA

# Add blurb about what is being validated etc.

# LOOP PART A: Isolate Unique Candidate Addresses
# LOOP PART B: Consolidate and Verify the Addresses
# LOOP PART B.i.: Validate Addresses with USPS Database
# LOOP PART B.ii.: Resolve Records with No Address Match Found
# LOOP PART B.iii.: Agnostically Resolve Record Heterogeneity
# LOOP PART C: Verify Geolocation with the US Census Bureau’s Geocoder Database
# LOOP PART D: Point-in-Polygon Spatial Assignment of Census Information
# LOOP PART E: Add Back to Main Dataset
# LOOP PART F: Quality Checks — Address Validation and Consolidation Results
# LOOP PART G: Quality Checks — Variation with Geolocation
# LOOP PART H: Quality Checks — Variation with Census Information
# LOOP PART I: Commit Results


## ----------------------------------------------------------------
## PART C: Recompile Results from the HPC

# Compute resources for batches deployed on Yale's High Performance Computing
# (HPC) cluster are managed using SLURM. Due to these resource constraints,
# only 25 arrays could be processed concurrently.
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
# be clarified accordingly.
qc_df_18$data <- qc_df_18$data %>%
  mutate(
    address_verified = case_when(
      is.na(address_verified) ~ "No address_line_1",
      TRUE                    ~ address_verified
    )
  )


qc_address_18 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_18850425/Results/Address QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
)

# Unique reported address and year combinations are checked for correspondence
# to the raw data. No induced duplications were detected after address
# verification and matching.
table(qc_address_18$qc1$`New vs Old differ`, useNA = "ifany")

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
    usps_status = case_when(
      usps_status_detail == "200 Successful operation" ~ 200,
      TRUE                                             ~ usps_status
    ),
    usps_status_detail = case_when(
      usps_status_detail == "Other unanticipated errors" ~ "200 Successful operation",
      usps_status_detail == "403 Forbidden"              ~ "403 Access denied",
      TRUE                                               ~ usps_status_detail
    )
  )


qc_census_18 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_18850425/Results/Census QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
)

qc_geo_18 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_18850425/Results/Geo QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
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


qc_address_20 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_20823868/Results/Address QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
)

qc_census_20 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_20823868/Results/Census QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
)

qc_geo_20 <- compile_duckdb_folder(
  subset_dir = file.path(getwd(), data_root, "batch_array_20823868/Results/Geo QC/"),
  abi_ref    = unique(church_2026_form_dt$abi)
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

# Build/refresh "data" by unioning both parquet datasets and aligning columns 
# by name.
dbExecute(con, sprintf("
  CREATE OR REPLACE TABLE data AS
  SELECT * FROM read_parquet('%s/**/*.parquet')
  UNION ALL BY NAME
  SELECT * FROM read_parquet('%s/**/*.parquet');
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

church_2026_form_validated <- import_church_db(
  db_path = file.path(data_root, "Compiled by Batches", "church_2026_form_validated_08.03.2026.db"),
  import_data = "data"
)
church_2026_form_validated_dt <- as.data.table(church_2026_form_validated$data)  # Convert for efficient data manipulation
setorder(church_2026_form_validated_dt, abi)  # Organize the table by state to increase census boundary efficiency then abi


# Otherwise, import the QC results, which cover algorithm cleaning and
# validation as well as high-level batch result checks. List elements
# containing "import" pertain to the latter.

church_2026_form_validated_import_qc <- import_church_db(
  db_path = file.path(data_root, "Compiled by Batches", "church_2026_form_validated_08.03.2026.db"),
  import_data = "qc"
)

# Import the QC datasets generated by the cleaning and validation algorithms
# at the time of processing. The import framework and import metrics are
# provided here, but the quality assessment for the cleaning and validation
# method is documented in the separate "Preparation Step 2 QC_2026 Format"
# PDF report.
address_qc <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "address_qc_08.05.2026.db"))
census_qc  <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "census_qc_08.06.2026.db"))
geo_qc     <- read_list_from_duckdb(file.path(data_root, "Compiled by Batches", "geo_qc_08.06.2026.db"))


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


## --------------------
## SUBSECTION D3: 

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
## SUBSECTION D3: 



df1 <- qc_df_18$data %>% collect()
df2 <- qc_df_20$data %>% collect()


c(unique(df1$abi), unique(df2$abi))




align_cols <- c(
  fips   = "fips_vintages_aligned",
  county = "county_vintages_aligned",
  tract  = "tract_vintages_aligned",
  block  = "block_vintages_aligned"
)

tab <- sapply(align_cols, \(nm) round(table(qc_census$qc1[[nm]], useNA = "ifany")/nrow(qc_census$qc1) * 100, digits = 2))
out <- as.data.frame.matrix(t(tab))
out

tab <- table(
  "Address Verified" = qc_geo$qc1$address_verified,
  "Geo Verified"     = qc_geo$qc1$geolocation_verified,
  useNA = "ifany"
)

row_pct <- round(prop.table(tab, margin = 1) * 100, 2)
row_pct[,-c(2)]

# NOTE: Sometimes one or both geolocation was missing. During the matching,
# this is overriden unless one geolocation test fails. Also, the census
# annotation was missing from pair matches with missing geolocation, but
# this was reconciled through address matching.
# 
# Want to see how many were.











