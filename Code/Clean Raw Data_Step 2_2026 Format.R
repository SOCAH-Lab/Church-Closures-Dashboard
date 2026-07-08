## ----------------------------------------------------------------
## 
##
## NOTE: This script was designed for the 2026 raw data format and reflects
##       an updated procedure from the original version found in
##       "Clean Raw Data_Step 1_2023 Format.R", "Clean Raw Data_Step 2_2023 Format.R", 
##       and "Clean Raw Data_Step 2 HPC_2023 Format.R". Refer to 
##       "Process Data Update.R" for a description of the differences and any 
##       handling variations.
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: July 7th, 2026
## 
## Description: 
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
##       Check for the TIGER driver (returns one row if installed):
##       sf::st_drivers() |> subset(name == "TIGER")
##
##       Confirm the GDAL version (this script was developed using v3.5.3):
##       sf::sf_extSoftVersion()["GDAL"]
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
##    - PART B: 
##        * SUBSECTION B1: 
##        * SUBSECTION B2: 
##        * SUBSECTION B3: 

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("arrow")            # Parquet/Feather & fast I/O (Arrow)
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  # library("ggplot2")          # Graphics and visualization
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests for APIs (GET/POST, headers, auth)
  library("jsonlite")         # Parse/write JSON (fromJSON/toJSON)
  library("future.apply")     # Parallel processing
  library("stringdist")       # Measuring string distances
  # library("progress")         # Progress bars
  library("data.table")       # High-performance data manipulation
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
})

# Set up the plan for parallel processing.
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

# NOTE: Individual-level data is stored in the "Data/Raw KEEP LOCAL" file
# to comply with the Data Use Agreement (DUA).


# In May 2026, an updated version of the raw data was provided in a format
# different from versions exported in July 2023 and provided in the summer
# of 2025. As a result, data processing was split into two paths: one for
# the 2023 format and one for the 2026 format.
#
# The following is a modified version of the 2023 pipeline in which "Steps
# 1–3" and "Step 5" are consolidated into a single script with additional
# safeguards to prevent duplicate entries and maximize validation coverage.
# At the end of this step, all entries used for data visualization
# (addresses and longitude/latitude) are validated, address uniqueness is
# maximized through similarity checking, and census boundaries (block,
# tract, county, state, CBSA, and CSA) are associated across three
# decennial periods (2000, 2010, and 2020) using longitude/latitude.
#
# In the preceding step, nomenclature was standardized and its consistency
# verified. Data were also converted from CSV to Parquet for faster
# handling. In the following step, data will be transformed from long format
# (dates as rows) to wide format (dates as columns). Generalized classifiers
# will also be added based on SIC codes, and PO Boxes will be associated
# with their nearest physical address.
#
# Differences between the two formats were evaluated in
# "./Code/Process Data Update.R", with findings summarized on the
# corresponding Review page at:
# https://socah-lab.github.io/Church-Closures-Dashboard/Pages/Review_2026%20Format.html
#
# Insights from the 2023 format are assumed to apply to the 2026 format as
# well. For complete data exploration and the reporting that justified the
# steps taken here, please refer to "./Code/Explore the Raw Data.R" and the
# corresponding Review page at:
# https://socah-lab.github.io/Church-Closures-Dashboard/Pages/Review_2023%20Format.html

# Load standardized and converted data
church_2026_form <- read_parquet("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")

# Toggle based on whether the zip code field was originally imported as a
# character type; if not, leading and trailing zeros may have been lost.
zip_codes_character <- TRUE

# Load core field handling/rename spec.
core_fields <- read_csv("Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")

# Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)

# Load the combined national-level Metropolitan/Micropolitan Statistical Area 
# (CBSA), Combined Statistical Area (CSA), and ZIP Code Tabulation Areas (ZCTA)
# GeoPackage file. If nothing loads, skip to "SUBSECTION A3: Build Precompiled 
# TIGER/Line GeoPackages" first before coming back to this line.
# 
# State block or block group, tract, county, and state geometries are loaded 
# later in the script, as they depend on which states are present in the 
# current subset.
core_areas_layers <- sf::st_layers("Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg")$name

core_areas <- setNames(
  lapply(core_areas_layers, function(lyr) st_read("Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg", layer = lyr, quiet = TRUE)),
  core_areas_layers
)




## ----------------------------------------------------------------
## PART A: CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA

## --------------------
## SUBSECTION A1: 

# Update merge protocol and move to HPC
# Fix the expansion algorithm so it capture all expected cases
#   - what is up with 102351087?
#   - ensure the non-expanded are converted to something other than FALSE, that's confusing



# Data evaluation revealed that most reduplicated entries arise from either new 
# addresses added within the observation period or typographical errors. To 
# resolve this, stringdist() and Depth-First Search (DFS) are used to cluster 
# similar address variants, with one address randomly selected from each group 
# to carry forward.
#
# While manual adjudication would be required to guarantee that the retained 
# address is a verified street address, the approach described here incorporates 
# a Boolean quality check to flag potentially incorrect combinations of similar 
# but distinct addresses. This check evaluates the maximum change in geographic 
# coordinates (longitude and latitude) to ensure that retained address variants 
# are either near-identical or exact matches.

# For reference, one degree of longitude is approximately 69 miles (111 
# kilometers), while one degree of latitude varies based on proximity to the 
# equator, averaging approximately 54 miles (87 kilometers) across the 
# contiguous United States. The length of a typical U.S. city block also varies, 
# with common estimates ranging from 100 to 200 meters. Based on these benchmarks, 
# a deviation exceeding 0.002 degrees in either longitude or latitude 
# — approximately 222 meters — is used as the threshold for flagging significant 
# geographic discrepancy.
# 
# Sources:
#   - https://www.usgs.gov/faqs/how-much-distance-does-a-degree-minute-and-second-cover-your-maps
#   - https://www.nhc.noaa.gov/gccalc.shtml
#   - https://en.wikipedia.org/wiki/City_block
#   - https://en.wikipedia.org/wiki/List_of_United_States_cities_by_area

# NOTE: Results were already generated and saved. Load them below.



# Add note that the character check on zipcode is to trigger the leading and trailing zeros script

is.na(church_2026_form$address_line_1) %>% table()
is.na(church_2026_form$city) %>% table()
is.na(church_2026_form$state) %>% table()
nchar(church_2026_form$zipcode) %>% unique()
nchar(church_2026_form$zip4) %>% unique()

sapply(church_2026_form[, c("address_line_1", "zipcode", "zip4")], is.na) %>%
  as.data.frame() %>%
  (\(x) {
    table(
      "zipcode" = x$zipcode, 
      "zip4" = x$zip4,
      "address_line_1" = x$address_line_1
      )
    })()



abi_cannot_verify <- church_2026_form %>%
  group_by(abi) %>%
  filter(any(is.na(address_line_1))) %>%
  pull(abi) %>%
  unique()

round(length(abi_cannot_verify)/length(unique(church_2026_form$abi))*100, digits = 2)


results <- sapply(church_2026_form[, c("address_line_1", "zipcode", "zip4")], is.na) %>%
  as.data.frame()

church_2026_form[which(results$address_line_1 == FALSE & results$zipcode == TRUE), "abi"]

church_2026_form %>%
  filter(abi %in% "789391643")


sapply(church_2026_form[church_2026_form$abi %!in% abi_cannot_verify, c("address_line_1", "zipcode", "zip4")], is.na) %>%
  as.data.frame() %>%
  (\(x) {
    table(
      "zipcode" = x$zipcode, 
      "zip4" = x$zip4,
      "address_line_1" = x$address_line_1
    )
  })()




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

# Generate GeoPackages for any source files detected in ~/Raw/Census Bureau
# TIGER Line Shapefiles/ that are missing from ~/Results/Census Bureau TIGER
# Line Shapefiles/, based on the preceding status check.

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
## PART B: Algorithm to Standardize and Validate Entries

# Add blurb about what is being validated etc.

# PART B: Correct Addresses with USPS Database
# PART C: Resolve Records with No Address Match Found
# PART D: Verify Geolocation with the US Census Bureau’s Geocoder Database
# PART E: Add Census Information by GEO Coordinates
# PART F: Add Back to Main Dataset
# PART G: Quality Checks — Address Validation Results
# PART H: Quality Checks — Variation with Geolocation
# PART I: Quality Checks — Variation with Census Information


## --------------------
## SUBSECTION B1: Load API Keys and Define Search Space

# Load the USPS API Keys
Sys.getenv("R_ENVIRON_USER")
consumer_key <- Sys.getenv("USPS_CONSUMER_KEY", unset = "<UNSET>")
consumer_secret <- Sys.getenv("USPS_CONSUMER_SECRET", unset = "<UNSET>")

# Sort by state (to minimize number of geospatial data loaded)
# Index by array

# Prepare the dataframes used in the script
church_2026_form_dt <- as.data.table(church_2026_form)  # Convert for efficient data manipulation

#c(53496, 4)
index <- c(53496:53498)

# Define the search space
search_space <- unique(church_2026_form$abi)[index]


## --------------------
## SUBSECTION B2: Load Relevant Block-Level GeoPackages by State

# State block- and block group-level shapefiles were precompiled into 
# GeoPackage (*.gpkg) files containing all desired metadata (block or block 
# group, tract, county, and state).
# 
# Loading all files simultaneously is computationally prohibitive, so data
# is first sorted by state and then subset for processing. The following
# function loads only the GeoPackage required for the current batch.

# Define the geography level = c("blocks", "block group")
block_geography <- "block groups"

# Identify the unique states represented in the data.
states_present <- church_2026_form %>%
  filter(abi %in% search_space) %>%
  pull(state) %>%
  unique()

# Load the relevant GeoPackages as a nested list: states at the first level,
# decennial years at the second.
blocks_by_state <- read_state_gpkgs_for_data(
  states_present, "Data/Results/Census Bureau TIGER Line Shapefiles/", 
  geography = block_geography
)


## --------------------
## SUBSECTION B3: 

# Initialize the empty lists
finish_build <- vector("list", length(search_space))
qc_address   <- vector("list", length(search_space))
qc_geo       <- list(qc1 = list(), qc2 = list(), qc3 = list())
qc_census    <- list(qc1 = list(), qc2 = list())

# Initialize progress bar
pb = txtProgressBar(min = 0, max = length(search_space), style = 3)

for (i in 1:length(search_space)) {
  # Subset to only entries associated with one business ABI.
  subset <- church_2026_form_dt[abi %in% search_space[i]]
  
  # --------------------
  # PART A: Isolate Unique Candidate Addresses
  
  # For this script, it is not necessary to process each individually listed
  # address. Instead, they will be compressed down to the exact unique addresses
  # until the validation is completed, then rejoined with the remaining, whole
  # dataset.
  # 
  # To ensure this information relates back to subset, the following additions
  # are made:
  #     - Anchored min/max year to show the span of years that address was
  #       observed.
  #     - Geolocation (longitude/latitude) is averaged for each exact address.
  
  candidate_addresses <- subset %>%
    # Keep only the address fields plus a numeric year we can aggregate (and lat/lon for averaging)
    transmute(
      address_line_1, city, state, zipcode, zip4, combined_address,
      year = as.integer(archive_version_year),  # convert year to integer for min/max
      latitude, longitude
    ) %>%
    # Collapse multiple yearly rows down to one row per unique combined_address
    group_by(combined_address) %>%
    summarise(
      # Representative address fields to carry forward for this combined_address
      # (they may vary slightly over time; we keep the first observed)
      address_line_1 = dplyr::first(address_line_1),
      city           = dplyr::first(city),
      state          = dplyr::first(state),
      zipcode        = dplyr::first(zipcode),
      zip4           = dplyr::first(zip4),
      
      # Earliest year this combined_address appears in the chronological subset
      anchor_year_min = min(year, na.rm = TRUE),
      
      # Latest year this combined_address appears in the chronological subset
      anchor_year_max = max(year, na.rm = TRUE),
      
      # Mean lat/long per combined_address (and count of non-missing coordinate rows)
      n_geo = sum(!is.na(latitude) & !is.na(longitude)),
      latitude_avg  = ifelse(n_geo > 0, mean(latitude,  na.rm = TRUE), NA_real_),
      longitude_avg = ifelse(n_geo > 0, mean(longitude, na.rm = TRUE), NA_real_),
      
      # Return an ungrouped data frame
      .groups = "drop"
    ) %>%
    mutate(address_verified = NA, verified_address = NA)
  
  # Ensure the rows are ordered by ascending year of observation.
  setorder(candidate_addresses, anchor_year_min)
  
  # Prepare candidate_addresses for QC output:
  candidate_addresses <- candidate_addresses %>%
    #   - Initialize attempt_succeeded as NA to track which retry attempt
    #     validated each address and capture any non-hard-stop API errors
    #     (e.g. HTTP 400 "Address Not Found") for audit.
    mutate(attempt_succeeded = NA_character_) %>%
    #   - Initialize geolocation_test as NA for downstream geocoding checks.
    mutate(geolocation_test = NA_character_)

  
  # --------------------
  # PART B: Correct Addresses with USPS Database
  
  for (j in 1:nrow(candidate_addresses)) {
    
    # ---------------------------------------------------------------------------
    # Pull the j-th row into variables used by validate_usps_address().
    # ---------------------------------------------------------------------------
    address1 <- candidate_addresses$address_line_1[j]
    address2 <- ""
    city     <- candidate_addresses$city[j]
    state    <- candidate_addresses$state[j]
    zip5     <- candidate_addresses$zipcode[j]
    zip4     <- ifelse(is.na(candidate_addresses$zip4[j]), "", candidate_addresses$zip4[j])
    
    # Track which attempt succeeded and collect non-hard-stop failure reasons
    # across all attempts for audit/debugging purposes.
    attempt_succeeded <- NA_character_
    attempt_log       <- list()
    
    # ---------------------------------------------------------------------------
    # Attempt 1: validate using the original inputs as-is.
    # ---------------------------------------------------------------------------
    usps_validated <- validate_usps_address(
      consumer_key, consumer_secret,
      address1, address2, city, state, zip5, zip4
    )
    
    if (usps_validated$ok) {
      attempt_succeeded <- "First try"
    } else {
      attempt_log[[1]] <- list(attempt = 1L, status = usps_validated$status,
                               detail  = usps_validated$status_detail)
    }
    
    # ---------------------------------------------------------------------------
    # Attempt 2: if no match, assess/correct city via ZIP lookup, then retry.
    #
    # Nuanced so that imports that had the 5-digit code as a numeric, which
    # would cause the leading and trailing zeros to be stripped.
    # ---------------------------------------------------------------------------
    if (!usps_validated$ok) {
      
      if (zip_codes_character) {
        
        # Look up the city in the SimpleMaps U.S. Cities dataset.
        query_result <- zip5 %>%
          ifelse(is.na(.) || . == "", "", .) %>%
          (\(z) get_city_info(z, zip_city_lookup))()
        
        # If a match is found, re-query the USPS database to confirm.
        if (!str_detect(query_result, "No Matches")) {
          city <- query_result
          
          suppressWarnings({
            usps_validated <- validate_usps_address(
              consumer_key, consumer_secret,
              address1, address2, city, state, zip5, zip4
            )
          })
          
          if (usps_validated$ok) {
            attempt_succeeded <- "With city correction by zip5"
          } else {
            attempt_log[[2]] <- list(attempt = 2L, status = usps_validated$status,
                                     detail  = usps_validated$status_detail)
          }
        }
        
      } else if (!zip_codes_character) {
        
        # Leading/trailing zeros were stripped prior to receiving the raw data.
        # Some ZIP-to-city sources treat those edge zeros differently, so we test
        # multiple orientations by "sliding" the same count of edge zeros between
        # the front and back of the ZIP (still 5 digits).
        zip5_raw <- zip5 %>% ifelse(is.na(.) || . == "", "", .)
        zip5_raw <- ifelse(nzchar(zip5_raw),
                           str_pad(zip5_raw, width = 5, side = "left", pad = "0"), "")
        
        zip5_candidates <- make_zip5_candidates(zip5_raw) %>% .[. %!in% zip5]
        
        # Try candidates until one returns a city (then stop); otherwise do nothing.
        for (z in zip5_candidates) {
          query_result <- get_city_info(z, zip_city_lookup)
          
          if (!str_detect(query_result, "No Matches")) {
            city <- query_result
            zip5 <- z
            
            suppressWarnings({
              usps_validated <- validate_usps_address(
                consumer_key, consumer_secret,
                address1, address2, city, state, zip5, zip4
              )
            })
            
            if (usps_validated$ok) {
              attempt_succeeded <- "With city correction by zip5"
            } else {
              attempt_log[[2]] <- list(attempt = 2L, status = usps_validated$status,
                                       detail  = usps_validated$status_detail)
            }
            
            # Stop after the first candidate that yields a city, regardless of
            # whether the USPS call succeeded — avoid burning more candidates.
            break
          }
        }
      }
    }
    
    # ---------------------------------------------------------------------------
    # Save results back into candidate_addresses (single write block).
    # ---------------------------------------------------------------------------
    if (!usps_validated$ok) {
      
      # Nothing matched after all attempts — record failure reason from the last
      # non-empty attempt log entry for the most actionable status message.
      last_log <- Filter(Negate(is.null), attempt_log)
      last_log <- if (length(last_log) > 0) last_log[[length(last_log)]] else NULL
      
      candidate_addresses$address_verified[j]  <- FALSE
      candidate_addresses$verified_address[j]  <- "No address match found"
      candidate_addresses$attempt_succeeded[j] <- "None"
      
    } else {
      
      # Build a formatted address string ("line1, line2, city, state ZIP-EXT"),
      # treating blank/whitespace-only fields as NA to omit them from the output.
      candidate_addresses$address_verified[j]  <- TRUE
      candidate_addresses$attempt_succeeded[j] <- attempt_succeeded
      
      candidate_addresses$verified_address[j] <-
        paste(
          stringr::str_c(
            stats::na.omit(
              dplyr::na_if(
                stringr::str_trim(as.character(unlist(usps_validated[1:4]))),
                ""
              )
            ),
            collapse = ", "
          ),
          stringr::str_c(
            stats::na.omit(
              dplyr::na_if(
                stringr::str_trim(as.character(unlist(usps_validated[5:6]))),
                ""
              )
            ),
            collapse = "-"
          )
        ) |>
        stringr::str_trim() |>
        stringr::str_remove("-$") |>
        stringr::str_remove("\\s*$")
    }
  }
  
  # Re-cast candidate_addresses as a data.table for downstream processing.
  setDT(candidate_addresses)
  
  
  # --------------------
  # PART C: Resolve Records with No Address Match Found
  
  # Prepare candidate_addresses for QC output:
  candidate_addresses <- candidate_addresses %>%
    #   - Normalize address_verified to a "TRUE"/"FALSE" string.
    mutate(address_verified = as.numeric(address_verified)) %>%
    mutate(address_verified = as.character(address_verified)) %>%
    mutate(address_verified = ifelse(as.numeric(address_verified) == 1, "TRUE", "FALSE"))
  
  
  # For addresses that could not be verified, route them through a triage process
  # that attempts to associate each record with a verified address:
  # 
  #   a. Exact match on address_line_1 with differing metadata:
  #         - Link to the nearest chronologically preceding verified match
  #           (or following if no preceding match exists).
  #         - Bypass the geolocation validation step.
  # 
  #   b. Fuzzy match on address_line_1:
  #         - Link to the nearest chronologically verified match.
  #         - Apply the geolocation validation step.
  #         - If validation fails, retain the record as a separate unverified entry.
  
  if(any(candidate_addresses$verified_address %in% "No address match found")) {
    
    # Separate addresses into two groups: records to be matched and potential match candidates.
    match_to  <- candidate_addresses[address_verified == TRUE, ]
    unmatched <- candidate_addresses[address_verified == FALSE, ]
    
    # Assign a unique row index.
    unmatched[, u_id := .I]
    
    for(j in 1:nrow(unmatched)) {
      
      # Isolate the addresses for exact test comparison.
      comparisons_line_1 <- c(match_to$address_line_1, unmatched[j, address_line_1])
      
      # Exact match tests for the address_line_1 and whole address.
      match_line_1 <- find_similar_addresses(comparisons_line_1, threshold = 0)
      
      # -- a. Exact Match on address_line_1 with Conflicting Metadata ----------
      
      if( any(match_to$address_line_1 == unmatched[j, address_line_1]) ) {
        
        # Join verified candidates to unmatched records on address_line_1.
        # allow.cartesian = TRUE handles the many-to-many relationship.
        cand <- merge(
          match_to,
          unmatched[j, .(u_id, address_line_1, u_min = anchor_year_min, u_max = anchor_year_max)],
          by = "address_line_1",
          allow.cartesian = TRUE
        )
        
        # Apply preference rules for match selection:
        #     - Prefer the nearest chronologically preceding verified match.
        #     - If no preceding match exists, fall back to the nearest 
        #       following match.
        cand[, `:=`(
          is_before = anchor_year_max <= u_min,
          is_after  = anchor_year_min >= u_max,
          dist = fifelse(anchor_year_max < u_min, u_min - anchor_year_max,
                         fifelse(anchor_year_min > u_max, anchor_year_min - u_max, 0))
        )]
        
        # Select the best verified match per unmatched record using the 
        # preference rules.
        best <- cand[
          order(
            u_id,
            -is_before,                              # prefer prior
            -is_after,                               # else prefer after
            fifelse(is_before, -anchor_year_max, 0), # nearest prior
            fifelse(is_after,  anchor_year_min, 0),  # nearest after
            dist
          ),
          .SD[1],
          by = u_id
        ]
        
        # Update unmatched records with the associated verified address.
        unmatched[j, "verified_address"] <- best[1, verified_address]
        unmatched[j, "address_verified"] <- "Exact match"
        unmatched[j, "geolocation_test"] <- "Override"
        unmatched[j, "latitude_avg"] <- best[1, latitude_avg]
        unmatched[j, "longitude_avg"] <- best[1, longitude_avg]
        
        
      # -- b. Fuzzy Match on address_line_1 -----------------------------------
        
      } else {
        
        # -- i. Identify Candidate Fuzzy Matches -------------------------------
        
        # Fuzzy matching is run iteratively with decreasing similarity thresholds
        # until each unmatched address resolves to a single best candidate.
        # If no match is found at any threshold, the record falls through to
        # section iii.
        
        # Starting string similarity comparator (line_1 only)
        threshold_line1 <- 0.2
        
        repeat {
          # Find candidate matches at the current threshold
          match_line_1 <- find_similar_addresses(comparisons_line_1, threshold = threshold_line1)
          
          # Check whether the cluster with the unmatched record still has >2 candidate matches
          too_many_line1 <- unlist(lapply(match_line_1, function(x) any(x %in% unmatched[j, address_line_1]))) %>%
            (\(x) {match_line_1[x]})() %>%
            unlist() %>% 
            length() > 2
          
          # Stop when matches are sufficiently narrow, OR threshold hits 0,
          # OR (threshold < 0.2 and all are singletons)
          if (!too_many_line1 || threshold_line1 <= 0 || (threshold_line1 < 0.2 && all(vapply(match_line_1, length, integer(1)) == 1))) break
          
          # Tighten threshold and try again
          threshold_line1 <- max(0, threshold_line1 - 0.01)
        }
        
        # Check if the unmatched address got matched with any verified addresses
        match_check <- unlist(lapply(match_line_1, function(x) any(x %in% unmatched[j, address_line_1]))) %>%
          (\(x) {match_line_1[x]})() %>%
          unlist() %>%
          length()
        
        # -- ii.  Associate Candidates and Perform Geolocation Test ------------
        
        if ( match_check == 2 ) {
          
          # Bind the fuzzy-matched address_line_1 candidates to the unmatched 
          # record, retaining only the fields needed for geolocation testing.
          cand <- bind_cols(
            match_to[match_to$address_line_1 %in% match_line_1[vapply(match_line_1, length, integer(1)) > 1][[1]], ],
            unmatched[j, .(u_id, address_line_1, u_min = anchor_year_min, u_max = anchor_year_max, u_lat = latitude_avg, u_lon = longitude_avg)]
          )
          
          setDT(cand)
          
          # Apply preference rules for match selection:
          #     - Prefer the nearest chronologically preceding verified match.
          #     - If no preceding match exists, fall back to the nearest 
          #       following match.
          cand[, `:=`(
            is_before = anchor_year_max <= u_min,
            is_after  = anchor_year_min >= u_max,
            dist = fifelse(anchor_year_max < u_min, u_min - anchor_year_max,
                           fifelse(anchor_year_min > u_max, anchor_year_min - u_max, 0))
          )]
          
          # Test how similar the longitude and latitude are.
          #     negligible_change <- 0.002  # Change in degrees (~222 meters or 728 feet)
          cand[, `:=`(
            lat_diff = abs(latitude_avg  - u_lat),
            lon_diff = abs(longitude_avg - u_lon)
          )]
          
          cand[, geolocation_test := (lat_diff < 0.002) & (lon_diff < 0.002)]
          
          # Select the best verified match per unmatched record using the 
          # preference rules (ONLY among coordinate-close candidates).
          best <- cand[geolocation_test == TRUE][
            order(
              u_id,
              -is_before,                              # prefer prior
              -is_after,                               # else prefer after
              fifelse(is_before, -anchor_year_max, 0), # nearest prior
              fifelse(is_after,  anchor_year_min, 0),  # nearest after
              dist
            ),
            .SD[1],
            by = u_id
          ]
          
          if(nrow(best) == 0L) {
            
            # No candidate survived the geolocation test — flag as failed.
            unmatched[j, "geolocation_test"] <- "FALSE"
            
          } else {
            
            # Geolocation test passed — assign the top candidate as the verified address.
            unmatched[j, "verified_address"] <- best[1, verified_address]
            unmatched[j, "address_verified"] <- "Fuzzy match"
            unmatched[j, "geolocation_test"] <- "TRUE"
            unmatched[j, "latitude_avg"] <- best[1, latitude_avg]
            unmatched[j, "longitude_avg"] <- best[1, longitude_avg]
            
          }
          
          # -- iii. No Fuzzy Match Found ---------------------------------------
          
        } else if ( threshold_line1 < 0.2 && match_check > 2 ) {
          
          # Threshold reached 0 before candidates could be separated, or the
          # step-wise threshold failed to resolve to a single candidate —
          # no single best match could be isolated.
          unmatched[j, "address_verified"] <- "No Separable Match"
          
        } else if ( match_check == 1 ) {
          
          # No verified match exists at any threshold.
          unmatched[j, "address_verified"] <- "FALSE"
          
        } else {
          
          # Fallback: condition was unanticipated — flag for manual review.
          unmatched[j, "address_verified"] <- "Unanticipated Error"
          
        }
      }
      
    }
    
    # Recombine all separated dataset partitions into a single unified dataset
    common <- Reduce(intersect, list(names(match_to), names(unmatched)))
    candidate_addresses <- rbindlist(list(match_to[, ..common], unmatched[, ..common]), use.names = TRUE)
  }
  
  # Organize columns
  candidate_addresses <- candidate_addresses %>%
    relocate(attempt_succeeded, .after = address_verified) %>%
    relocate(geolocation_test, .after = attempt_succeeded)
  setDT(candidate_addresses)
  
  # Ensure the rows are ordered by ascending year of observation.
  setorder(candidate_addresses, anchor_year_min)
  
  
  # --------------------
  # PART D: Verify Geolocation with the US Census Bureau’s Geocoder Database
  
  # helper to safely pull a field from each attempt
  pluck_or_na <- function(x, name) if (!is.null(x[[name]])) as.character(x[[name]]) else NA_character_
  
  out_row <- list()
  for (j in 1:nrow(candidate_addresses)) {
    address_to_check <- candidate_addresses[j, ] %>% 
      (\(x) {ifelse(
        !is.na(x$verified_address) & x$verified_address != "No address match found",
        x$verified_address,
        x$combined_address
        )
      })()
    
    # -- i. Parse the Address into Its Components -----------------------------
    
    # Split the address into its components (assuming comma-separated format).
    parts <- trimws(strsplit(address_to_check, ",")[[1]])
    
    if (length(parts) < 3) {
      warning("Invalid address format. Please provide a full address.")
      return(NULL)
    }
    
    if (length(parts) == 3) {
      # street, city, "ST ZIP"
      street <- parts[1]
      city   <- parts[2]
      
      state_zip <- strsplit(parts[3], "\\s+")[[1]]
      if (length(state_zip) < 2) {
        warning("Invalid state and ZIP code format. Please provide a full address.")
        return(NULL)
      }
      state <- state_zip[1]
      zip   <- state_zip[2]
      
    } else {
      # street may contain commas (address line 2), and state/zip may be split by commas.
      # Take the last tokens as state + zip, and the one before them as city.
      last <- parts[length(parts)]
      
      # Case A: ... , ST ZIP
      # Case B: ... , ST, ZIP
      if (grepl("\\b[A-Z]{2}\\b\\s+\\d", last)) {
        # last is "ST ZIP"
        state_zip <- strsplit(last, "\\s+")[[1]]
        state <- state_zip[1]
        zip   <- state_zip[2]
        city  <- parts[length(parts) - 1]
        street <- paste(parts[1:(length(parts) - 2)], collapse = ", ")
      } else {
        # assume last is ZIP and previous is state
        zip   <- last
        state <- parts[length(parts) - 1]
        city  <- parts[length(parts) - 2]
        street <- paste(parts[1:(length(parts) - 3)], collapse = ", ")
      }
    }
    
    
    # -- ii. Run the Query ----------------------------------------------------
    
    res <- validate_geolocation(
      street = street,
      city   = city,
      state  = state,
      zip    = zip,
      tries  = geocoder_census_tries,
      quiet = TRUE
    )
    
    
    # -- iii. Save Result -----------------------------------------------------
    
    if (res$ok) {
      succeeded_i <- which(vapply(res$attempts, function(a) isTRUE(a$ok), logical(1)))[1]
      
      search_res <- tibble::tibble(
        geolocation_verified = TRUE,
        n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
        query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                             FUN.VALUE = character(1))),
                                              collapse = " | "),
        matched_address      = res$best$matched_address,
        benchmark            = res$best$benchmark,
        vintage_input        = res$best$vintage_input,
        latitude_ver         = res$best$lat,
        longitude_ver        = res$best$lon
      )
      
      out_row[[j]] <- dplyr::bind_cols(candidate_addresses[j, , drop = FALSE], search_res)
      
    } else {
      search_res <- tibble::tibble(
        geolocation_verified = FALSE,
        n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
        query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                             FUN.VALUE = character(1))),
                                              collapse = " | "),
        matched_address      = NA_character_,
        benchmark            = NA_real_,
        vintage_input        = NA_character_,
        latitude_ver         = NA_real_,
        longitude_ver        = NA_real_
      )
      
      out_row[[j]] <- dplyr::bind_cols(candidate_addresses[j, , drop = FALSE], search_res)
    }
  }
  
  # Save results
  candidate_addresses <- bind_rows(out_row)
  
  
  # --------------------
  # PART E: Add Census Information by GEO Coordinates
  
  candidate_addresses <- candidate_addresses %>%
    mutate(
      # Create a stable key for joining results back later
      row_id = row_number(),
      
      # Prefer verified coordinates; fall back to averaged coordinates
      lon = coalesce(longitude_ver, longitude_avg),
      lat = coalesce(latitude_ver,  latitude_avg),
      
      # Flag rows that have enough information to attempt spatial matching
      enough_info = !(is.na(lon) | is.na(lat) | is.na(state))
    )
  
  setDT(candidate_addresses)
  
  # Build an sf points table (only rows with usable lon/lat/state)
  cand_sf <- candidate_addresses %>%
    transmute(row_id, state, lon, lat) %>%
    filter(!is.na(lon), !is.na(lat), !is.na(state)) %>%
    mutate(state = toupper(trimws(state))) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  
  # --- Add DECENNIAL census geography (Census blocks) for each vintage ---
  # Adds: geoid_2000, geoid_2010, geoid_2020 (block GEOIDs)
  census_results <- add_decennial_geoid_block(
    cand_sf, 
    blocks_by_state, 
    geography = block_geography
  )
  
  setDT(census_results)
  
  # --- Add METRO/MICRO areas (CBSA) and CSA codes for each vintage ---
  # Adds: cbsa_2007/csa_2007, cbsa_2010/csa_2010, and cbsa_2020/csa_2020
  # Adds: zcta_2000, zcta_2010, and zcta_2020
  
  vintages <- list(
    list(cbsa_csa_year = 2007, cbsa_csa = core_areas$cbsa_csa_2007, zcta_year = "zcta_2000", zcta = core_areas$zcta_2000),
    list(cbsa_csa_year = 2010, cbsa_csa = core_areas$cbsa_csa_2010, zcta_year = "zcta_2010", zcta = core_areas$zcta_2010),
    list(cbsa_csa_year = 2020, cbsa_csa = core_areas$cbsa_csa_2020, zcta_year = "zcta_2020", zcta = core_areas$zcta_2020)
  )
  
  # Process each vintage and collect results
  core_areas_list <- lapply(vintages, function(v) {
    
    out <- list()
    
    # Decode CBSA/CSA for this vintage if a layer is provided
    if (!is.null(v$cbsa_csa)) {
      out$cbsa_csa <- decode_cbsa_csa(
        cand_sf     = cand_sf,
        cbsa_csa_sf = v$cbsa_csa,
        year        = v$cbsa_csa_year,
        state_col   = "state"
      )
    }
    
    # Decode ZCTA for this vintage if a layer is provided
    if (!is.null(v$zcta)) {
      out$zcta <- decode_zcta(
        cand_sf      = cand_sf,
        zcta_sf      = v$zcta,
        zcta_colname = v$zcta_year,
        state_col    = "state"
      )
    }
    
    out
  })
  
  # Join all results back onto cand_sf
  core_areas_results <- sf::st_drop_geometry(cand_sf)
  
  for (v in core_areas_list) {
    
    if (!is.null(v$cbsa_csa)) {
      core_areas_results <- dplyr::left_join(core_areas_results, v$cbsa_csa, by = "row_id")
    }
    
    if (!is.null(v$zcta)) {
      core_areas_results <- dplyr::left_join(core_areas_results, v$zcta, by = "row_id")
    }
  }
  
  setDT(core_areas_results)
  
  # Join DECENNIAL block GEOIDs back onto candidate_addresses
  cols_to_add <- c("row_id", "geoid_2000", "geoid_2010", "geoid_2020")
  candidate_addresses[census_results[, ..cols_to_add], on = "row_id",
                      `:=`(
                        geoid_2000 = i.geoid_2000,
                        geoid_2010 = i.geoid_2010,
                        geoid_2020 = i.geoid_2020
                      )]
  
  # Join METRO/MICRO CBSA + CSA codes back onto candidate_addresses
  cols_to_add <- c(
    "row_id",
    "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007",
    "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010",
    "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020",
    "zcta_2000", "zcta_2010", "zcta_2020"
  )
  candidate_addresses[core_areas_results[, ..cols_to_add], on = "row_id",
                      `:=`(
                        cbsa_code_2007  = i.cbsa_code_2007,
                        cbsa_level_2007 = i.cbsa_level_2007,
                        csa_code_2007   = i.csa_code_2007,
                        cbsa_code_2010  = i.cbsa_code_2010,
                        cbsa_level_2010 = i.cbsa_level_2010,
                        csa_code_2010   = i.csa_code_2010,
                        cbsa_code_2020  = i.cbsa_code_2020,
                        cbsa_level_2020 = i.cbsa_level_2020,
                        csa_code_2020   = i.csa_code_2020,
                        zcta_2000       = i.zcta_2000,
                        zcta_2010       = i.zcta_2010,
                        zcta_2020       = i.zcta_2020
                      )]
  
  # Summarize match completeness across the three decennial block vintages
  candidate_addresses[, geoid_match :=
                        fifelse(
                          !enough_info,
                          "Not enough info",
                          fifelse(
                            is.na(geoid_2000) & is.na(geoid_2010) & is.na(geoid_2020),
                            "Matches not found",
                            fifelse(
                              is.na(geoid_2000) | is.na(geoid_2010) | is.na(geoid_2020),
                              "Some matches not found",
                              "Matched"
                            )
                          )
                        )]
  
  # Drop temporary helper columns and organize columns
  candidate_addresses <- candidate_addresses %>%
    select(-anchor_year_min, -anchor_year_max, -row_id, -lon, -lat, -enough_info) %>%
    relocate(geoid_match, .after = longitude_ver)
  
  
  # --------------------
  # PART F: Add Back to Main Dataset
  
  # Define column structure
  combined_cols <- c(
    # ---- company / identifiers ----
    "archive_version_year", "company", "abi", "year_established", 
    "primary_naics6_code", "naics6_descriptions",
    "subsidiary_number", "company_holding_status",
    
    # ---- address ----
    "address_line_1", "city", "state", "zipcode", "zip4", "combined_address",
    "address_verified", "attempt_succeeded",   # validation cols
    "geolocation_test", "verified_address",    # validation cols
    
    # ---- industry codes / descriptions ----
    "primary_sic_code", "sic6_descriptions",
    "sic_code", "sic6_descriptions_sic",
    "sic_code_1", "sic6_descriptions_sic1",
    "sic_code_2", "sic6_descriptions_sic2",
    "sic_code_3", "sic6_descriptions_sic3",
    "sic_code_4", "sic6_descriptions_sic4",
    
    # ---- geocoding / verification ----
    "latitude", "longitude",
    "n_geo", "latitude_avg", "longitude_avg",                     # validation cols
    "geolocation_verified", "n_attempts", "query_statuses",       # validation cols
    "matched_address", "benchmark", "vintage_input",              # validation cols
    "latitude_ver", "longitude_ver",                              # validation cols
    
    # ---- census geography (current + vintages) ----
    "census_block", "census_tract", "county_code", "fips_code",
    "geoid_match", "geoid_2000", "geoid_2010", "geoid_2020",      # validation cols
    
    # ---- CBSA/CSA (current + vintages) and ZCTA (new) ----
    "cbsa_level", "cbsa_code", "csa_code",
    "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007", "zcta_2000",  # validation cols
    "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010", "zcta_2010",  # validation cols
    "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020", "zcta_2020",  # validation cols
    
    # ---- other firmographic fields ----
    "area_code", "site_number", "yellow_page_code", "office_size_code",
    "employee_size_location", "location_employee_size_code",
    "sales_volume_location", "location_sales_volume_code",
    "parent_number", "parent_actual_employee_size", "parent_employee_size_code",
    "parent_actual_sales_volume", "parent_sales_volume_code",
    
    # ---- status / misc identifiers ----
    "primary_naics2_code", "address_type_indicator", 
    "industry_specific_first_byte", "business_status_code",
    "population_code", "match_code", "ticker", "idcode"
  )
  
  # Merge the two datasets
  combined <- merge(
      candidate_addresses,
      subset,
      by = c("address_line_1", "city", "state", "zipcode", "zip4", "combined_address"),
      all.x = TRUE
    ) %>%
    select(all_of(combined_cols))
  
  setorder(combined, archive_version_year)
  
  
  # --------------------
  # PART G: Quality Checks — Address Validation Results
  
  # Address verification proceeds in two stages, summarised across three columns:
  #
  #   Stage 1 — USPS API (attempt_succeeded): each address is submitted to the
  #   USPS API with up to two attempts. The second attempt corrects the city via
  #   ZIP lookup before retrying.
  #
  #   Stage 2 — String matching (geolocation_test): addresses that failed Stage 1
  #   are matched against verified addresses using address_line_1 string
  #   comparisons to inherit a verified result.
  #
  #   address_verified reflects the final outcome of both stages.
  
  # Address QC: summarize verification stages
  qc_address[[i]] <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = coalesce(verified_address, combined_address)
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- address ----
      combined_address,
      
      # ---- verification ----
      address_verified, attempt_succeeded,   # validation cols
      geolocation_test, verified_address,    # validation cols
    ) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    (\(x) {setDT(x)} )()
  
  
  # --------------------
  # PART H: Quality Checks — Variation with Geolocation
    
  # Assesses the reliability of reported geolocation values for each address
  # using two approaches:
  #
  #   1) Deviation test: compare validated coordinates to reported/averaged coords.
  #   2) Dispersion test: measure spread of reported coords for the same address
  #      when the address appears multiple times.
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_geo_df <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = coalesce(verified_address, combined_address)
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- geocoding ----
      latitude, longitude,              # reported coords (if present)
      latitude_avg, longitude_avg,      # averaged coords
      latitude_ver, longitude_ver,      # validated coords
      
      # ---- verification ----
      geolocation_verified, n_attempts, query_statuses, 
      matched_address, benchmark, vintage_input
    )
  
  
  # (a) Query QC: number of attempts, API status code, vintage used
  qc_geo$qc1[[i]] <- qc_geo_df %>%
    select(-latitude, -longitude, -latitude_avg, -longitude_avg, 
           -latitude_ver, -longitude_ver) %>%
    mutate(
      all_200 = ifelse(
        is.na(query_statuses) | lengths(strsplit(query_statuses, " \\| ")) < length(geocoder_census_tries),
        NA,
        sapply(strsplit(query_statuses, " \\| "), function(x) all(trimws(x) == "200"))
      ),
      matched_address_same = case_when(
        is.na(address) | is.na(matched_address) ~ NA,
        TRUE ~ stringr::str_remove_all(stringr::str_remove(stringr::str_squish(address),        "-\\d{4}$"), ",(?=\\s*\\d{5})") ==
          stringr::str_remove_all(stringr::str_remove(stringr::str_squish(matched_address), "-\\d{4}$"), ",(?=\\s*\\d{5})")
      )
    ) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    relocate(all_200, .after = query_statuses) %>%
    relocate(matched_address_same, .after = matched_address) %>%
    (\(x) {setDT(x)} )()
  
  
  # (b) Deviation QC: how far validated coords are from averaged coords (rounded)
  qc_geo$qc2[[i]] <- qc_geo_df %>%
    # Only evaluate deviation when validated coordinates exist
    filter(!is.na(latitude_ver), !is.na(longitude_ver)) %>%
    mutate(
      # Absolute differences (rounded to 4 decimals)
      lat_abs_diff = if_else(!is.na(latitude_ver),  round(abs(latitude_avg  - latitude_ver),  4), NA_real_),
      lon_abs_diff = if_else(!is.na(longitude_ver), round(abs(longitude_avg - longitude_ver), 4), NA_real_),
      
      # Flag whether the deviation exceeds the threshold
      lat_gt_002 = if_else(!is.na(lat_abs_diff), lat_abs_diff > 0.002, NA),
      lon_gt_002 = if_else(!is.na(lon_abs_diff), lon_abs_diff > 0.002, NA)
    ) %>%
    # Keep only QC outputs + identifiers
    select(abi, archive_version_year, address,
           lat_abs_diff, lat_gt_002, lon_abs_diff, lon_gt_002) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(lat_gt_002, .after = lat_abs_diff) %>%
    relocate(archive_versions_present, .after = address) %>%
    (\(x) {setDT(x)} )()
  
  # (c) Dispersion QC: summarize the spread of reported coords within (abi, address)
  qc_geo$qc3[[i]] <- qc_geo_df %>%
    transmute(abi, address, latitude, longitude) %>%
    group_by(abi, address) %>%
    summarise(
      # Count complete (lat & lon) pairs available for this (abi, address)
      n_geo = sum(!is.na(latitude) & !is.na(longitude)),
      
      # Latitude distribution summary (return NA if no latitude values)
      lat_min    = ifelse(sum(!is.na(latitude)) > 0, min(latitude, na.rm = TRUE), NA_real_),
      lat_q1     = ifelse(sum(!is.na(latitude)) > 0, quantile(latitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lat_median = ifelse(sum(!is.na(latitude)) > 0, median(latitude, na.rm = TRUE), NA_real_),
      lat_mean   = ifelse(sum(!is.na(latitude)) > 0, mean(latitude, na.rm = TRUE), NA_real_),
      lat_q3     = ifelse(sum(!is.na(latitude)) > 0, quantile(latitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lat_max    = ifelse(sum(!is.na(latitude)) > 0, max(latitude, na.rm = TRUE), NA_real_),
      
      # Longitude distribution summary (return NA if no longitude values)
      lon_min    = ifelse(sum(!is.na(longitude)) > 0, min(longitude, na.rm = TRUE), NA_real_),
      lon_q1     = ifelse(sum(!is.na(longitude)) > 0, quantile(longitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lon_median = ifelse(sum(!is.na(longitude)) > 0, median(longitude, na.rm = TRUE), NA_real_),
      lon_mean   = ifelse(sum(!is.na(longitude)) > 0, mean(longitude, na.rm = TRUE), NA_real_),
      lon_q3     = ifelse(sum(!is.na(longitude)) > 0, quantile(longitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lon_max    = ifelse(sum(!is.na(longitude)) > 0, max(longitude, na.rm = TRUE), NA_real_),
      
      # Flag if the spread of lat or lon across observations exceeds 0.02 degrees
      lat_spread_gt_002 = abs(max(latitude, na.rm = TRUE) - min(latitude, na.rm = TRUE)) > 0.02,
      lon_spread_gt_002 = abs(max(longitude, na.rm = TRUE) - min(longitude, na.rm = TRUE)) > 0.02,
      
      .groups = "drop"
    ) %>%
    # Only keep addresses with multiple geo observations (so "spread" is meaningful)
    filter(n_geo > 1) %>%
    relocate(lat_spread_gt_002, .after = lat_max) %>%
    (\(x) {setDT(x)} )()
  
  
  # --------------------
  # PART H: Quality Checks — Variation with Census Information
  
  # Assesses the reliability of reported census boundry values for each address.
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_census_df <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = coalesce(verified_address, combined_address)
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- census geography ----
      census_block, census_tract, county_code, fips_code,   # reported coords (if present)
      geoid_match, geoid_2000, geoid_2010, geoid_2020,      # validated coords
      
      # ---- CBSA/CSA (current + vintages) ----
      cbsa_level, cbsa_code, csa_code,                   # reported coords (if present)
      cbsa_code_2007, cbsa_level_2007, csa_code_2007,    # validated coords
      cbsa_code_2010, cbsa_level_2010, csa_code_2010,    # validated coords
      cbsa_code_2020, cbsa_level_2020, csa_code_2020     # validated coords
    )
  
  
  # Determined once before the pipeline — drives substring width for all vintages.
  #   "Blocks"       -> substr(geoid, 12, 15)  matches 4-character block code
  #   "Block Groups" -> substr(geoid, 12, 12)  matches 1-character block group code
  block_substr_end <- if (block_geography == "blocks") 15L else 12L
  
  # (a) Census Boundaries QC: compare given and verified census boundaries
  qc_census$qc1[[i]] <- qc_census_df %>%
    filter(geoid_match %in% "Matched") %>%
    mutate(
      # ---- coerce + standardize ----
      fips_code_chr    = as.character(fips_code),           # expect 5
      county_code_chr  = as.character(county_code),         # expect 3
      census_tract_chr = as.character(census_tract),        # expect 6
      census_block_chr = as.character(census_block),        # expect 4 (Block) or 1 (Block Group)
      
      # ---- length checks ----
      fips_ok         = str_length(fips_code_chr)    == 5,
      county_ok       = str_length(county_code_chr)  == 3,
      tract_ok        = str_length(census_tract_chr) == 6,
      
      # ---- fips_code (positions 1-5) ----
      fips_match_2000 = fips_ok & substr(geoid_2000, 1, 5) == fips_code_chr,
      fips_match_2010 = fips_ok & substr(geoid_2010, 1, 5) == fips_code_chr,
      fips_match_2020 = fips_ok & substr(geoid_2020, 1, 5) == fips_code_chr,

      fips_code_any_match = if_else(fips_ok, fips_match_2000 | fips_match_2010 | fips_match_2020, FALSE),
      fips_vintages_raw = paste0(
        if_else(fips_match_2000, "2000, ", ""),
        if_else(fips_match_2010, "2010, ", ""),
        if_else(fips_match_2020, "2020, ", "")
      ),
      fips_vintages = case_when(
        !fips_ok ~ NA_character_,
        !fips_code_any_match ~ "None",
        TRUE ~ str_remove(fips_vintages_raw, ", $")
      ),

      # ---- county_code (positions 3-5) ----
      county_match_2000 = county_ok & substr(geoid_2000, 3, 5) == county_code_chr,
      county_match_2010 = county_ok & substr(geoid_2010, 3, 5) == county_code_chr,
      county_match_2020 = county_ok & substr(geoid_2020, 3, 5) == county_code_chr,

      county_code_any_match = if_else(county_ok, county_match_2000 | county_match_2010 | county_match_2020, FALSE),
      county_vintages_raw = paste0(
        if_else(county_match_2000, "2000, ", ""),
        if_else(county_match_2010, "2010, ", ""),
        if_else(county_match_2020, "2020, ", "")
      ),
      county_vintages = case_when(
        !county_ok ~ NA_character_,
        !county_code_any_match ~ "None",
        TRUE ~ str_remove(county_vintages_raw, ", $")
      ),

      # ---- census_tract (positions 6-11) ----
      tract_match_2000 = tract_ok & substr(geoid_2000, 6, 11) == census_tract_chr,
      tract_match_2010 = tract_ok & substr(geoid_2010, 6, 11) == census_tract_chr,
      tract_match_2020 = tract_ok & substr(geoid_2020, 6, 11) == census_tract_chr,

      census_tract_any_match = if_else(tract_ok, tract_match_2000 | tract_match_2010 | tract_match_2020, FALSE),
      tract_vintages_raw = paste0(
        if_else(tract_match_2000, "2000, ", ""),
        if_else(tract_match_2010, "2010, ", ""),
        if_else(tract_match_2020, "2020, ", "")
      ),
      tract_vintages = case_when(
        !tract_ok ~ NA_character_,
        !census_tract_any_match ~ "None",
        TRUE ~ str_remove(tract_vintages_raw, ", $")
      ),
      
      # Classify block resolution based on the length of the raw block code:
      #   str_length == 4  -> full block-level precision
      #   str_length == 1  -> block-group-level precision
      #   anything else    -> NA (unexpected format, surfaces upstream issues)
      block_ok = case_when(
        str_length(census_block_chr) == 4 ~ "Block",
        str_length(census_block_chr) == 1 ~ "Block Group",
        TRUE                              ~ "FALSE"
      ),
      
      # ---- census_block ----
      block_match_2000 = block_ok != "FALSE" &
        substr(geoid_2000, 12, block_substr_end) == census_block_chr,
      block_match_2010 = block_ok != "FALSE" &
        substr(geoid_2010, 12, block_substr_end) == census_block_chr,
      block_match_2020 = block_ok != "FALSE" &
        substr(geoid_2020, 12, block_substr_end) == census_block_chr,
      
      census_block_any_match = if_else(
        block_ok != "FALSE",
        block_match_2000 | block_match_2010 | block_match_2020,
        FALSE
      ),
      
      block_vintages_raw = paste0(
        if_else(block_match_2000, "2000, ", ""),
        if_else(block_match_2010, "2010, ", ""),
        if_else(block_match_2020, "2020, ", "")
      ),
      
      block_vintages = case_when(
        block_ok == "FALSE"     ~ NA_character_,
        !census_block_any_match ~ "None",
        TRUE                    ~ str_remove(block_vintages_raw, ", $")
      )
    ) %>%
    select(abi, archive_version_year, address, census_block, census_tract, 
           county_code, fips_code, geoid_2000, geoid_2010, geoid_2020, 
           fips_code_any_match, fips_vintages, county_code_any_match,
           county_vintages, census_tract_any_match, tract_vintages, block_ok, 
           census_block_any_match, block_vintages) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    (\(x) {setDT(x)} )()
  
  
  # (b) Metro/Micro QC: compare given and verified micro/metro census boundaries
  qc_census$qc2[[i]] <- qc_census_df %>% 
    mutate(
      # ---- coerce to character for safe comparisons ----
      cbsa_level_chr = as.character(cbsa_level),
      cbsa_code_chr  = as.character(cbsa_code),
      csa_code_chr   = as.character(csa_code),
      
      cbsa_level_2007_chr = as.character(cbsa_level_2007),
      cbsa_code_2007_chr  = as.character(cbsa_code_2007),
      csa_code_2007_chr   = as.character(csa_code_2007),
      
      cbsa_level_2010_chr = as.character(cbsa_level_2010),
      cbsa_code_2010_chr  = as.character(cbsa_code_2010),
      csa_code_2010_chr   = as.character(csa_code_2010),
      
      cbsa_level_2020_chr = as.character(cbsa_level_2020),
      cbsa_code_2020_chr  = as.character(cbsa_code_2020),
      csa_code_2020_chr   = as.character(csa_code_2020),
      
      # ---- flags: is the input missing? are all vintages missing? ----
      cbsa_level_input_na = is.na(cbsa_level_chr),
      cbsa_code_input_na  = is.na(cbsa_code_chr),
      csa_code_input_na   = is.na(csa_code_chr),
      
      cbsa_level_all_vintages_na = is.na(cbsa_level_2007_chr) & is.na(cbsa_level_2010_chr) & is.na(cbsa_level_2020_chr),
      cbsa_code_all_vintages_na  = is.na(cbsa_code_2007_chr)  & is.na(cbsa_code_2010_chr)  & is.na(cbsa_code_2020_chr),
      csa_code_all_vintages_na   = is.na(csa_code_2007_chr)   & is.na(csa_code_2010_chr)   & is.na(csa_code_2020_chr),
      
      # ---- OPTIONAL validity checks (edit if needed) ----
      cbsa_code_ok  = !cbsa_code_input_na  & str_length(cbsa_code_chr) == 5,
      csa_code_ok   = !csa_code_input_na   & str_length(csa_code_chr)  == 3,
      cbsa_level_ok = !cbsa_level_input_na & str_length(str_trim(cbsa_level_chr)) > 0,
      
      # ===================== CBSA CODE =====================
      cbsa_code_match_2007 = cbsa_code_ok & !is.na(cbsa_code_2007_chr) & cbsa_code_chr == cbsa_code_2007_chr,
      cbsa_code_match_2010 = cbsa_code_ok & !is.na(cbsa_code_2010_chr) & cbsa_code_chr == cbsa_code_2010_chr,
      cbsa_code_match_2020 = cbsa_code_ok & !is.na(cbsa_code_2020_chr) & cbsa_code_chr == cbsa_code_2020_chr,
      
      cbsa_code_any_match = case_when(
        cbsa_code_input_na ~ NA,               # input is NA -> NA
        cbsa_code_all_vintages_na ~ NA,        # all vintage fields NA -> NA
        !cbsa_code_ok ~ FALSE,                 # input present but invalid -> FALSE
        TRUE ~ (cbsa_code_match_2007 | cbsa_code_match_2010 | cbsa_code_match_2020)
      ),
      cbsa_code_vintages_raw = paste0(
        if_else(cbsa_code_match_2010, "2010, ", ""),
        if_else(cbsa_code_match_2020, "2020, ", "")
      ),
      cbsa_code_vintages = case_when(
        cbsa_code_input_na ~ NA_character_,
        cbsa_code_all_vintages_na ~ NA_character_,
        is.na(cbsa_code_any_match) ~ NA_character_,
        !cbsa_code_any_match ~ "None",
        TRUE ~ str_remove(cbsa_code_vintages_raw, ", $")
      ),
      
      # ===================== CBSA LEVEL =====================
      cbsa_level_match_2007 = cbsa_level_ok & !is.na(cbsa_level_2007_chr) & cbsa_level_chr == cbsa_level_2007_chr,
      cbsa_level_match_2010 = cbsa_level_ok & !is.na(cbsa_level_2010_chr) & cbsa_level_chr == cbsa_level_2010_chr,
      cbsa_level_match_2020 = cbsa_level_ok & !is.na(cbsa_level_2020_chr) & cbsa_level_chr == cbsa_level_2020_chr,
      
      cbsa_level_any_match = case_when(
        cbsa_level_input_na ~ NA,
        cbsa_level_all_vintages_na ~ NA,
        !cbsa_level_ok ~ FALSE,
        TRUE ~ (cbsa_level_match_2007 | cbsa_level_match_2010 | cbsa_level_match_2020)
      ),
      cbsa_level_vintages_raw = paste0(
        if_else(cbsa_level_match_2007, "2007, ", ""),
        if_else(cbsa_level_match_2010, "2010, ", ""),
        if_else(cbsa_level_match_2020, "2020, ", "")
      ),
      cbsa_level_vintages = case_when(
        cbsa_level_input_na ~ NA_character_,
        cbsa_level_all_vintages_na ~ NA_character_,
        is.na(cbsa_level_any_match) ~ NA_character_,
        !cbsa_level_any_match ~ "None",
        TRUE ~ str_remove(cbsa_level_vintages_raw, ", $")
      ),
      
      # ===================== CSA CODE =====================
      csa_code_match_2007 = csa_code_ok & !is.na(csa_code_2007_chr) & csa_code_chr == csa_code_2007_chr,
      csa_code_match_2010 = csa_code_ok & !is.na(csa_code_2010_chr) & csa_code_chr == csa_code_2010_chr,
      csa_code_match_2020 = csa_code_ok & !is.na(csa_code_2020_chr) & csa_code_chr == csa_code_2020_chr,
      
      csa_code_any_match = case_when(
        csa_code_input_na ~ NA,
        csa_code_all_vintages_na ~ NA,
        !csa_code_ok ~ FALSE,
        TRUE ~ (csa_code_match_2007 | csa_code_match_2010 | csa_code_match_2020)
      ),
      csa_code_vintages_raw = paste0(
        if_else(csa_code_match_2007, "2007, ", ""),
        if_else(csa_code_match_2010, "2010, ", ""),
        if_else(csa_code_match_2020, "2020, ", "")
      ),
      csa_code_vintages = case_when(
        csa_code_input_na ~ NA_character_,
        csa_code_all_vintages_na ~ NA_character_,
        is.na(csa_code_any_match) ~ NA_character_,
        !csa_code_any_match ~ "None",
        TRUE ~ str_remove(csa_code_vintages_raw, ", $")
      )
    ) %>%
    select(
      abi, archive_version_year, address,
      cbsa_level, cbsa_code, csa_code,
      cbsa_level_2007, cbsa_code_2007, csa_code_2007,
      cbsa_level_2010, cbsa_code_2010, csa_code_2010,
      cbsa_level_2020, cbsa_code_2020, csa_code_2020,
      cbsa_code_any_match, cbsa_code_vintages,
      cbsa_level_any_match, cbsa_level_vintages,
      csa_code_any_match, csa_code_vintages
    ) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    (\(x) {setDT(x)} )()


  # --------------------
  # PART I: Commit Results
  
  finish_build[[i]] <- combined %>%
    select(-latitude, -longitude, -n_geo, -census_block, -census_tract,
           -county_code, -fips_code, -cbsa_level, -cbsa_code, -csa_code)
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}

# Combine all data tables in the list into one data table.
finish_build <- rbindlist(finish_build, use.names = TRUE, fill = TRUE)

qc_address <- rbindlist(qc_address, use.names = TRUE, fill = TRUE)

qc_geo$qc1 <- dplyr::bind_rows(qc_geo$qc1, .id = "i")
qc_geo$qc2 <- dplyr::bind_rows(qc_geo$qc2, .id = "i")
qc_geo$qc3 <- dplyr::bind_rows(qc_geo$qc3, .id = "i")

qc_census$qc1 <- dplyr::bind_rows(qc_census$qc1, .id = "i")
qc_census$qc2 <- dplyr::bind_rows(qc_census$qc2, .id = "i")


# Commit results.
write.csv(finish_build, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B_04.22.2026.csv")

# Read in previously generated results.
finish_build <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B_04.22.2026.csv", 
                         col_types = cols(...1 = col_skip())) %>% as.data.frame()




## ----------------------------------------------------------------
## PART C: Recompile Results from the HPC






## ----------------------------------------------------------------
## PART D: Cleaning for Saving the Result


finish_build %>% duplicated() %>% any()


finish_build[, c(1, 3, 9:17, 30:52)]

finish_build[, c(1, 3)] %>% duplicated() %>% any()

table(finish_build$address_verified, useNA = "ifany")
table(finish_build$geolocation_verified, useNA = "ifany")
table(finish_build$geoid_match, useNA = "ifany")




# Save the part of the raw data where there are no duplicates.
no_duplicates <- church_wide %>% 
  (\(x) { x[x$abi %!in% duplicates_detected$abi, ] }) () %>% 
  `rownames<-`(NULL)

## --------------------
## SUBSECTION D4: Organize and Save the Results

# Now we do some formatting to finish this step.
step_1 <- step_1 %>%
  # Sort the rows by descending ABI.
  arrange(abi) %>%
  # Some ZIP codes are incorrectly formatted due to a missing leading or trailing
  # zero, resulting in codes that are fewer than five digits. To standardize these,
  # we pad them to five digits as follows: four-digit ZIP codes receive a leading
  # zero, and three-digit ZIP codes receive both a leading and a trailing zero.
  #
  # Note: These corrections are provisional. Address validation will be performed
  # later to verify and, where necessary, correct this information.
  mutate(zipcode = gsub("\\b(\\d{4})\\b", "0\\1", zipcode)) %>%
  mutate(zipcode = gsub("\\b(\\d{3})\\b", "0\\10", zipcode)) %>%
  # Search the dates columns for which year that entry first has a 1.
  mutate(First_One_Year = pmap_chr(select(., -colnames(step_1)[1:10]), find_first_one)) %>%
  # Rename the newly added column entries so the "X" added is removed.
  rename_with(~ sub("^X", "", .), starts_with("X")) %>%
  # Now we sort the rows so that the oldest address comes before the more
  # recent addresses.
  group_by(abi) %>%
  arrange(First_One_Year, .by_group = TRUE) %>%
  ungroup() %>%
  # Remove the column used for organizing.
  select(-First_One_Year) %>%
  as.data.frame()

# Most ZIP codes are the expected five digits in length. However, a small number
# have an anomalous length of one digit, which cannot be reliably padded to a
# valid five-digit ZIP code. These entries are replaced with "00000".
str_length(step_1$zipcode) %>% table()
step_1[str_length(step_1$zipcode) %in% 1, "zipcode"] <- "00000"

# Some entries have NA values where a zero should be recorded. These are
# replaced with zero accordingly.
step_1 <- mutate(step_1, across(all_of(names(step_1)[14:34]), ~ coalesce(.x, 0)))

# # Commit results.
# write.csv(step_1, file = "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 01_Completed Result_04.29.2026.csv")

# Load in the pre-produced test results for evaluation.
step_1 <- read_csv("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 01_Completed Result_04.29.2026.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()


## ----------------------------------------------------------------
## PART E: Assess Overall Performance

# In this section, we verify that all ABIs were accounted for and that none
# are missing from the collapsed dataset. We also evaluate the overall
# performance of the collapsing procedure by examining how many records
# were consolidated in the process.

# Confirm date and abi combo is unique
finish_build[, c(1, 3)] %>% duplicated() %>% any()

# If any FALSE then check query_statuses all 200 (query went through)
table(finish_build$geolocation_verified, useNA = "ifany")




df <- qc_geo$qc2 %>%
  select(-i, -abi, -address, -n_geo) %>%
  as.data.frame()

df$row_id <- seq_len(nrow(df))

# mean-center each row's summaries (lat and lon separately)
box_df_centered <- bind_rows(
  df %>%
    mutate(mu = lat_mean) %>%
    transmute(
      row_id,
      coord  = "lat",
      ymin   = lat_min    - mu,
      lower  = lat_q1     - mu,
      middle = lat_median - mu,
      upper  = lat_q3     - mu,
      ymax   = lat_max    - mu
    ),
  df %>%
    mutate(mu = lon_mean) %>%
    transmute(
      row_id,
      coord  = "lon",
      ymin   = lon_min    - mu,
      lower  = lon_q1     - mu,
      middle = lon_median - mu,
      upper  = lon_q3     - mu,
      ymax   = lon_max    - mu
    )
)

ggplot(box_df_centered, aes(x = row_id, group = row_id)) +
  geom_boxplot(
    aes(ymin = ymin, lower = lower, middle = middle, upper = upper, ymax = ymax),
    stat = "identity",
    width = 0.6
  ) +
  geom_hline(yintercept = 0, linetype = "dashed", alpha = 0.5) +
  facet_wrap(~ coord, scales = "free_y", ncol = 1) +
  labs(
    x = "Row",
    y = "Value (mean-centered within row)",
    title = "Lat/Lon summaries (mean-centered)"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank())





# Confirm that all ABIs were accounted for:
#
#   a) All ABIs for which duplicates were detected are present in the
#      duplicates detection output.
all(unique(df_duplicates$abi) %in% duplicates_detected$abi) & all(duplicates_detected$abi %in% unique(df_duplicates$abi)) == TRUE

#   b) The duplicate and non-duplicate subsets are mutually exclusive,
#      i.e., no ABI appears in both.
any(unique(df_duplicates$abi) %in% unique(no_duplicates$abi)) | any(unique(no_duplicates$abi) %in% unique(df_duplicates$abi)) == FALSE

#   c) All ABIs from both the duplicate and non-duplicate subsets are
#      present in the final combined dataset.
all(unique(c(no_duplicates$abi, df_duplicates$abi)) %in% unique(church_wide$abi)) & all(unique(church_wide$abi) %in% unique(c(no_duplicates$abi, df_duplicates$abi))) == TRUE

#   d) All ABIs in the final dataset, after excluding those removed due to
#      a missing address, are present in the raw dataset, and vice versa.
church_wide %>%
  filter(abi %!in% no_address_abi) %>%
  (\(x) {all(unique(x$abi) %in% unique(step_1$abi)) & all(unique(step_1$abi) %in% unique(x$abi)) == TRUE} )()

#   e) Conversely to (d), confirm that the only ABIs absent from the final
#      dataset relative to the raw dataset are those removed due to a
#      missing address.
all(unique(church_wide$abi)[unique(church_wide$abi) %!in% unique(step_1$abi)] %in% no_address_abi) &
  all(no_address_abi %in% unique(church_wide$abi)[unique(church_wide$abi) %!in% unique(step_1$abi)]) == TRUE


# Among businesses with detected duplicates, the collapsing procedure reduced
# the number of records by approximately 60%.
church_wide %>%
  filter(abi %in% duplicates_detected$abi) %>%
  (\(x) {round((nrow(x) - nrow(df_duplicates))/nrow(x)*100, digits = 2)} )()

# Approximately 40% of unique businesses had no duplicate records.
round(nrow(no_duplicates)/length(unique(church_wide$abi))*100, digits = 2)

# Overall, the collapsing procedure reduced the total number of records in
# the dataset by approximately 53%.
round((nrow(church_wide) - nrow(step_1))/nrow(church_wide)*100, digits = 2)










