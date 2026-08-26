## ----------------------------------------------------------------
## 
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: August 17th, 2026
## Date Modified: August 20th, 2026
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
##       The Spring 2026 refactoring was not completed across all steps. Steps
##       1 and 2 reflect the full updates, with results generated for the
##       entire dataset. Steps 3-5 were updated for reporting clarity only and
##       continue to process the restricted 2023 Format produced for the
##       Summer 2025 symposium.
## 
##       Results from the refactored pipeline are stored in:
##         "~/KEEP LOCAL/From Clean Raw Data/Step *_2023 Format/"
##       Results from the original prototype run are archived in:
##         "~/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED"
##       GeoJSON files visualized on the dashboard are in:
##         "~/Dashboard Datasets/"
##       and reflect data as of June 2025.
## 
## US Census Bureau API Keys:
## tidycensus previously required a client API key registered and configured
## with the US Census Bureau. These credentials CANNOT be shared and must
## remain private to each user. They must be kept untracked by Git, stored
## locally, and never published to GitHub.
##
## If an API key is required when running tidycensus, follow the steps below
## to configure your credentials and environment.
## 
## 1. Register for a Census API key by visiting the link below, entering your
##    information, and following the provided steps. Store your key in a secure,
##    private location such as a password manager.
##
##    http://api.census.gov/data/key_signup.html
##
##    As noted above, API keys must NOT be hard-coded into the script, as they
##    must remain private to each user.
## 
## 2. In the project root directory, create a ".Renviron" file if one does not
##    already exist. Add your credentials as shown below, with no extra spaces
##    or hidden characters:
## 
##       CENSUS_API_KEY="your_key"
## 
##    These variables will be loaded in the script below using sys.getenv().
## 
## 3. Ensure that the ".Renviron" file is listed in your ".gitignore" file and
##    is not being tracked by Git.
## 
## NOTE: If environment variables fail to load, explicitly set the path using:
##
##    rprojroot::find_rstudio_root_file()
##    readRenviron(rprojroot::find_rstudio_root_file(".Renviron"))
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: CONSTRUCT THE DENOMINATOR REFERENCE TABLES
##        * SUBSECTION A1: Get Square Miles
##        * SUBSECTION A2: Get Population Counts
## 
##    - PART B: FINAL RESHAPE AND JOIN REFERENCE DENOMINATORS
##        * SUBSECTION B1: Exclude Metadata Not Conducive to the Analysis and Reshape
##        * SUBSECTION B2: Aggregate Block-Level GEOIDs to Dataset Boundary Level
##        * SUBSECTION B3: Join Reference Denominators
## 
##    - PART C:
##    - PART D:

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
  library("tidycensus")
  library("ggplot2")          # Graphics and visualization
  library("scales")           # Scale/label helpers for plots (percent/number/date formatting, breaks)
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("lubridate")
  library("future.apply")     # Parallel processing
  library("progress")         # Progress bars
  library("data.table")       # High-performance data manipulation
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
  library("units")
})

# Set up the plan for parallel processing
plan(multisession, workers = 4)

# Load in the functions
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Generate the Metrics_2026 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))

# Define the "if else" for null options operation
"%||%" <- function(a, b) if (!is.null(a)) a else b

# Cache TIGRIS shapefiles locally to avoid re-downloading each session
options(tigris_use_cache = TRUE)




## ----------------------------------------------------------------
## LOAD IN THE DATA

# PART A constructs the denominator lookup tables for square mileage and total
# population. PART B joins those lookup tables to a reshaped version of the
# Step 3 data, producing the final analysis-ready form.
#
# If the lookup tables have not yet been created, refer to
# "PART A: CONSTRUCT THE DENOMINATOR REFERENCE TABLES" before proceeding.
# 
# If the lookup tables exist but the analysis-ready data has not yet been
# created, refer to "PART B: FINAL RESHAPE AND JOIN REFERENCE DENOMINATORS"
# before proceeding.
# 
# Otherwise, load the analysis-ready dataset and skip to PART C to generate
# the metrics.


# Load the analysis ready data.
church_2026_form_analysis <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/church_2026_form_analysis_ready_08.20.2026.parquet")
setDT(church_2026_form_analysis)




## ----------------------------------------------------------------
## PART A: CONSTRUCT THE DENOMINATOR REFERENCE TABLES

# Two metrics require denominators that vary across census boundaries and
# decennial years. To improve the efficiency of downstream calculations,
# these denominators are pre-compiled into a lookup table. If the
# corresponding output files already exist, this section can be skipped.
# 
# Additionally, processing each census boundary level independently is 
# unnecessary, as population and square mileage can be summed for higher-level 
# aggregated GEOIDs. The only exception is ZCTAs, which must be calculated 
# independently since their polygon boundaries do not cleanly align with GEOIDs. 
# Under this approach, the smallest GEOID, block, will be annotated to maximize 
# future metric generation.


# To efficiently handle block-level GEOID data, state-level shapefiles will be 
# imported by census division (1–9), each grouping as many as 9 states. These 
# will then be compiled into a single lookup table containing both metrics.

# Associate FIPS codes with their corresponding state abbreviations.
fips_lu <- data.table(
  abbr = names(f <- c(AL="01",AK="02",AZ="04",AR="05",CA="06",CO="08",CT="09",DE="10",DC="11",FL="12",
                      GA="13",HI="15",ID="16",IL="17",IN="18",IA="19",KS="20",KY="21",LA="22",ME="23",
                      MD="24",MA="25",MI="26",MN="27",MS="28",MO="29",MT="30",NE="31",NV="32",NH="33",
                      NJ="34",NM="35",NY="36",NC="37",ND="38",OH="39",OK="40",OR="41",PA="42",RI="44",
                      SC="45",SD="46",TN="47",TX="48",UT="49",VT="50",VA="51",WA="53",WV="54",WI="55",WY="56")),
  fips = unname(f)
)

# Compile the state-to-division lookup table.
state_fips_division <- data.table(
  state = c(state.name, "District of Columbia"),
  abbr  = c(state.abb,  "DC"),
  division = c(as.character(state.division), "South Atlantic")
)[fips_lu, on = "abbr"][order(fips), .(state, abbr, fips, division)]


## --------------------
## SUBSECTION A1: Get Square Miles

# Square mileage can be calculated from polygon boundaries in TIGER/Line
# shapefiles using set_units(st_area(x_ea), "acre"), then converted to square
# miles. Because census GEOIDs are non-overlapping, this calculation is only
# required for the smallest available boundary (block or block group). These
# are then summed to aggregate into larger GEOID census boundaries.
# 
# ZCTAs are processed separately by the same method and are not aggregated into
# other census boundaries, as their polygon boundaries do not cleanly cross-walk
# with standard GEOIDs.


# Import the smallest available census boundary TIGER/Line shapefiles, at
# minimum matching, if not finer than, the resolution encoded in the cleaned
# data from Step 2. Also define the US census divisions to iterate over; the
# algorithm imports state boundaries one division at a time, processes them,
# then removes them from memory before proceeding to the next division.

block_geography <- "blocks"   # Optionally geography level = c("blocks", "block groups")
divisions <- unique(state_fips_division$division)
sigfigs   <- 4

# Load the functions required to compile the lookup tables. If the lookup
# tables have already been produced, this step can be skipped.
source("./Code/Support Functions/For Step 2_2026 Format.R")


# -- Process GEOIDs ----------------------------------

pieces_by_year_all <- list()  # Initialize the empty list
for (div in divisions) {
  
  # Pull the states from the divisions-to-states lookup table.
  states_present <- state_fips_division %>%
    filter(division == div) %>%
    pull(abbr) %>% unique()
  
  # Import the relevant states' GeoPackages for this division only (memory-safe).
  blocks_by_state <- read_state_gpkgs_for_data(
    states_present, "./Data/Results/Census Bureau TIGER Line Shapefiles/",
    geography = block_geography
  )
  
  # Estimate the number of tasks in this division to size an accurate progress bar.
  n_tasks_div <- count_sf_in_blocks_by_state(blocks_by_state)
  
  # Initialize progress bar (per-division).
  pb_div <- progress_bar$new(
    format = paste0(div, " [:bar] :current/:total (:percent) | :message"),
    total  = n_tasks_div,
    clear  = FALSE,
    width  = 80
  )
  
  # Process each state's list entries and each decennial-year dataset within 
  # that state.
  for (st in names(blocks_by_state)) {
    for (nm in names(blocks_by_state[[st]])) {
      
      # Pull the current sf object; skip non-sf entries.
      x <- blocks_by_state[[st]][[nm]]
      if (!inherits(x, "sf")) next
      
      # Update progress bar with the current state/year item being processed.
      pb_div$tick(tokens = list(message = paste(st, nm, sep = " / ")))
      
      # Detect the decennial year from the object name.
      yr <- get_year_from_name(nm)
      if (is.na(yr)) stop("Couldn't detect year from name: ", div, " / ", st, " / ", nm)
      
      # Compile results by year for later bind_rows (within-year) + full_join (across years).
      pieces_by_year_all[[as.character(yr)]] <- append(
        pieces_by_year_all[[as.character(yr)]],
        list(area_table_one(x, yr, id_col = "geoid"))
      )
    }
  }
  
  # Remove the imported shapefiles to free memory before moving to the next division.
  rm(blocks_by_state); gc()
}


year_tables_all <- imap(pieces_by_year_all, \(lst, yr_chr) {
  # Within each year, stack all state-level result tables into one year table.
  # distinct() is a safety check in case any GEOIDs appear more than once.
  bind_rows(lst) %>%
    distinct(geoid, .keep_all = TRUE)
})

area_master_all <- reduce(year_tables_all, full_join, by = "geoid") %>%
  # Join year tables side-by-side on GEOID to create the final wide master file,
  # then sort for readability.
  arrange(geoid)


#' @description
#' Codebook for square mileage estimated from TIGER/Line Shapefile polygon
#' boundaries, converted to acres using \code{set_units(st_area(x_ea), "acre")}
#' and then to square miles.
#'
#' @field geoid Block-level GEOID covering the entire US and District of Columbia.
#'
#' @field area[2000|2010|2020]_mi2 Calculated square mileage for each decennial
#'                                 period's GEOID mapping.

# # Save result.
# write_parquet(area_master_all, "./Data/Results/KEEP LOCAL/From Generate the Metrics/GEOID Square Miles_Block Level_08.18.2026.parquet")


# -- Process ZCTA ------------------------------------

# Load the combined national-level Metropolitan/Micropolitan Statistical Area
# (CBSA), Combined Statistical Area (CSA), and ZIP Code Tabulation Areas (ZCTA)
# GeoPackage file.

core_areas_layers <- sf::st_layers("./Data/Results/KEEP LOCAL/From Generate the Metrics/core_areas.gpkg")$name
core_areas <- setNames(
  lapply(core_areas_layers, function(lyr) st_read("./Data/Results/KEEP LOCAL/From Generate the Metrics/core_areas.gpkg", layer = lyr, quiet = TRUE)),
  core_areas_layers
)

# Identify the ZCTA names.
zcta_names <- names(core_areas)[grepl("^zcta_(2000|2010|2020)$", names(core_areas))]

# Initialize progress bar (per-division).
pb <- progress_bar$new(
  format = "ZCTAs [:bar] :current/:total (:percent) | :message",
  total  = length(zcta_names),
  clear  = FALSE,
  width  = 80
)

pieces_by_year <- list()  # Initialize the empty list
for (nm in zcta_names) {
  
  # Pull the ZCTA sf object for this vintage (e.g., core_areas$zcta_2010)
  x <- core_areas[[nm]]
  if (!inherits(x, "sf")) next  # Skip any non-sf entries
  
  # Update progress bar with the current vintage being processed
  pb$tick(tokens = list(message = nm))
  
  # Read the vintage year from the data itself (should be a single value)
  yr <- unique(x$vintage_year)
  if (length(yr) != 1 || is.na(yr)) stop("Unexpected vintage_year in: ", nm)
  
  # Compile results by year for later bind_rows (within-year) + full_join (across years).
  pieces_by_year[[as.character(yr)]] <- append(
    pieces_by_year[[as.character(yr)]],
    list(area_table_one(x, yr, id_col = "area_code"))
  )
}

year_tables <- imap(pieces_by_year, \(lst, yr_chr) {
  # Within each year, stack all result tables into one year table.
  # distinct() is a safety check in case any GEOIDs appear more than once.
  bind_rows(lst) %>%
    distinct(area_code, .keep_all = TRUE)
})

area_master <- reduce(year_tables, full_join, by = "area_code") %>%
  # Join year tables side-by-side on area_code to create the final wide master 
  # file, then sort for readability.
  arrange(area_code) %>%
  rename(zcta = area_code)


#' @description
#' Codebook for square mileage estimated from TIGER/Line Shapefile polygon
#' boundaries, converted to acres using \code{set_units(st_area(x_ea), "acre")}
#' and then to square miles.
#'
#' @field zcta ZIP Code Tabulation Areas (ZCTA) covering the entire US and 
#'             District of Columbia.
#'
#' @field area[2000|2010|2020]_mi2 Calculated square mileage for each decennial
#'                                 period's ZCTA mapping.

# # Save result.
# write_parquet(area_master, "./Data/Results/KEEP LOCAL/From Generate the Metrics/ZCTA Square Miles_08.18.2026.parquet")


## --------------------
## SUBSECTION A2: Get Population Counts

# Total population counts are retrieved via the tidycensus API, which queries
# the US Census Bureau using a specified table and variable. As with square
# mileage, data are fetched at the smallest available geographic unit (block
# or block group) and then aggregated by summing up to larger GEOID census
# boundaries.
#
# ZCTAs follow the same retrieval method but are handled separately and are
# not rolled up into other census boundaries, as their polygon extents do not
# align cleanly with standard GEOID hierarchies.
# Census Bureau API query note (copied):
# 
# Note: 2020 decennial Census data use differential privacy, a technique that
# introduces errors into data to preserve respondent confidentiality.
# ℹ Small counts should be interpreted with caution.
# ℹ See https://www.census.gov/library/fact-sheets/2021/protecting-the-confidentiality-of-the-2020-census-redistricting-data.html for additional guidance.


# Define the variable IDs and reference tables used by tidycensus.
spec <- tibble::tribble(
  ~year, ~sumfile, ~var_totpop,
  2000,  "sf1",    "P001001",  # 2000 SF1 total population
  2010,  "sf1",    "P001001",  # 2010 SF1 total population
  2020,  "dhc",    "P1_001N"   # 2020 DHC total population
)


states <- c(state.abb, "DC")   # Choose states to pull (all 50 + DC)
pop_block_master <- get_block_pop_all_states(states, spec)   # Retrieve block-level GEOID populations

#' @description
#' Codebook for total population retrieved via the tidycensus API, which queries
#' the US Census Bureau using a specified table and variable. The 2000 and 2010
#' decennial census data were drawn from Summary File 1 ("sf1"), Table P001
#' ("P001001"). The 2020 decennial census data were drawn from the Demographic
#' and Housing Characteristics File ("dhc"), Table P1.
#'
#' @field geoid Block-level GEOID covering the entire US and District of Columbia.
#'
#' @field pop[2000|2010|2020] Total population count for each decennial census
#'                            year at the block level.

# # Save result.
# write_parquet(pop_block_master, "./Data/Results/KEEP LOCAL/From Generate the Metrics/GEOID Population_Block Level_08.20.2026.parquet")


zcta_pop <- get_zcta_pop_all_decennials(spec)   # Retrieve ZCTA populations

#' @description
#' Codebook for total population retrieved via the tidycensus API, which queries
#' the US Census Bureau using a specified table and variable. The 2000 and 2010
#' decennial census data were drawn from Summary File 1 ("sf1"), Table P001
#' ("P001001"). The 2020 decennial census data were drawn from the Demographic
#' and Housing Characteristics File ("dhc"), Table P1.
#'
#' @field zcta ZIP Code Tabulation Areas (ZCTA) covering the entire US and 
#'             District of Columbia.
#'
#' @field pop[2000|2010|2020] Total population count for each decennial period's 
#'                            ZCTA mapping.

# # Save result.
# write_parquet(zcta_pop, "./Data/Results/KEEP LOCAL/From Generate the Metrics/ZCTA Population_08.20.2026.parquet")




## ----------------------------------------------------------------
## PART B: FINAL RESHAPE AND JOIN REFERENCE DENOMINATORS

# The Step 3 results need to be reshaped one final time to allow the reference 
# denominators used to generate the per-square-mile and per-10,000-persons 
# metrics to be joined by decennial year. If the output file for this step 
# already exists, this section can be skipped.


## --------------------
## SUBSECTION B1: Exclude Metadata Not Conducive to the Analysis and Reshape

# In the final data cleaning and validation step (Step 3), the cleaned and
# validated data were reshaped and annotated with metrics that support the
# analysis presented here. In this process, the original metadata and select
# quality control outputs from Steps 2 and 3 were retained. As these are not
# needed for generating the metrics in subsequent steps, they are excluded here.

# Load the cleaned and validated wide-format data.
church_2026_form_wide <- read_parquet("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 3_2026 Format/church_2026_form_wide_annotated_08.16.2026.parquet")
setDT(church_2026_form_wide)

church_2026_form_analysis <- church_2026_form_wide %>%
  # Drop SIC-related fields (raw codes and decoded descriptions). These were
  # needed for classification earlier but are not used in the downstream
  # analysis/visualization.
  select(-matches("^(primary_sic(_desc)?|overflow_sic(_desc)?_[0-9]+)$")) %>%
  # Drop metropolitan-area identifiers (CBSA/CSA codes and levels). These
  # boundary-based attributes are not part of the geography used in this
  # visualization.
  select(-matches("^(cbsa_(code|level)_4*[0-9]+|csa_code_4*[0-9]+)$")) %>%
  # Drop ETL / QC bookkeeping fields created during cleaning, matching, and
  # imputation. Keep only variables required for the analytic dataset.
  select(-c(
    address_verified, address_matched,
    gap_filled, n_gaps_filled, avg_gap_len_filled,
    geolocation_verified, geoid_match,
    to_address
  ))

# Reshape the census boundaries so that each row represents a single decennial period.
church_2026_form_analysis <- melt(
  church_2026_form_analysis,
  measure = patterns("^geoid_(2000|2010|2020)$", "^zcta_(2000|2010|2020)$"),
  variable.name = "year",
  value.name   = c("geoid", "zcta")
)

# Factor and reorder the columns and rows so that each row represents one ABI/address.
church_2026_form_analysis[, year := c(2000L, 2010L, 2020L)[year]]
setcolorder(
  church_2026_form_analysis,
  c("abi", "address", "year", "geoid", "zcta",
    setdiff(names(setcolorder), c("abi", "address", "year", "geoid", "zcta")))
)
setorder(church_2026_form_analysis, abi, address, year) 

# Clear up RAM by removing the complete dataset
rm(church_2026_form_wide)


## --------------------
## SUBSECTION B2: Aggregate Block-Level GEOIDs to Dataset Boundary Level

# Square mileage lookup tables for GEOIDs and ZCTAs were produced in "SUBSECTION 
# A1: Get Square Miles". Load the pre-computed results.
geoid_sqMiles <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/GEOID Square Miles_Block Level_08.18.2026.parquet")
setDT(geoid_sqMiles)

zcta_sqMiles <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/ZCTA Square Miles_08.18.2026.parquet")
setDT(zcta_sqMiles)

# Total population lookup tables for GEOIDs and ZCTAs were produced in 
# "SUBSECTION A2: Get Population Counts". Load the pre-computed results.
geoid_pop <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/GEOID Population_Block Level_08.20.2026.parquet")
setDT(geoid_pop)

zcta_pop <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/ZCTA Population_08.20.2026.parquet")
setDT(zcta_pop)


# GEOID lookup tables were intentionally compiled at the smallest available
# census boundary level: blocks. These must be pre-aggregated to the appropriate 
# level present in the dataset. Based on the preceding steps, it is assumed that 
# only one boundary level will be represented. While blocks or block groups are 
# the expected units, this step agnostically inspects the GEOID length and 
# aggregates accordingly.


# Detect observed (non-NA) GEOID lengths in church data
geoid_lens <- church_2026_form_analysis[
  !is.na(geoid),
  unique(str_length(geoid))
]
geoid_lens <- sort(geoid_lens)

if (length(geoid_lens) == 0L) {
  warning("church_2026_form_analysis$geoid is all NA; leaving geoid_pop/geoid_sqMiles unaggregated.")
  # do nothing
  
} else if (length(geoid_lens) > 1L) {
  warning(
    "Multiple non-NA GEOID lengths detected in church_2026_form_analysis$geoid: ",
    paste(geoid_lens, collapse = ", "),
    ". Using the minimum length (", min(geoid_lens), ") for aggregation."
  )
  target_len <- min(geoid_lens)
  
} else {
  target_len <- geoid_lens[1L]
}

# Only aggregate if we have a target_len
if (exists("target_len")) {
  
  # Optional: sanity check against common Census GEOID lengths
  common_lens <- c(2L, 5L, 11L, 12L, 15L)  # state, county, tract, block group, block
  if (!target_len %in% common_lens) {
    warning(
      "Observed church GEOID length is ", target_len,
      ", which is not one of the common Census lengths {2,5,11,12,15}. Proceeding anyway."
    )
  }
  
  # If target_len is block length (15), no need to aggregate
  if (target_len != 15L) {
    
    # Aggregate denominators to target_len
    geoid_pop[, geoid := substr(geoid, 1, target_len)]
    geoid_pop <- geoid_pop[
      , .(
        pop2000 = sum(pop2000, na.rm = TRUE),
        pop2010 = sum(pop2010, na.rm = TRUE),
        pop2020 = sum(pop2020, na.rm = TRUE)
      ),
      by = geoid
    ]
    
    geoid_sqMiles[, geoid := substr(geoid, 1, target_len)]
    geoid_sqMiles <- geoid_sqMiles[
      , lapply(.SD, function(x) round(sum(x, na.rm = TRUE), 4)),
      by = geoid,
      .SDcols = setdiff(names(geoid_sqMiles), "geoid")
    ]
  }
}


## --------------------
## SUBSECTION B3: Join Reference Denominators

# -- Pivot Denominators to Long ----------------------

# GEOID pop: pop2000/pop2010/pop2020 -> year + geoid_pop
geoid_pop_long <- melt(
  geoid_pop,
  id.vars = "geoid",
  measure.vars = patterns("^pop(2000|2010|2020)$"),
  variable.name = "year",
  value.name = "geoid_pop"
)
geoid_pop_long[, year := as.integer(gsub("^pop", "", year))]

# GEOID area: area2000_mi2/area2010_mi2/area2020_mi2 -> year + geoid_sqMiles
geoid_sqMiles_long <- melt(
  geoid_sqMiles,
  id.vars = "geoid",
  measure.vars = patterns("^area(2000|2010|2020)_mi2$"),
  variable.name = "year",
  value.name = "geoid_sqMiles"
)
geoid_sqMiles_long[, year := as.integer(gsub("^area(\\d{4})_mi2$", "\\1", year))]

# Compiled GEOID lookup table
geoid_denominators <- geoid_pop_long[geoid_sqMiles_long, on = .(geoid, year)]


# ZCTA pop -> year + zcta_pop
zcta_pop_long <- melt(
  zcta_pop,
  id.vars = "zcta",
  measure.vars = patterns("^pop(2000|2010|2020)$"),
  variable.name = "year",
  value.name = "zcta_pop"
)
zcta_pop_long[, year := as.integer(gsub("^pop", "", year))]

# ZCTA area: area2000_mi2/area2010_mi2/area2020_mi2 -> year + zcta_sqMiles
zcta_sq_long <- melt(
  zcta_sqMiles,
  id.vars = "zcta",
  measure.vars = patterns("^area(2000|2010|2020)_mi2$"),
  variable.name = "year",
  value.name = "zcta_sqMiles"
)
zcta_sq_long[, year := as.integer(gsub("^area(\\d{4})_mi2$", "\\1", year))]

# Compiled ZCTA lookup table
zcta_denominators <- zcta_pop_long[zcta_sq_long, on = .(zcta, year)]


# -- Join Onto Church Table --------------------------

# Row id to restore original order
church_2026_form_analysis[, rid__ := .I]

# Ensure church has integer year
church_2026_form_analysis[, year := as.integer(year)]

# ---- GEOID denominators ----
# Expects: geoid_denominators has columns geoid, year, geoid_pop, geoid_sqMiles
church_2026_form_analysis[
  geoid_denominators, on = .(geoid, year),
  `:=`(
    geoid_pop    = i.geoid_pop,
    geoid_sqMiles = i.geoid_sqMiles
  )
]

# ---- ZCTA denominators ----
# Expects: zcta_denominators has columns zcta, year, zcta_pop, zcta_sqMiles
church_2026_form_analysis[
  zcta_denominators, on = .(zcta, year),
  `:=`(
    zcta_pop    = i.zcta_pop,
    zcta_sqMiles = i.zcta_sqMiles
  )
]

# Restore original order and drop helper id
setorder(church_2026_form_analysis, rid__)
church_2026_form_analysis[, rid__ := NULL]

# A small handful of entries had a move detected but the distance was
# uncalculatable, likely due to missing geocoordinates. These can be recoded
# as NA for easier downstream calculations.

church_2026_form_analysis %>% filter(max_dist_km %in% -Inf)
church_2026_form_analysis[max_dist_km == -Inf, `:=`(
  max_dist_km  = NA_real_,
  mean_dist_km = NA_real_
)]


#' @description
#' Codebook for new output fields representing the two lookup table values,
#' square miles and total population, by GEOID and ZCTA, as generated in
#' PART A. All other fields were carried forward from the Step 3 data.
#'
#' @field `[geoid|zcta]_pop` Total population count for each decennial census
#'                           year, mapped to the corresponding GEOID or ZCTA.
#'                           GEOID-level values were pre-aggregated to match
#'                           the census boundary level present in the Step 3
#'                           table.
#'
#' @field `[geoid|zcta]_sqMiles` Total square mileage for each decennial census
#'                               year, mapped to the corresponding GEOID or
#'                               ZCTA. GEOID-level values were pre-aggregated
#'                               to match the census boundary level present in
#'                               the Step 3 table.

# # Save result.
# write_parquet(church_2026_form_analysis, "./Data/Results/KEEP LOCAL/From Generate the Metrics/church_2026_form_analysis_ready_08.20.2026.parquet")




## ----------------------------------------------------------------
## PART C: 

## --------------------
## SUBSECTION C1: Exclude Entries with Critical Missing Information

# Entries with critical missing information are excluded from metric generation:
#   - Missing geocoordinates (no GEOID or ZCTA assigned)
#   - Missing address_line_1 (address-based geocoding not possible)
#   - PO Box listed instead of a physical address (physical location cannot be 
#     confirmed, as it may differ significantly from the PO Box)

# Pull address_line_1, everything before first comma, to check for missing values.
church_2026_form_analysis[, addr_line1 := fifelse(
  is.na(address),
  NA_character_,
  trimws(sub(",.*$", "", address))
)]

# Assign a missing-information label to each entry. Final results will include
# the count of removed entries broken down by date range and area.
church_2026_form_analysis[, `:=`(
  addr1_is_na = is.na(addr_line1),
  addr1_is_pobox = !is.na(addr_line1) &
    grepl("^\\s*(p\\.?\\s*o\\.?\\s*(box)?|post\\s*office\\s*box)\\b",
          addr_line1, ignore.case = TRUE),
  geoid_is_na = is.na(geoid),
  zcta_is_na  = is.na(zcta)
)][, addr_line1 := NULL]


## --------------------
## SUBSECTION C2: 

date_range <- build_year_windows(church_2026_form_analysis, min_span = 5L) #%>%
  #filter(label %in% c("2010_2015", "2012_2018", "2010_2019"))

relig_cols <- c(
  "buddhist_temple", "christian_church", "hindu_mandir", "jewish_synagogue",
  "muslim_mosque", "sikh_gurdwara", "other_religion", "interfaith", "unspecified"
)

pb <- utils::txtProgressBar(min = 0, max = nrow(date_range), style = 3)
on.exit(close(pb), add = TRUE)

rollups_accum <- NULL

for (i in seq_len(nrow(date_range))) {
  
  # ---------------------------------------------------------------------------
  # (0) Window setup
  # ---------------------------------------------------------------------------
  
  NO_FILTER_SENTINEL <- "__NO_FILTER__"
  NO_FILTER_RELIGION_VALUE <- "all_religions"
  
  start_y <- date_range$start[i]
  end_y   <- date_range$end[i]
  yrs     <- start_y:end_y
  lab     <- date_range$label[i]
  
  message("Window: ", lab, " (", start_y, "–", end_y, ")")
  utils::setTxtProgressBar(pb, i)
  
  church_by_years <- filter_ts(church_2026_form_analysis, yrs)
  data.table::setDT(church_by_years)
  
  discrete_list <- vector("list", length(relig_cols) + 1L)
  k <- 0L
  
  # ---------------------------------------------------------------------------
  # (1) Inner loop: build ABI-year discrete tables for each religion mode + unfiltered
  # ---------------------------------------------------------------------------
  
  for (mode in c(relig_cols, NO_FILTER_SENTINEL)) {
    
    # (1a) filtered vs unfiltered universe
    if (identical(mode, NO_FILTER_SENTINEL)) {
      church_relig <- church_by_years
      relig_value  <- NO_FILTER_RELIGION_VALUE
    } else {
      relig_value <- mode
      abi_keep <- abi_any_true(church_by_years, cols = mode)
      if (length(abi_keep) == 0L) next
      church_relig <- church_by_years[abi %in% abi_keep]
    }
    if (nrow(church_relig) == 0L) next
    
    # (1b) flags by (geoid, zcta)
    flag_counts <- church_relig[
      ,
      .(
        n_open      = uniqueN(abi),
        addr1_na    = uniqueN(abi[addr1_is_na    %in% TRUE]),
        addr1_pobox = uniqueN(abi[addr1_is_pobox %in% TRUE]),
        geoid_na    = uniqueN(abi[geoid_is_na    %in% TRUE]),
        zcta_na     = uniqueN(abi[zcta_is_na     %in% TRUE])
      ),
      by = .(geoid, zcta)
    ]
    
    # (1c) drop bad ABIs
    bad_abi <- unique(church_relig[
      addr1_is_na %in% TRUE |
        addr1_is_pobox %in% TRUE |
        geoid_is_na %in% TRUE |
        zcta_is_na %in% TRUE,
      abi
    ])
    
    church_clean <- church_relig[!abi %in% bad_abi]
    if (nrow(church_clean) == 0L) next
    
    yrs_present <- sort(unique(church_clean$year))
    if (!length(yrs_present)) next
    
    message("  Group: ", relig_value, " | decennial periods: ", paste(yrs_present, collapse = ", "))
    
    # (1d) closures by year (compute closures_all + closures_no_moves)
    closures_by_year <- lapply(yrs_present, function(yr) {
      dt_y <- church_clean[year == yr]
      if (nrow(dt_y) == 0L) return(NULL)
      
      closures_all <- suppressWarnings(suppressMessages({
        tmp <- NULL
        capture.output(
          tmp <- calculate_closure(DT = dt_y, min_zero_run = 4L, multi_addr_mode = "compress"),
          file = NULL
        )
        tmp
      }))
      
      closures_no_moves <- suppressWarnings(suppressMessages({
        tmp <- NULL
        capture.output(
          tmp <- calculate_closure(DT = dt_y, min_zero_run = 4L, multi_addr_mode = "skip"),
          file = NULL
        )
        tmp
      }))
      
      closures <- closures_no_moves[closures_all, on = "abi"]
      closures[, year := yr]
      closures
    })
    
    closures_dt <- data.table::rbindlist(closures_by_year, fill = TRUE, use.names = TRUE)
    if (nrow(closures_dt) == 0L) next
    
    # (1e) add window metadata + religion label
    flag_counts[, `:=`(start_y = start_y, end_y = end_y, label = lab, religion = relig_value)]
    closures_dt[, `:=`(start_y = start_y, end_y = end_y, label = lab, religion = relig_value)]
    
    # Join table providing ABI-year geographies + denominators
    church_join <- unique(church_clean[, c(
      "abi", "year", "geoid", "zcta",
      "geoid_pop", "geoid_sqMiles",
      "zcta_pop",  "zcta_sqMiles"
    )])
    
    flag_counts_aug <- flag_counts[church_join, on = .(geoid, zcta)]
    closures_aug    <- closures_dt[church_join,  on = .(abi, year)]
    
    flag_counts_aug[, religion := relig_value]
    closures_aug[,    religion := relig_value]
    
    # (1f) suffix metric columns by window label
    id_keep_flags <- c(
      "geoid","zcta","start_y","end_y","label","religion",
      "abi","year","geoid_pop","geoid_sqMiles","zcta_pop","zcta_sqMiles"
    )
    metric_cols_flags <- setdiff(names(flag_counts_aug), id_keep_flags)
    if (length(metric_cols_flags)) {
      data.table::setnames(flag_counts_aug, metric_cols_flags, paste0(metric_cols_flags, "__", lab))
    }
    
    id_keep_clos <- c(
      "abi","year","start_y","end_y","label","religion",
      "geoid","zcta","geoid_pop","geoid_sqMiles","zcta_pop","zcta_sqMiles"
    )
    metric_cols_clos <- setdiff(names(closures_aug), id_keep_clos)
    if (length(metric_cols_clos)) {
      data.table::setnames(closures_aug, metric_cols_clos, paste0(metric_cols_clos, "__", lab))
    }
    
    # (1g) n_move__<lab> = count ABIs with NA closures_no_moves__<lab>
    cnm    <- paste0("closures_no_moves__", lab)
    n_move <- paste0("n_move__", lab)
    
    if (cnm %chin% names(closures_aug)) {
      n_move_by_geo <- closures_aug[, .(tmp_nm = sum(is.na(get(cnm)))), by = .(geoid, zcta)]
      data.table::setnames(n_move_by_geo, "tmp_nm", n_move)
      flag_counts_aug <- n_move_by_geo[flag_counts_aug, on = .(geoid, zcta)]
    }
    
    # (1h) Join closures + flags -> ABI-year discrete_results
    discrete_results <- closures_aug[
      flag_counts_aug,
      on = .(
        abi, year, geoid, zcta,
        geoid_pop, geoid_sqMiles, zcta_pop, zcta_sqMiles,
        start_y, end_y, label, religion
      )
    ]
    
    # drop window metadata if you don't want them in ABI-year output
    drop_cols <- intersect(c("start_y", "end_y", "label"), names(discrete_results))
    if (length(drop_cols)) discrete_results[, (drop_cols) := NULL]
    
    # ABI-year ordering (keep only the ABI-year rules; NO rate ordering here)
    n_open <- paste0("n_open__", lab)
    
    if (all(c(n_open, cnm) %chin% names(discrete_results))) {
      cur <- names(discrete_results)
      cur <- cur[cur != n_open]
      pos <- match(cnm, cur)
      if (!is.na(pos)) {
        cur <- append(cur, n_open, after = pos - 1L)
        data.table::setcolorder(discrete_results, cur)
      }
    }
    
    reopen_nm <- paste0("reopenings_no_moves__", lab)
    if (all(c(reopen_nm, n_move) %chin% names(discrete_results))) {
      cur <- names(discrete_results)
      cur <- cur[cur != n_move]
      pos <- match(reopen_nm, cur)
      if (!is.na(pos)) {
        cur <- append(cur, n_move, after = pos)
        data.table::setcolorder(discrete_results, cur)
      }
    }
    
    k <- k + 1L
    discrete_list[[k]] <- discrete_results
  }
  
  # ---------------------------------------------------------------------------
  # (2) Bind all religion-mode discrete tables into one “window-wide” table
  # ---------------------------------------------------------------------------
  discrete_list <- discrete_list[seq_len(k)]
  if (!length(discrete_list)) next
  
  discrete_window <- data.table::rbindlist(discrete_list, fill = TRUE, use.names = TRUE)
  
  # ---------------------------------------------------------------------------
  # (3) Roll up (creates *_any/_avg/_max + rates)
  # ---------------------------------------------------------------------------
  message("    Rollup: ", lab, " | all relig groups")
  rollups <- rollup_results(discrete_window)
  
  # ---------------------------------------------------------------------------
  # (4) REORDER ROLLUP COLUMNS (this is the part you asked to implement)
  #     Requires reorder_rollup_cols(dt, lab, id=...) to exist.
  # ---------------------------------------------------------------------------
  
  if (!is.null(rollups$by_zcta) && nrow(rollups$by_zcta)) {
    rollups$by_zcta <- reorder_rollup_cols(rollups$by_zcta, lab, id = "zcta")
  }
  
  if (!is.null(rollups$by_geoid) && length(rollups$by_geoid)) {
    rollups$by_geoid <- lapply(rollups$by_geoid, function(dt) {
      reorder_rollup_cols(dt, lab, id = "geoid")
    })
  }
  
  # ---------------------------------------------------------------------------
  # (5) Accumulate rollups across windows (merge by id + religion)
  # ---------------------------------------------------------------------------
  
  if (is.null(rollups_accum)) {
    rollups_accum <- rollups
    
  } else {
    
    if (is.null(rollups_accum$by_geoid)) rollups_accum$by_geoid <- list()
    if (is.null(rollups$by_geoid))       rollups$by_geoid       <- list()
    
    for (lvl in union(names(rollups_accum$by_geoid), names(rollups$by_geoid))) {
      
      a <- rollups_accum$by_geoid[[lvl]]
      b <- rollups$by_geoid[[lvl]]
      
      if (is.null(a) || !nrow(a)) {
        rollups_accum$by_geoid[[lvl]] <- b
        next
      }
      if (is.null(b) || !nrow(b)) next
      
      key_cols <- intersect(c("geoid", "year", "religion"), union(names(a), names(b)))
      
      denom_cols <- intersect(c("geoid_pop", "geoid_sqMiles"), names(a))
      denom_drop <- intersect(denom_cols, names(b))
      if (length(denom_drop)) data.table::set(b, j = denom_drop, value = NULL)
      
      overlap <- intersect(setdiff(names(a), key_cols), setdiff(names(b), key_cols))
      if (length(overlap)) data.table::set(b, j = overlap, value = NULL)
      
      data.table::setkeyv(a, key_cols)
      data.table::setkeyv(b, key_cols)
      
      out <- data.table::copy(a)
      new_cols <- setdiff(names(b), key_cols)
      
      if (length(new_cols)) {
        out[b, (new_cols) := mget(paste0("i.", new_cols))]
      }
      
      b_only <- b[!out, on = key_cols]
      rollups_accum$by_geoid[[lvl]] <- data.table::rbindlist(list(out, b_only),
                                                             use.names = TRUE, fill = TRUE)
    }
    
    # zcta
    {
      a <- rollups_accum$by_zcta
      b <- rollups$by_zcta
      
      if (is.null(a) || !nrow(a)) {
        rollups_accum$by_zcta <- b
        
      } else if (!is.null(b) && nrow(b)) {
        
        key_cols <- intersect(c("zcta", "year", "religion"), union(names(a), names(b)))
        
        denom_cols <- intersect(c("zcta_pop", "zcta_sqMiles"), names(a))
        denom_drop <- intersect(denom_cols, names(b))
        if (length(denom_drop)) data.table::set(b, j = denom_drop, value = NULL)
        
        overlap <- intersect(setdiff(names(a), key_cols), setdiff(names(b), key_cols))
        if (length(overlap)) data.table::set(b, j = overlap, value = NULL)
        
        data.table::setkeyv(a, key_cols)
        data.table::setkeyv(b, key_cols)
        
        out <- data.table::copy(a)
        new_cols <- setdiff(names(b), key_cols)
        
        if (length(new_cols)) {
          out[b, (new_cols) := mget(paste0("i.", new_cols))]
        }
        
        b_only <- b[!out, on = key_cols]
        rollups_accum$by_zcta <- data.table::rbindlist(list(out, b_only),
                                                       use.names = TRUE, fill = TRUE)
      }
    }
  }
  
  # ---------------------------------------------------------------------------
  # (6) REORDER ACCUMULATED TABLES TOO (merge can reshuffle)
  # ---------------------------------------------------------------------------
  
  if (!is.null(rollups_accum$by_zcta) && nrow(rollups_accum$by_zcta)) {
    rollups_accum$by_zcta <- reorder_rollup_cols(rollups_accum$by_zcta, lab, id = "zcta")
  }
  
  if (!is.null(rollups_accum$by_geoid) && length(rollups_accum$by_geoid)) {
    rollups_accum$by_geoid <- lapply(rollups_accum$by_geoid, function(dt) {
      reorder_rollup_cols(dt, lab, id = "geoid")
    })
  }
}


#' @description
#' Codebook 
#'
#' @field `[geoid|zcta]` The Census boundary unit to which all metrics in this
#'                       row have been aggregated: block group, tract, county,
#'                       or state GEOID, or ZIP Code Tabulation Area (ZCTA).
#' 
#' @field religion The religious classification represented by this row's
#'                 metrics: e.g. \code{"Christian Churches"},
#'                 \code{"Other Religion"}, \code{"All Religions"}.
#' 
#' @field year Decennial census reference year: \code{2000}, \code{2010},
#'             or \code{2020}.
#' 
#' @field `[geoid|zcta]_pop` Total resident population for the corresponding
#'                           decennial census year, assigned to each GEOID or
#'                           ZCTA. GEOID-level counts were aggregated to match
#'                           the census boundary granularity.
#'
#' @field `[geoid|zcta]_sqMiles` Total land area in square miles for the
#'                               corresponding decennial census year, assigned
#'                               to each GEOID or ZCTA. GEOID-level values
#'                               were aggregated to match the census boundary 
#'                               granularity.
#' 
#' @field n_open Total number of ABIs located within the geographic unit and
#'               time window. Includes ABIs subsequently excluded by a
#'               missingness flag.
#' 
#' @field closures_[no_moves|all]_any Count of ABIs with at least one
#'                                    qualifying closure event, defined as
#'                                    four or more consecutive inactive years
#'                                    following at least one active year.
#'                                    \code{no_moves} indicates ABIs appearing
#'                                    under multiple addresses within the
#'                                    selected date range were excluded prior
#'                                    to analysis. \code{all} indicates all
#'                                    ABIs were retained and compressed: 
#'                                    year-activity indicators across all
#'                                    addresses were summed colum-wise before 
#'                                    event detection.
#' 
#' @field closures_[no_moves|all]_per_sqmi Count of ABIs with at least one
#'                                         qualifying closure event, normalised
#'                                         by the total land area (square
#'                                         miles) of the census boundary unit
#'                                         in the corresponding decennial
#'                                         period.
#' 
#' @field closures_[no_moves|all]_per_10k Count of ABIs with at least one
#'                                        qualifying closure event, normalised
#'                                        by the total resident population of
#'                                        the census boundary unit in the
#'                                        corresponding decennial period,
#'                                        expressed per 10,000 residents.
#'                                        
#' @field closures_[no_moves|all]_avg Mean number of closure events recorded by
#'                                    any singe ABI within the selected 
#'                                    geographic unit and time window.
#' 
#' @field closures_[no_moves|all]_max Maximum number of closure events
#'                                    recorded by any single ABI within the
#'                                    selected geographic unit and time window.
#'                                    
#' @field reopenings_[no_moves|all]_[any|avg|max] Reopening-event counterparts
#'                                                to the closure fields above,
#'                                                sharing identical
#'                                                \code{no_moves}/\code{all}
#'                                                and \code{any}/\code{avg}/
#'                                                \code{max} semantics. A
#'                                                reopening event is defined as
#'                                                two or more consecutive active
#'                                                years immediately following a
#'                                                qualifying closure.
#' 
#' @field n_move Count of ABIs within the geographic unit that relocated at
#'               least once during the selected time window.
#' 
#' @field moves_total Mean number of relocations per ABI within the selected
#'                    geographic unit and time window.
#' 
#' @field wavg_dist_km Mean across all ABIs of each ABI's move-count-weighted
#'                     average relocation distance (km). The per-ABI weighted
#'                     average uses number of moves at each recorded distance
#'                     as weights; this field reports the mean of those per-ABI
#'                     values across the geographic unit.
#' 
#' @field max_dist_km Greatest single-move distance (km) recorded among all
#'                    ABIs within the geographic unit and time window.
#' 
#' @field move_gt_[5|10|25]mi Count of ABIs within the geographic unit that
#'                            made at least one relocation exceeding the stated
#'                            distance threshold (5, 10, or 25 miles).
#' 
#' @field addr1_na Missingness flag. \code{address_line_1} is \code{NA},
#'                 preventing address-based geocoding.
#'                 
#' @field addr1_pobox Missingness flag. \code{address_line_1} contains a PO
#'                    Box value; the physical location cannot be determined.
#'                    
#' @field `[geoid|zcta]_na` Missingness flag. The GEOID or ZCTA value is
#'                          \code{NA}, preventing assignment to a geographic
#'                          unit.

# Save result for select time window.
write_parquet(rollups_accum$by_geoid$block_group, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_blockgroup_08.26.2026.parquet")
write_parquet(rollups_accum$by_geoid$tract, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_tract_08.26.2026.parquet")
write_parquet(rollups_accum$by_geoid$county, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_county_08.26.2026.parquet")
write_parquet(rollups_accum$by_geoid$state, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_state_08.26.2026.parquet")
write_parquet(rollups_accum$by_zcta, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_zcta_08.26.2026.parquet")

# # Save result for all time windows.
# write_parquet(rollups_accum$by_geoid$block_group, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_blockgroup_08.26.2026.parquet")
# write_parquet(rollups_accum$by_geoid$tract, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_tract_08.26.2026.parquet")
# write_parquet(rollups_accum$by_geoid$county, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_county_08.26.2026.parquet")
# write_parquet(rollups_accum$by_geoid$state, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_state_08.26.2026.parquet")
# write_parquet(rollups_accum$by_zcta, "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/metrics_3timeframes_zcta_08.26.2026.parquet")




## ----------------------------------------------------------------
## PART D: Make GeoJSON

## --------------------
## SUBSECTION D1: Confirm Deliverables with Subset

# The metric calculation algorithm is time-intensive; most processing time is
# spent generating metrics across new time windows. Subsetting the data does
# not reduce processing time.
# 
# A single representative time window was calculated and joined with geographic
# boundaries for dashboard testing and deliverable validation.


# --- 1) Read in the TIGER/Line Shapefiles for WI -----------------------------
# --- 1) Read in the TIGER/Line Shapefiles for WI -----------------------------

subset_timeframe <- read_parquet("./Data/Results/KEEP LOCAL/From Generate the Metrics/2010 to 2015 Subset/metrics_2010to2015_tract_08.25.2026.parquet")
setDT(subset_timeframe)


# --- 1) Read in the TIGER/Line Shapefiles for WI -----------------------------

block_geography <- "blocks"   # Optionally geography level = c("blocks", "block groups")
states_present <- "WI"

# Import the relevant states' GeoPackages for this division only (memory-safe).
blocks_by_state <- read_state_gpkgs_for_data(
  states_present, "./Data/Results/Census Bureau TIGER Line Shapefiles/",
  geography = block_geography
)




# --- 2) Retrieve the Census Tract Details for Milwaukee, WI ------------------

# Part of the validation involved creating visualizations and summary metrics
# for a presentation on religious organization closures in Milwaukee, WI
# between 2010 and 2015.
# 
# The current dataset does not include census boundaries at the city level.
# Instead, the Milwaukee, WI Census PLACE boundary was crosswalked to its
# constituent census tracts.


# Milwaukee city place polygon (PLACEFP = 53000) in Wisconsin (STATEFP = 55)
mil_place <- places(state = "WI", year = 2023, class = "sf") %>%
  filter(PLACEFP == "53000") %>%
  st_make_valid()

# All Wisconsin tracts (or you can restrict to county = 079 for speed, see below)
wi_tracts <- tracts(state = "WI", year = 2023, class = "sf") %>%
  st_make_valid()

# Use a projected CRS for area calculations (EPSG:5070 is good for CONUS)
mil_place_5070 <- st_transform(mil_place, 5070)
wi_tracts_5070 <- st_transform(wi_tracts, 5070)

# Intersect tracts with the Milwaukee place polygon
x <- st_intersection(
  wi_tracts_5070 %>% select(GEOID),
  mil_place_5070 %>% select(PLACEFP)
)

# Relationship table: tract GEOID -> PLACEFP, with area share (optional)
tract_place_rel <- x %>%
  mutate(
    inter_area = as.numeric(st_area(geometry))
  ) %>%
  group_by(GEOID) %>%
  mutate(
    tract_area = as.numeric(st_area(st_geometry(wi_tracts_5070[match(GEOID, wi_tracts_5070$GEOID), ]))),
    place_area_share_of_tract = inter_area / tract_area
  ) %>%
  ungroup() %>%
  st_drop_geometry() %>%
  arrange(desc(place_area_share_of_tract))




# If you just want the list of tract GEOIDs that touch the city at all:
milwaukee_tract_geoids <- tract_place_rel %>%
  distinct(GEOID) %>%
  pull(GEOID)

milwaukee_tract_geoids

blocks_mke <- blocks_by_state$WI$blocks_2010 %>%
  mutate(geoid_tract = substr(geoid_block, 1, 11)) %>%
  filter(geoid_tract %in% milwaukee_tract_geoids)


subset_timeframe %>%
  filter(geoid %in% blocks_mke$geoid_tract) %>%
  (\(x) {write.csv(x,
          file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Three Timeframe Subset/milwaukee_tract_closures_2010to2015.csv",
          row.names = FALSE)})()

milwaukee_tract_closures <- subset_timeframe %>%
  filter(geoid %in% blocks_mke$geoid_tract, religion == "all_religions") %>%
  filter(year == 2010)


blocks_mke_joined <- blocks_mke[, c("geoid_tract", "geom")] |>
  dplyr::left_join(
    milwaukee_tract_closures,
    by = c("geoid_tract" = "geoid")
  )

# --- 3) Plot (example: closures_no_moves_any__2010_2015) ---------------------

# Project to an equal-area CRS for nicer-looking city maps
blocks_mke_plot <- st_transform(blocks_mke_joined, 5070)

ggplot() +
  geom_sf(
    data = blocks_mke_plot,
    fill = NA,
    color = "black",
    linewidth = 0.2
  ) +
  geom_sf(
    data = blocks_mke_plot,
    aes(fill = closures_all_any__2010_2015),
    color = NA
  ) +
  # monotone BLUE: low = light, high = dark
  scale_fill_gradient(
    low  = "#deebf7",   # very light blue
    high = "#08519c",   # dark blue
    na.value = "grey85",
    name = "# ABIs",
    labels = scales::label_comma()
  ) +
  coord_sf(crs = sf::st_crs(5070), datum = NA) +
  labs(
    title    = "Place of worship closures, 2010 - 2015",
    subtitle = "Milwaukee, WI"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    panel.grid = element_blank(),
    axis.title = element_blank(),
    axis.text  = element_blank(),
    axis.ticks = element_blank(),
    plot.title    = element_text(size = 14),
    plot.subtitle = element_text(size = 11),
    legend.title  = element_text(size = 11),
    legend.text   = element_text(size = 10)
  )


# --- 4) Metrics --------------------------------------------------------------

mean(milwaukee_tract_closures$addr1_na__2010_2015, na.rm = TRUE)
max(milwaukee_tract_closures$addr1_na__2010_2015, na.rm = TRUE)

mean(milwaukee_tract_closures$addr1_pobox__2010_2015, na.rm = TRUE)
max(milwaukee_tract_closures$addr1_pobox__2010_2015, na.rm = TRUE)

mean(milwaukee_tract_closures$geoid_na__2010_2015, na.rm = TRUE)
max(milwaukee_tract_closures$geoid_na__2010_2015, na.rm = TRUE)

mean(milwaukee_tract_closures$zcta_na__2010_2015, na.rm = TRUE)
max(milwaukee_tract_closures$zcta_na__2010_2015, na.rm = TRUE)


mean(milwaukee_tract_closures$n_move__2010_2015/milwaukee_tract_closures$n_open__2010_2015, na.rm = TRUE)
mean(milwaukee_tract$n_move__2010_2015/milwaukee_tract$n_open__2010_2015, na.rm = TRUE)

mean(milwaukee_tract_closures$moves_total__2010_2015, na.rm = TRUE)
max(milwaukee_tract_closures$moves_total__2010_2015, na.rm = TRUE)
mean(milwaukee_tract_closures$wavg_dist_km__2010_2015, na.rm = TRUE)
mean(milwaukee_tract_closures$max_dist_km__2010_2015, na.rm = TRUE)

mean(milwaukee_tract_closures$closures_all_any__2010_2015, na.rm = TRUE)
sd(milwaukee_tract_closures$closures_all_any__2010_2015, na.rm = TRUE)
mean(milwaukee_tract$closures_all_any__2010_2015, na.rm = TRUE)
sd(milwaukee_tract$closures_all_any__2010_2015, na.rm = TRUE)


## --------------------
## SUBSECTION D2: 

# To efficiently handle block-level GEOID data, state-level shapefiles will be 
# imported by census division (1–9), each grouping as many as 9 states. These 
# will then be compiled into a single lookup table containing both metrics.

# Associate FIPS codes with their corresponding state abbreviations.
fips_lu <- data.table(
  abbr = names(f <- c(AL="01",AK="02",AZ="04",AR="05",CA="06",CO="08",CT="09",DE="10",DC="11",FL="12",
                      GA="13",HI="15",ID="16",IL="17",IN="18",IA="19",KS="20",KY="21",LA="22",ME="23",
                      MD="24",MA="25",MI="26",MN="27",MS="28",MO="29",MT="30",NE="31",NV="32",NH="33",
                      NJ="34",NM="35",NY="36",NC="37",ND="38",OH="39",OK="40",OR="41",PA="42",RI="44",
                      SC="45",SD="46",TN="47",TX="48",UT="49",VT="50",VA="51",WA="53",WV="54",WI="55",WY="56")),
  fips = unname(f)
)

# Compile the state-to-division lookup table.
state_fips_division <- data.table(
  state = c(state.name, "District of Columbia"),
  abbr  = c(state.abb,  "DC"),
  division = c(as.character(state.division), "South Atlantic")
)[fips_lu, on = "abbr"][order(fips), .(state, abbr, fips, division)]

block_geography <- "block groups"   # Optionally geography level = c("blocks", "block groups")
divisions <- unique(state_fips_division$division)

# Load the functions required to compile the lookup tables. If the lookup
# tables have already been produced, this step can be skipped.
source("./Code/Support Functions/For Step 2_2026 Format.R")

data_root <- "./Data/Results/KEEP LOCAL/From Generate the Metrics/2010 to 2015 Subset"


# -- Process GEOIDs ----------------------------------


subset_timeframe <- read_parquet(file.path(data_root, "metrics_2010to2015_blockgroup_08.25.2026.parquet"))
setDT(subset_timeframe)

# Ensure expected key names exist (no normalization, just naming)
# If your df uses geoid_block / decennial_year already, skip this renaming.
if (!("geoid" %in% names(subset_timeframe)) && "geoid_block" %in% names(subset_timeframe)) {
  setnames(subset_timeframe, "geoid_block", "geoid")
}
if (!("year" %in% names(subset_timeframe)) && "decennial_year" %in% names(subset_timeframe)) {
  setnames(subset_timeframe, "decennial_year", "year")
}

# subset_timeframe is your data.table with keys geoid, year
setDT(subset_timeframe)

divisions <- unique(state_fips_division$division)
out_by_div <- vector("list", length(divisions))
names(out_by_div) <- divisions

pb <- progress_bar$new(
  format = "Divisions [:bar] :current/:total (:percent) | :message",
  total  = length(divisions),
  clear  = FALSE,
  width  = 80
)

for (div in divisions) {
  
  pb$tick(tokens = list(message = div))
  
  states_present <- state_fips_division |>
    filter(division == div) |>
    pull(abbr) |>
    unique()
  
  polys_by_state <- read_state_gpkgs_for_data(
    states_present,
    "./Data/Results/Census Bureau TIGER Line Shapefiles/",
    geography = block_geography
  )
  
  poly_div <- bind_rows(lapply(polys_by_state, function(st_list) {
    bind_rows(lapply(st_list, function(x) {
      if (!inherits(x, "sf")) return(NULL)
      x |>
        transmute(
          geoid = geoid,
          year  = as.integer(decennial_year),
          geometry = sf::st_geometry(x)
        )
    }))
  })) |>
    distinct(geoid, year, .keep_all = TRUE)
  
  # keep each division result as data.table (faster to rbind later)
  out_by_div[[div]] <- as.data.table(
    as_tibble(subset_timeframe) |>
      left_join(poly_div, by = c("geoid", "year"))
  )
  
  rm(polys_by_state, poly_div); gc()
}

# fast row-bind at the end (data.table)
result_dt <- rbindlist(out_by_div, use.names = TRUE, fill = TRUE)






tiger_root <- "./Data/Results/Census Bureau TIGER Line Shapefiles/"

# levels to run (edit the "geography" values to match read_state_gpkgs_for_data())
levels <- data.table(
  level     = c("blockgroup", "tract", "county", "state"),
  parquet   = c("metrics_2010to2015_blockgroup_08.25.2026.parquet",
                "metrics_2010to2015_tract_08.25.2026.parquet",
                "metrics_2010to2015_county_08.25.2026.parquet",
                "metrics_2010to2015_state_08.25.2026.parquet"),
  geography = c("block groups", "tracts", "counties", "states")
)

divisions <- unique(state_fips_division$division)

pb <- progress_bar$new(
  format = "Level :level | Division [:bar] :current/:total (:percent) | :message",
  total  = nrow(levels) * length(divisions),
  clear  = FALSE,
  width  = 90
)

for (i in seq_len(nrow(levels))) {
  
  lvl      <- levels$level[i]
  pq_file  <- levels$parquet[i]
  geog     <- levels$geography[i]
  
  # ---- read metrics for this level
  subset_timeframe <- read_parquet(file.path(data_root, pq_file))
  setDT(subset_timeframe)
  
  # standardize key names (names only)
  if (!("geoid" %in% names(subset_timeframe)) && "geoid_block" %in% names(subset_timeframe)) {
    setnames(subset_timeframe, "geoid_block", "geoid")
  }
  if (!("year" %in% names(subset_timeframe)) && "decennial_year" %in% names(subset_timeframe)) {
    setnames(subset_timeframe, "decennial_year", "year")
  }
  stopifnot(all(c("geoid", "year") %in% names(subset_timeframe)))
  
  # ---- per-level output dir (so no slow rbind at the end)
  out_dir <- file.path(data_root, paste0("joined_geom_", lvl))
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  for (div in divisions) {
    
    pb$tick(tokens = list(level = lvl, message = div))
    
    states_present <- state_fips_division |>
      filter(division == div) |>
      pull(abbr) |>
      unique()
    
    polys_by_state <- read_state_gpkgs_for_data(
      states_present,
      tiger_root,
      geography = geog
    )
    
    # IMPORTANT: assumes each sf already has columns: geoid, decennial_year
    poly_div <- bind_rows(lapply(polys_by_state, function(st_list) {
      bind_rows(lapply(st_list, function(x) {
        if (!inherits(x, "sf")) return(NULL)
        x |>
          transmute(
            geoid = geoid,
            year  = as.integer(decennial_year),
            geometry = sf::st_geometry(x)
          )
      }))
    })) |>
      distinct(geoid, year, .keep_all = TRUE)
    
    div_joined <- as.data.table(
      as_tibble(subset_timeframe) |>
        left_join(poly_div, by = c("geoid", "year"))
    )
    
    out_path <- file.path(out_dir, paste0("joined_", gsub("[^A-Za-z0-9]+", "_", div), ".parquet"))
    write_parquet(div_joined, out_path)
    
    rm(polys_by_state, poly_div, div_joined); gc()
  }
  
  rm(subset_timeframe); gc()
}

