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
## SUBSECTION C1: 

# Algorithm - Closures
# Subset years columns based on user selection
# Calculate Churches open/closed (by ABI and decennial year)
#   - For including moves, column-wise sum over all entries by BI
#   - For excluding moves, remove ABI with a move tag prior to column-wise sum
#   - Filter ABI with all zeros, these are treated as not open
#   - Count number of ABI, these are businesses open
#   - Calculate closures and reopenings
# Save as list by date range

# Algorithm - Metrics
# Using the first results, call the correct list to process
# 1. Aggregate counts to census boundary (block-level, going up)
# 2. Aggregate over different religions including an "all" category
# 3. Calculate per sq mile and 10,000 people
# Save each results as one datatable with lists as the aggregation level




## ----------------------------------------------------------------
## PART D: Make GeoJSON

# Algorithm - Make GEOJSON




