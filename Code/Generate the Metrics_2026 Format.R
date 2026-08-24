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

rollup_results <- function(discrete_results,
                           agg_fun = function(x) if (is.numeric(x)) sum(x, na.rm = TRUE) else uniqueN(x)) {
  library(data.table)
  
  DT <- as.data.table(discrete_results)
  DT[, geoid := as.character(geoid)]
  DT[, zcta  := as.character(zcta)]
  
  has_relig <- "religion" %in% names(DT)
  
  # what to aggregate (exclude ids + any grouping keys)
  drop_ids <- c("abi", "year", "geoid", "zcta")
  if (has_relig) drop_ids <- c(drop_ids, "religion")
  agg_cols <- setdiff(names(DT), drop_ids)
  
  levels <- list(
    state       = 2L,
    county      = 5L,
    tract       = 11L,
    block_group = 12L,
    block       = 15L
  )
  
  # per-column aggregator (no current_column())
  agg_one <- function(nm, x) {
    if (grepl("^wavg_dist_km__", nm)) {
      if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
    } else if (grepl("^max_dist_km__", nm)) {
      if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
    } else {
      agg_fun(x)
    }
  }
  
  # helper: add per-capita / per-area rates after rollup
  add_rates <- function(out, id_col = c("geoid", "zcta")) {
    if (is.null(out) || !nrow(out)) return(out)
    
    id_col <- match.arg(id_col)
    pop_col  <- if (id_col == "geoid") "geoid_pop"     else "zcta_pop"
    area_col <- if (id_col == "geoid") "geoid_sqMiles" else "zcta_sqMiles"
    
    cnm_cols <- grep("^closures_no_moves__", names(out), value = TRUE)
    ca_cols  <- grep("^closures_all__",     names(out), value = TRUE)
    
    for (cc in cnm_cols) {
      suf <- sub("^closures_no_moves__", "", cc)
      pop_rate  <- paste0("closures_no_moves_per_pop__",  suf)
      area_rate <- paste0("closures_no_moves_per_sqmi__", suf)
      
      out[, (pop_rate)  := fifelse(is.na(get(pop_col))  | get(pop_col)  == 0, NA_real_, get(cc) / get(pop_col))]
      out[, (area_rate) := fifelse(is.na(get(area_col)) | get(area_col) == 0, NA_real_, get(cc) / get(area_col))]
    }
    
    for (cc in ca_cols) {
      suf <- sub("^closures_all__", "", cc)
      pop_rate  <- paste0("closures_all_per_pop__",  suf)
      area_rate <- paste0("closures_all_per_sqmi__", suf)
      
      out[, (pop_rate)  := fifelse(is.na(get(pop_col))  | get(pop_col)  == 0, NA_real_, get(cc) / get(pop_col))]
      out[, (area_rate) := fifelse(is.na(get(area_col)) | get(area_col) == 0, NA_real_, get(cc) / get(area_col))]
    }
    
    out
  }
  
  # roll up by a GEOID level (no zcta in grouping)
  roll_geoid <- function(L) {
    x <- DT[nchar(geoid) >= L]
    if (!nrow(x)) return(NULL)
    x[, geoid_roll := substr(geoid, 1L, L)]
    
    by_expr <- if (has_relig) quote(.(geoid = geoid_roll, religion)) else quote(.(geoid = geoid_roll))
    
    out <- x[, {
      vals <- lapply(agg_cols, function(nm) agg_one(nm, get(nm)))
      setNames(vals, agg_cols)
    }, by = eval(by_expr)]
    
    add_rates(out, id_col = "geoid")
  }
  
  out_geoid <- lapply(levels, roll_geoid)
  out_geoid <- out_geoid[!vapply(out_geoid, is.null, logical(1))]
  
  # roll up by ZCTA only (once)
  by_expr_z <- if (has_relig) quote(.(zcta, religion)) else quote(.(zcta))
  
  out_zcta <- DT[, {
    vals <- lapply(agg_cols, function(nm) agg_one(nm, get(nm)))
    setNames(vals, agg_cols)
  }, by = eval(by_expr_z)]
  
  out_zcta <- add_rates(out_zcta, id_col = "zcta")
  
  list(
    by_geoid = out_geoid,  # named list: $block, $block_group, $tract, $county, $state (where available)
    by_zcta  = out_zcta    # single DT aggregated by zcta (and religion, if present)
  )
}


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


date_range <- build_year_windows(church_2026_form_analysis, min_span = 5L) %>% .[1:2, ]

relig_cols <- c(
  "buddhist_temple", "christian_church", "hindu_mandir", "jewish_synagogue",
  "muslim_mosque", "sikh_gurdwara", "other_religion", "interfaith", "unspecified"
)


pb <- utils::txtProgressBar(min = 0, max = nrow(date_range), style = 3)
on.exit(close(pb), add = TRUE)

rollups_accum <- NULL

for (i in seq_len(nrow(date_range))) {
  
  start_y <- date_range$start[i]
  end_y   <- date_range$end[i]
  yrs     <- start_y:end_y
  lab     <- date_range$label[i]
  
  message("Window: ", lab, " (", start_y, "–", end_y, ")")
  utils::setTxtProgressBar(pb, i)
  
  church_by_years <- filter_ts(church_2026_form_analysis, yrs)
  data.table::setDT(church_by_years)
  
  # collect ALL religion-specific discrete results for this window (ABI-year grain)
  discrete_list <- vector("list", length(relig_cols))
  names(discrete_list) <- relig_cols
  k <- 0L
  
  for (relig_j in relig_cols) {
    
    abi_keep <- abi_any_true(church_by_years, cols = relig_j)
    if (length(abi_keep) == 0L) next
    
    church_relig <- church_by_years[abi %in% abi_keep]
    if (nrow(church_relig) == 0L) next
    
    # --- flags on the religion-filtered universe ---
    flag_counts <- church_relig[
      ,
      .(
        n_abi = data.table::uniqueN(abi),
        abi_addr1_is_na    = data.table::uniqueN(abi[addr1_is_na    %in% TRUE]),
        abi_addr1_is_pobox = data.table::uniqueN(abi[addr1_is_pobox %in% TRUE]),
        abi_geoid_is_na    = data.table::uniqueN(abi[geoid_is_na    %in% TRUE]),
        abi_zcta_is_na     = data.table::uniqueN(abi[zcta_is_na     %in% TRUE])
      ),
      by = .(geoid, zcta)
    ]
    data.table::setnames(flag_counts, old = "n_abi", new = "n_open", skip_absent = TRUE)
    
    # --- clean on the religion-filtered universe ---
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
    if (length(yrs_present) == 0L) next
    
    message("  Religion: ", relig_j, " | years: ", min(yrs_present), "–", max(yrs_present))
    
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
    
    # add identifiers (needed for suffixing; will be dropped before rollup)
    flag_counts[,  `:=`(start_y = start_y, end_y = end_y, label = lab, religion = relig_j)]
    closures_dt[,  `:=`(start_y = start_y, end_y = end_y, label = lab, religion = relig_j)]
    
    church_join <- unique(church_clean[, c(
      "abi", "year", "geoid", "zcta", "geoid_pop",
      "geoid_sqMiles", "zcta_pop", "zcta_sqMiles"
    )])
    
    flag_counts_aug <- flag_counts[church_join, on = .(geoid, zcta)]
    closures_aug    <- closures_dt[church_join, on = .(abi, year)]
    
    # re-assert religion right before join (prevents losing it)
    flag_counts_aug[, religion := relig_j]
    closures_aug[,    religion := relig_j]
    
    # suffix metric columns by window label
    id_keep_flags <- c("geoid","zcta","start_y","end_y","label","religion",
                       "abi","year","geoid_pop","geoid_sqMiles","zcta_pop","zcta_sqMiles")
    metric_cols_flags <- setdiff(names(flag_counts_aug), id_keep_flags)
    if (length(metric_cols_flags) > 0L) {
      data.table::setnames(flag_counts_aug, metric_cols_flags, paste0(metric_cols_flags, "__", lab))
    }
    
    id_keep_clos <- c("abi","year","start_y","end_y","label","religion",
                      "geoid","zcta","geoid_pop","geoid_sqMiles","zcta_pop","zcta_sqMiles")
    metric_cols_clos <- setdiff(names(closures_aug), id_keep_clos)
    if (length(metric_cols_clos) > 0L) {
      data.table::setnames(closures_aug, metric_cols_clos, paste0(metric_cols_clos, "__", lab))
    }
    
    # NA count only onto flags
    cnm    <- paste0("closures_no_moves__", lab)
    na_col <- paste0("na_closures_no_moves__", lab)
    if (cnm %in% names(closures_aug)) {
      na_by_geo <- closures_aug[, .(tmp_na = sum(is.na(get(cnm)))), by = .(geoid, zcta)]
      data.table::setnames(na_by_geo, "tmp_na", na_col)
      flag_counts_aug <- na_by_geo[flag_counts_aug, on = .(geoid, zcta)]
    }
    
    discrete_results <- closures_aug[
      flag_counts_aug,
      on = .(
        abi, year, geoid, zcta, geoid_pop, geoid_sqMiles, zcta_pop, zcta_sqMiles,
        start_y, end_y, label, religion
      )
    ]
    
    # drop cols start_y, end_y, and label
    drop_cols <- intersect(c("start_y", "end_y", "label"), names(discrete_results))
    if (length(drop_cols) > 0L) discrete_results[, (drop_cols) := NULL]
    
    k <- k + 1L
    discrete_list[[k]] <- discrete_results
  }
  
  # compile to ONE table (all religions) then roll up ONCE
  discrete_list <- discrete_list[seq_len(k)]
  if (length(discrete_list) == 0L) next
  
  discrete_window <- data.table::rbindlist(discrete_list, fill = TRUE, use.names = TRUE)
  
  message("    Rollup: ", lab, " | ALL religions")
  rollups <- rollup_results(discrete_window)
  
  # accumulate rollups across windows (merge by id + religion)
  if (is.null(rollups_accum)) {
    rollups_accum <- rollups
  } else {
    
    for (lvl in union(names(rollups_accum$by_geoid), names(rollups$by_geoid))) {
      a <- rollups_accum$by_geoid[[lvl]]
      b <- rollups$by_geoid[[lvl]]
      
      if (is.null(a)) {
        rollups_accum$by_geoid[[lvl]] <- b
      } else if (!is.null(b)) {
        
        key_cols <- intersect(c("geoid", "religion"), names(a))
        overlap  <- intersect(setdiff(names(a), key_cols), setdiff(names(b), key_cols))
        if (length(overlap) > 0L) b[, (overlap) := NULL]
        
        rollups_accum$by_geoid[[lvl]] <- merge(a, b, by = key_cols, all = TRUE)
      }
    }
    
    {
      a <- rollups_accum$by_zcta
      b <- rollups$by_zcta
      
      key_cols <- intersect(c("zcta", "religion"), names(a))
      overlap  <- intersect(setdiff(names(a), key_cols), setdiff(names(b), key_cols))
      if (length(overlap) > 0L) b[, (overlap) := NULL]
      
      rollups_accum$by_zcta <- merge(a, b, by = key_cols, all = TRUE)
    }
  }
}

# FINAL OUTPUT:
# rollups_accum is a list:
#   - rollups_accum$by_geoid: named list of DTs ($state, $county, $tract, $block_group, $block)
#     where each window label contributes new metric__<label> columns
#   - rollups_accum$by_zcta: a DT keyed by zcta with the same property



# only output: discrete_long
discrete_long



# 5. Save the results. These represent the results for the smallest available
#    census boundary. Over this, sums can be made for each GEOID level.
discrete_results <- closures[church_clean[, c(
  "abi", "year", "geoid", "zcta", "geoid_pop", 
  "geoid_sqMiles", "zcta_pop", "zcta_sqMiles"
)], on = "abi"] %>%
  relocate(year, .after = abi) %>%
  relocate(geoid, .after = year) %>%
  relocate(zcta, .after = geoid)

# Add the missing data info
discrete_results <- discrete_results[discrete_results[
  ,
  .(na_closures_no_moves = sum(is.na(closures_no_moves))),
  by = .(geoid, zcta)
], on = .(geoid, zcta)][flag_counts, on = .(geoid, zcta)] %>%
  relocate(na_closures_no_moves, .after = n_abi)

rollups <- rollup_results(discrete_results)




# Algorithm
# Subset years columns based on user selection
# Filter any ABI with no 1's; no metrics to calculate
# Remove ABI if any entries have: 
#   - A PO Box in the address
#   - NA in the first part of the address
#   - Missing GEOID/ZCTA
# Record the number of ABI removed; where possible aggregate this by GEOID/ZCTA. 
# Otherwise, apply ABI removed by missing GEOID/ZCTA as for that date selection.
# Prepare to calculate open/closed (by ABI and decennial year)
#   - For including moves, column-wise sum over all entries by ABI
#   - For excluding moves, remove ABI with a move tag prior to column-wise sum
# Calculate closures; add a column counting the number of qualifying closure events
# Calculate reopenings; add a column counting the number of qualifying reopening events
# Aggregate to the GEOID/ZCTA and include the total number of ABI for that region by decennial period
# Calculate the rates per 10,000 people and per sq mile.
# Save each aggregation as a table by region and add columns for new date ranges.


# Algorithm - Closures
# Subset years columns based on user selection
# Calculate Churches open/closed (by ABI and decennial year)
#   - For including moves, column-wise sum over all entries by ABI
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




