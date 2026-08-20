## ----------------------------------------------------------------
## 
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
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
## The tidycensus package was explored but ultimately replaced with the tigris
## package. The tidycensus implementation was not retained, though the premise
## is similar between the two packages.
## 
## For those looking to use tidycensus instead, a client API key must be
## registered and configured with the US Census Bureau. These credentials
## CANNOT be shared and must remain private to each user. They must be kept
## untracked by Git, stored locally, and never published to GitHub.
## 
## Follow the steps below to set up your credentials and environment.
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
##    - PART A: PREPARE DATA FOR METRIC CALCULATION
##        * SUBSECTION A1: Define the Date Ranges
##        * SUBSECTION A2: Fill in Minor Years-Open Gaps
##        * SUBSECTION A3: Add State and County FIPS Codes
## 
##    - PART B: Calculate the Counts Metrics
##        * SUBSECTION B1: Persistence
##        * SUBSECTION B2: Openings and Closures
##        * SUBSECTION B3: Validation
##        * SUBSECTION B4: Generate the Openings, Closures, and Persistence Counts
## 
##    - PART C: CALCULATE THE RATE METRICS
##        * SUBSECTION C1: Retrieve the Census Populations and Boundary Area
##        * SUBSECTION C2: Generate Rate Per 10,000 Persons
##        * SUBSECTION C3: Generate Rate Per Square Miles

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("stringi")          # Low-level string processing and encoding
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
  library("tidycensus")       # Retrieves US Census and ACS data with geometries
  library("purrr")            # Functional programming tools
  library("lubridate")        # Date and time parsing and arithmetic
  library("future.apply")     # Parallel processing
  library("progress")         # Displays progress bars for loops
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Generate the Metrics_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# The original approach was to process data with census boundaries appended
# via the spatial join in Step 5. However, this proved more computationally
# intensive and time-consuming than expected. As a result, Step 4 was used
# temporarily to develop this script. Census boundaries were approximated
# using ZIP codes to annotate entries with county and state FIPS codes via
# a simple table join ("SUBSECTION A3: Add FIPS Codes").
#
# Once more results from Step 5 became available, they were used in place of the
# Step 4 results to generate the final dashboard metrics used at the Summer 2025
# symposium.

# In place of Step 5 results, Step 4 results were used.
step_4 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 04_Cluster Addresses and Collapse By Area_06.17.2025.csv.gz",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()


# When Step 5 results become available, load them in place of Step 4.
step_5 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 05_Append Decennial Info Associated with an Area_05.27.2025.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

df <- step_5   # Data variable referenced in the script.




## ----------------------------------------------------------------
## PART A: PREPARE DATA FOR METRIC CALCULATION

## --------------------
## SUBSECTION A1: Define the Date Ranges

# Metrics must be calculated across all potential user-selected date ranges
# spanning 2001 to 2021. To support closure event detection, defined as a
# business not filing for four or more consecutive years, the minimum
# selectable range is set to five years.

years <- seq(2001, 2021)  # Total span of time available
gap   <- seq(5, 20)  # Range of gaps selectable, from minimum of 5 years to all dates.


# For each allowed range length (5–20 years), generate all possible start/end
# year pairs within 2001–2021, then combine into one table.

all_ranges <- vector("list", length(gap))  # Initialize the empty list
for (i in seq_along(gap)) {
  all_ranges[[i]] <- find_date_combinations(years, gap[i])  # start/end pairs
}

# Stack all gap results into a single data frame: startDate, endDate
all_ranges <- do.call(rbind, all_ranges) %>%
  as.data.frame() %>%
  `colnames<-`(c("startDate", "endDate")) %>%
  `rownames<-`(NULL)


# Leading up to the Summer 2025 prototype release, it was discussed which
# decennial year basemap should be used: should a single basemap apply across
# all user-selected date ranges? Should it update automatically based on the
# user's selection? Should the user be able to manually select a basemap
# independent of their chosen date range?
# 
# The following function determines which decennial year basemap to use based
# on the user's selected date range, supporting two methods: "ending" selects
# the basemap corresponding to the final year of the range; "spanning" selects
# the basemap covering the decennial period into which the majority of the
# selected years fall.

# Example demonstrating basemap selection logic.
user_range <- 1

decide_reference(
  all_ranges[user_range, "startDate"], 
  all_ranges[user_range, "endDate"], 
  method = "ending"
)


## --------------------
## SUBSECTION A2: Fill in Minor Years-Open Gaps

# A closure is defined as an event in which four or more consecutive years have 
# no filings under any address. To simplify metric calculations, these missing 
# years will be filled in. Care must be taken to ensure that this process does 
# not induce duplicate records in the event of temporary relocations within 
# intervening years.


# Initialize progress bar
total_groups <- df %>% group_by(abi) %>% n_groups()
pb <- progress_bar$new(
  format = "  processing [:bar] :percent eta: :eta",
  total = total_groups, 
  clear = FALSE, width = 60
)

# Replace the year columns (e.g., 2001–2021) with the “filled” versions
df[, colnames(select(df, starts_with("20")))] <-
  # Pull only the year columns
  select(df, starts_with("20")) %>%
  # Row-wise: collapse each row to a string, run the fill step, split back to digits
  (\(x) {
    apply(x, 1, function(y) {
      fill_zeros_with_progress(pb, str_flatten(y)) %>%
        (\(z) str_split_1(z, pattern = ""))()
    })
  })() %>%
  # Convert back to the original row/column layout
  t() %>%
  as.data.frame() %>%
  # Ensure values are numeric (not character)
  (\(x) sapply(x, as.numeric))() %>%
  # Restore original year column names
  `colnames<-`(colnames(select(df, starts_with("20"))))


# After filling in the identified zero patterns, the next step is to confirm
# that no extraneous records have been inadvertently introduced. For example,
# a business may appear at an alternate location for a single year, surrounded
# by years where it was recorded at its primary location.

# NOTE: Results were already generated and saved. Load them below.


# Initialize progress bar
total_groups <- df %>% group_by(abi, decennial_census) %>% n_groups()
pb <- progress_bar$new(
  format = "  processing [:bar] :percent eta: :eta",
  total = total_groups,
  clear = FALSE, width = 60
)

test_fill <- df %>%
  # Process separately within each ABI × decennial_census group
  group_by(abi, decennial_census) %>%
  # For each group (.x), run the processing function and update the progress bar
  group_modify(~ process_with_progress(pb, .x, check_all_counts_0_or_1)) %>%
  # Drop grouping structure after processing
  ungroup() %>%
  # Ensure the result is a base data.frame (not a grouped tibble)
  as.data.frame()


#' @description 
#' Codebook for the output fields produced by the evaluation.
#'
#' @field abi Unique business identifier. Evaluation is performed over each 
#'            unique business ID.
#' @field decennial_census The decennial census period: 2000, 2010, or 2020.
#'                         Each value should be replicated across all rows,
#'                         as rows represent distinct census boundary
#'                         metadata records.
#' @field `2001:2021` Column-wise sum of all entries associated with the given 
#'                    business ID.
#' @field all_counts_0_or_1 Boolean. TRUE if all date entry sums for the given 
#'                          business ID are equal to 0 or 1.

# # Commit results.
# write.csv(test_fill, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Confirm No Duplicates Added After Filling Ones_05.26.2025.csv")

# Load in the pre-produced results.
test_fill <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Confirm No Duplicates Added After Filling Ones_05.26.2025.csv",
                      col_types = cols(...1 = col_skip())) %>% as.data.frame()


# All entries passed, confirming each has a unique years-open value.
round(prop.table(table(test_fill$all_counts_0_or_1))*100, digits = 2)


## --------------------
## SUBSECTION A3: Add State and County FIPS Codes

# While Step 5 was running, the 2010 ZCTA-to-County crosswalk file was used
# to annotate ZIP codes with county and state FIPS codes in the Step 4 results. 
# The results of this section were used solely to develop and test the 
# subsequent sections until Step 5 results became available. This step is now 
# skipped, as Step 5 output includes the necessary census boundary designations: 
# tract, county, and state.
#
# The crosswalk file data source is consistent with the one used by Dr. Insang 
# Song in his version of this process.

col_classes <- c(
  # --- Identifiers ---
  "ZCTA5" = "character", "STATE" = "character",
  "COUNTY" = "character", "GEOID" = "character",
  
  # --- Point-level Counts ---
  "POPPT" = "numeric", "HUPT" = "numeric",
  "AREAPT" = "numeric", "AREALANDPT" = "numeric",
  
  # --- ZIP Code Totals & Percentages ---
  "ZPOP" = "numeric", "ZHU" = "numeric",
  "ZAREA" = "numeric", "ZAREALAND" = "numeric",
  "ZPOPPCT" = "numeric", "ZHUPCT" = "numeric",
  "ZAREAPCT" = "numeric", "ZAREALANDPCT" = "numeric",
  
  # --- County Totals & Percentages ---
  "COPOP" = "numeric", "COHU" = "numeric",
  "COAREA" = "numeric", "COAREALAND" = "numeric",
  "COPOPPCT" = "numeric", "COHUPCT" = "numeric",
  "COAREAPCT" = "numeric", "COAREALANDPCT" = "numeric"
)

# Loading ZCTA-to-County mapping file for geocoding using address ZIP codes.
zip_fips_mapping <- read.table("./Data/Raw/zcta_county_rel_10.txt", header = TRUE, sep = ",", colClasses = col_classes)


# Keep one ZIP→FIPS match per ZIP:
# - group by ZIP (ZCTA5)
# - sort candidate matches by POPPT (population weight) descending
# - take the top row (highest POPPT) as the “best” mapping
zip_fips_mapping <- zip_fips_mapping %>%
  group_by(ZCTA5) %>%
  arrange(desc(POPPT)) %>%
  slice(1) %>%
  ungroup() %>%
  # Keep only the fields needed for joining
  select(ZCTA5, STATE, COUNTY) %>%
  # Standardize column names used downstream
  rename(zipcode = ZCTA5, state_fips = STATE, county_fips = COUNTY) %>%
  # Ensure ZIP is character (preserves leading zeros and matches df join type)
  mutate(zipcode = as.character(zipcode))

# Join state/county FIPS onto the main dataset using the ZIP mapping
data_with_fips <- add_fips_codes(df[1:10,], zip_fips_mapping)

# Drop records where the ZIP could not be mapped to either a state or county FIPS
df <- data_with_fips %>%
  filter(!is.na(state_fips) | !is.na(county_fips))




## ----------------------------------------------------------------
## PART B: Calculate the Counts Metrics

# Four metrics were requested for visualization on the dashboard:
#   1. Number of churches closed
#   2. Churches closed rate per 10,000 persons
#   3. Churches closed per square mile
#   4. Church persistence rate
# 
# The first metric represents a simple count of churches closed within a given
# area. The remaining three were calculated using modified versions of the
# definitions developed by Dr. Insang Song. Refer to
# "data/church_tabulation_5year_periods.R" in his GitHub repository for
# further details.
# 
# The non-rate metrics were calculated first, as the denominator information
# required for the rate metrics is introduced at the time of calculation.
# The rate metrics are computed later under "PART C: CALCULATE THE RATE METRICS
# Some sections also discuss and demonstrate the modifications made to Dr. 
# Song's metric definitions and formulas.


## --------------------
## SUBSECTION B1: Persistence

# Definition provided in Dr. Insang Song's code:
#     "Persistence, defined as the sum of institutions during the period.
#      A higher value indicates fewer closures."
# 
# Persistence was calculated by Dr. Insang Song using a "lookahead" approach,
# where an entry was considered persistent if it closed with a status of 1,
# or if it remained open during or after the specified date range.
#
# 
# Scenarios to consider:
# 
# With the dynamic slider, it is possible to select a range where only 0s
# were recorded after the end date, despite the church having been persistently 
# open otherwise. This risk is greater for sub-selections closer to the last 
# recorded date, where some entries lacked a response in the final years of the 
# record. These gaps need not necessarily indicate a closure event under our 
# definition of 4 consecutive years without a response.
# 
# The user may also select an end date that coincides with the last recorded 
# date. In this scenario, a church may fail the persistence test because no 
# information is available after the end date and the final recorded status was 
# 0, not 1, even if the church was otherwise persistent throughout the remainder 
# of the selected period.
# 
# The selected range may also span a temporary closure period, potentially
# underestimating the degree to which the organization was otherwise open.
# 
# 
# Methods and modifications:
#
# Two methods will be made available for evaluating persistence. The first
# replicates the "lookahead" approach used in Dr. Song's published work. The
# second interprets persistence strictly within the selected range,
# contextualizing it over the entire available record rather than looking
# forward only.
# 
# When calculating persistence using the "lookahead" method, any trailing one
# to three 0s are replaced with 1s to avoid undercounting persistence near
# the last recorded date. This modification is applied temporarily and is
# not retained after persistence has been estimated.
# 
# The custom function developed for this purpose is
# get_persistence(method = c("ratio", "lookahead")).


## --------------------
## SUBSECTION B2: Openings and Closures

# A closure is defined as a run of at least four consecutive 0s following an
# open period. A correction is applied to prevent isolated 1s from being used as 
# a closing reference point.
# 
# A reopening is defined as a run of at least two consecutive 1s (i.e., "11"
# or "101") following a closure. A correction is applied such that the reopening 
# count is equal to or no more than one greater than the closure count for the 
# corresponding substring.
# 
# The custom function developed for this purpose is count_closures().


## --------------------
## SUBSECTION B3: Validation

# To test the limits of the church closing, reopening, and persistence
# functions, the following simulation has been set up to evaluate performance.

# Data frame for testing
dt <- data.frame(
  abi = c("A", "B", "C", "D", "E", "F"),
  check.names = FALSE,
  `2001` = c(1, 1, 0, 1, 0, 1),
  `2002` = c(0, 1, 0, 0, 0, 0),
  `2003` = c(1, 1, 0, 0, 1, 0),
  `2004` = c(1, 1, 0, 0, 1, 0),
  `2005` = c(1, 0, 0, 0, 0, 0),
  `2006` = c(1, 0, 0, 0, 1, 1),
  `2007` = c(1, 1, 0, 0, 1, 0),
  `2008` = c(1, 1, 0, 0, 1, 0),
  `2009` = c(1, 1, 0, 0, 0, 0),
  `2010` = c(1, 0, 0, 0, 0, 0),
  `2011` = c(1, 1, 0, 0, 0, 0),
  `2012` = c(1, 1, 0, 0, 0, 1),
  `2013` = c(1, 0, 0, 0, 0, 0),
  `2014` = c(1, 1, 0, 0, 1, 0),
  `2015` = c(0, 1, 0, 0, 1, 0),
  `2016` = c(1, 1, 0, 1, 1, 0),
  `2017` = c(1, 1, 0, 0, 1, 0),
  `2018` = c(1, 0, 0, 0, 1, 0),
  `2019` = c(1, 1, 0, 0, 1, 0),
  `2020` = c(1, 1, 0, 0, 1, 0),
  `2021` = c(0, 1, 0, 0, 1, 0)
)


# Persistence passes expectations
get_persistence(dt, all_ranges[15, "startDate"], all_ranges[15, "endDate"], method = "ratio")

# Closures/reopening's passes expectations
count_closures(dt, all_ranges[136, "startDate"], all_ranges[136, "endDate"])


## --------------------
## SUBSECTION B4: Generate the Openings, Closures, and Persistence Counts

# -- Individual Line Results -------------------------

# First, the individual-level counts of closures, reopenings, and persistence
# are calculated. These are then aggregated by the census boundary units of
# county and state. Under "PART C: CALCULATE THE RATE METRICS", these
# aggregated values are used to calculate the corresponding rate metrics:
# per 10,000 persons and per square mile.

# NOTE: Results were already generated and saved. Load them below.


# Initialize progress bar
pb = txtProgressBar(min = 0, max = nrow(all_ranges), style = 3)

# Iterate over all possible date ranges.
for(i in 1:nrow(all_ranges)) {
  df <- df %>%
    mutate(count_closures(df, all_ranges$startDate[i], all_ranges$endDate[i]),
           get_persistence(df, all_ranges$startDate[i], all_ranges$endDate[i]))

  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}


#' @description 
#' Codebook for the new output fields produced by the evaluation. All other 
#' fields were present in the Step 5 form of the data.
#' 
#' @field closure_count_YYYY-YYYY Integer. The count of closures observed
#'                                between the specified start and end years.
#' @field reopening_count_YYYY-YYYY Integer. The count of reopenings observed
#'                                  between the specified start and end years.
#' @field persistence_YYYY-YYYY Integer. A binary indicator of whether
#'                              persistence was observed between the specified 
#'                              start and end years, where \code{1} indicates 
#'                              persistence and \code{0} indicates none.
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Calculate Counts and Persistence_05.29.2025.csv")

# Load in the pre-produced results.
df <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Calculate Counts and Persistence_05.29.2025.csv",
               col_types = cols(...1 = col_skip())) %>% as.data.frame()


# Brief confirmation that the start and end indices correspond to the count
# columns. Count columns are ordered as closure, reopening, then persistence, 
# spanning from the first to last date range: [2001, 2006] to [2001, 2021].
all_ranges[c(1, 136), ]
colnames(df)[c(42, ncol(df))]


# Function that converts specified numeric columns to binary (1 if > 0, else 0).
convert_to_binary <- function(df, cols_to_convert) {
  df %>%
    mutate(across(all_of(cols_to_convert), ~ if_else(. > 0, 1, 0)))
}


# -- Aggregated by County ----------------------------

# NOTE: Results were already generated and saved. Load them below.


# Initialize progress bar
pb <- txtProgressBar(min = 0, max = n_groups(group_by(df, decennial_census, county_fips, state_fips)), style = 3)

df_county <- df %>%
  # Convert closure and reopening counts to binary. To retain raw counts instead,
  # comment out the mutate_with_progress and use "df %>%" only.
  mutate_with_progress(
    colnames(.)[42:449],
    c("decennial_census", "county_fips", "state_fips"),
    convert_to_binary,
    pb
  ) %>%
  group_by(decennial_census, county_fips, state_fips) %>%
  # Aggregate by county, with decennial periods kept separate to ensure
  # accurate counts within each decennial boundary.
  summarise(across(all_of(colnames(df)[c(42:449)]), \(x) sum(x, na.rm = TRUE))) %>%
  ungroup() %>%
  as.data.frame()


#' @description 
#' Codebook for the new output fields produced by the evaluation. All other 
#' fields were present in the Step 5 form of the data.
#'              
#' NO NEW FIELDS ADDED

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Counts and Persistence By County_05.29.2025.csv")

# Load in the pre-produced results.
df_county <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Counts and Persistence By County_05.29.2025.csv",
                      col_types = cols(...1 = col_skip())) %>% as.data.frame()



# -- Aggregated by State -----------------------------

# NOTE: Results were already generated and saved. Load them below.


# Initialize progress bar
pb <- txtProgressBar(min = 0, max = n_groups(group_by(df, decennial_census, state_fips)), style = 3)

df_state <- df %>%
  # Convert closure and reopening counts to binary. To retain raw counts instead,
  # comment out the mutate_with_progress and use "df %>%" only.
  mutate_with_progress(
    colnames(.)[42:449],
    c("decennial_census", "state_fips"),
    convert_to_binary,
    pb
  ) %>%
  group_by(decennial_census, state_fips) %>%
  # Aggregate by state, with decennial periods kept separate to ensure accurate 
  # counts within each decennial boundary.
  summarise(across(all_of(colnames(df)[c(42:449)]), \(x) sum(x, na.rm = TRUE))) %>%
  ungroup() %>%
  as.data.frame()


#' @description 
#' Codebook for the new output fields produced by the evaluation. All other 
#' fields were present in the Step 5 form of the data.
#'              
#' NO NEW FIELDS ADDED

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Counts and Persistence By State_05.29.2025.csv")

# Load in the pre-produced results.
df_state <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Counts and Persistence By State_05.29.2025.csv",
                     col_types = cols(...1 = col_skip())) %>% as.data.frame()




## ----------------------------------------------------------------
## PART C: CALCULATE THE RATE METRICS

# A Census Bureau API key may no longer be required in this iteration.
# If required, refer to the header section for setup instructions.
census_key <- Sys.getenv("CENSUS_API_KEY")


## --------------------
## SUBSECTION C1: Retrieve the Census Populations and Boundary Area

# In "Clean Raw Data.R", tidycensus was used to pull the census tract, county, 
# and state associated with an address, longitude, and latitude for a given 
# decennial year. However, these results do not appear to align with the 
# population data sourced from the same package. The previous method has been 
# reviewed and is expected to accurately reflect the available sf data.
# 
# Two approaches can be taken to address the misalignment:
#   1. Set the affected entries to NA.
#   2. Use the nearest available decennial population data instead.
# 
# State and county FIPS combinations are checked for a given decennial year
# using get_decennial(variables = "P001001") to confirm existence in the
# database. get_acs(variables = "B01003_001") is excluded as the decennial
# census population value is preferred.

# Note: this is a long-running operation.
df_county[, 1:3] %>%
  rowwise() %>%
  mutate(exists = fips_combination_exists(state_fips, county_fips, decennial_census)) %>%
  ungroup() %>%
  as.data.frame()


# fetch_population_data() pulls population data from the US Census API for
# each unique county-state FIPS combination, or state only if the data is
# state-level. Population values are retrieved for the relevant decennial
# census year: 2000, 2010, or 2020.
# 
# To address missing county, state, and decennial combinations, the function
# supports fallback to alternative decennial years, defined as:
#
# alternatives <- data.frame(
#   decennial_year = c(2000, 2010, 2020),
#   alternative_1  = c(1990, 2000, 2010),
#   alternative_2  = c(2010, 2020, 2000)
# )
#
# The function iterates over the two alternatives in order. If both are
# exhausted, population is set to NA. This behaviour can be disabled by
# setting allow_alternative = FALSE.


## --------------------
## SUBSECTION C2: Generate Rate Per 10,000 Persons

# -- Aggregated by County ----------------------------

# NOTE: Results were already generated and saved. Load them below.

df_10Krate_county <- calculate_closure_rates(
  df_county, 
  geography = "county", 
  use_acs = FALSE, 
  level = "county", 
  allow_alternative = TRUE
)


#' @description 
#' Codebook for the new output fields produced by the evaluation. All other 
#' fields were present after "SUBSECTION B3: Generate the Openings, Closures, 
#' and Persistence Counts".
#' 
#' @field population Population value for the given county and decennial year
#'                   combination. The decennial year used depends on whether
#'                   the API call found a match in the tidycensus database;
#'                   if not, an alternative decennial year may have been used.
#' @field alternative_used Boolean. TRUE if an alternative decennial year was
#'                         used to source the population count, FALSE otherwise.
#' @field closure_rate_per_10000_YYYY-YYYY Closure rate calculated by dividing
#'                                         the closure count by `population`,
#'                                         scaled per 10,000.
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/10K Rate By County_05.30.2025.csv")

# Load in the pre-produced results.
df_10Krate_county <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/10K Rate By County_05.30.2025.csv",
                              col_types = cols(...1 = col_skip())) %>% as.data.frame()


# About one third of population results used an alternative decennial year.
round(prop.table(table(df_10Krate_county$alternative_used, useNA = "ifany"))*100, digits = 2)

# The API successfully returned 2020 population values, but an alternative
# decennial source was used for all entries associated with the 2000 and
# 2010 periods.
round(prop.table(table(
  df_10Krate_county$decennial_census,
  "Alternative Used?" = df_10Krate_county$alternative_used,
  useNA = "ifany"
), margin = 1)*100, digits = 2)

# If `allow_alternative = TRUE`, confirm that no NAs were introduced.
anyNA(df_10Krate_county$population)


# -- Aggregated by State -----------------------------

# NOTE: Results were already generated and saved. Load them below.

df_10Krate_state <- calculate_closure_rates(
  df_state, 
  geography = "state", 
  use_acs = FALSE, 
  level = "state", 
  allow_alternative = TRUE
)


#' @description
#' Codebook for the new output fields produced by the evaluation. All other
#' fields were present following the aggregation of "SUBSECTION B3: Generate
#' the Openings, Closures, and Persistence Counts" results to the county level.
#' 
#' NO NEW FIELDS ADDED
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/10K Rate By State_05.30.2025.csv")

# Load in the pre-produced results.
df_10Krate_state <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/10K Rate By State_05.30.2025.csv",
                             col_types = cols(...1 = col_skip())) %>% as.data.frame()


# About one third of population results used an alternative decennial year.
round(prop.table(table(df_10Krate_state$alternative_used, useNA = "ifany"))*100, digits = 2)

# The API successfully returned 2020 population values, but an alternative
# decennial source was used for all entries associated with the 2000 and
# 2010 periods.
round(prop.table(table(
  df_10Krate_state$decennial_census,
  "Alternative Used?" = df_10Krate_state$alternative_used,
  useNA = "ifany"
), margin = 1)*100, digits = 2)

# If `allow_alternative = TRUE`, confirm that no NAs were introduced.
anyNA(df_10Krate_state$population)


## --------------------
## SUBSECTION C3: Generate Rate Per Square Miles

# -- Aggregated by County ----------------------------

# NOTE: Results were already generated and saved. Load them below.

df_sqMilerate_county <- calculate_closure_rates_per_sq_mile(
  df_county, 
  geography = "county", 
  use_acs = FALSE, 
  level = "county", 
  allow_alternative = TRUE
)


#' @description 
#' Codebook for the new output fields produced by the evaluation. All other 
#' fields were present after "SUBSECTION B3: Generate the Openings, Closures, 
#' and Persistence Counts".
#'
#' @field land_area Land area of the given county in square miles, retrieved
#'                  for the given decennial year combination. The decennial
#'                  year used depends on whether the API call found a match
#'                  in the tidycensus database; if not, an alternative
#'                  decennial year may have been used.
#' @field alternative_used Boolean. TRUE if an alternative decennial year was
#'                         used to source the land area, FALSE otherwise.
#' @field closure_rate_per_sq_mile_YYYY-YYYY Closure rate calculated by dividing
#'                                           the closure count by `land_area`
#'                                           (square miles).
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Sq Mile Rate By County_05.30.2025.csv")

# Load in the pre-produced results.
df_sqMilerate_county <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Sq Mile Rate By County_05.30.2025.csv",
                                 col_types = cols(...1 = col_skip())) %>% as.data.frame()


# No alternative decennial years were required during the API query.
round(prop.table(table(df_sqMilerate_county$alternative_used, useNA = "ifany"))*100, digits = 2)

# If `allow_alternative = TRUE`, confirm that no NAs were introduced.
anyNA(df_sqMilerate_county$population)



# -- Aggregated by State -----------------------------

# NOTE: Results were already generated and saved. Load them below.

df_sqMilerate_state <- calculate_closure_rates_per_sq_mile(
  df_state, 
  geography = "state", 
  use_acs = FALSE, 
  level = "state", 
  allow_alternative = TRUE
)


#' @description
#' Codebook for the new output fields produced by the evaluation. All other
#' fields were present following the aggregation of "SUBSECTION B3: Generate
#' the Openings, Closures, and Persistence Counts" results to the county level.
#' 
#' NO NEW FIELDS ADDED
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# # Commit results.
# write.csv(df, file = "./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Sq Mile Rate By State_05.30.2025.csv")

# Load in the pre-produced results.
df_sqMilerate_state <- read_csv("./Data/Results/KEEP LOCAL/From Generate the Metrics/Summer 2025 Dashboard Prototype_ARCHIVED/Sq Mile Rate By State_05.30.2025.csv",
                                col_types = cols(...1 = col_skip())) %>% as.data.frame()


# No alternative decennial years were required during the API query.
round(prop.table(table(df_sqMilerate_state$alternative_used, useNA = "ifany"))*100, digits = 2)

# If `allow_alternative = TRUE`, confirm that no NAs were introduced.
anyNA(df_sqMilerate_state$population)




## ----------------------------------------------------------------
## PART D: COMPILE RESULTS AND FORMAT FOR THE DASHBOARD

# The prior steps focused on generating the metrics in preparation for
# coordinating the final output with the frontend developer. After meeting
# with the developer, Gordon Tu, on June 3rd, 2025, the following formatting
# was requested to best present the results for visualization and user
# downloading:
#   - Link the county and state FIPS codes with the GEOID variable provided
#     by the TIGER/Line Shapefile, and report them by this variable only.
#   - Merge all three datasets representing the four requested metrics (one
#     for state-level and one for county-level) into a single static file
#     served to the frontend.
#   - Remove the "land_area", "population", and "alternative_used" columns.
#
# WARNING: The population API methods failed to retrieve the appropriate
#          population values for the 2000 and 2010 decennial periods. The
#          alternative source used was not recorded at the time; time
#          constraints precluded revisiting this algorithm prior to the
#          prototype release.


# All 2000 and 2010 results used an alternative decennial reference for total
# population, with no missing values. These results were consistent across
# both the county- and state-level queries using tidycensus.
table("Decennial" = df_10Krate_county$decennial_census, "Alternative Used?" = df_10Krate_county$alternative_used)
df_10Krate_county$population %>% anyNA()

table("Decennial" = df_10Krate_state$decennial_census, "Alternative Used?" = df_10Krate_state$alternative_used)
df_10Krate_state$population %>% anyNA()


# The expected decennial shapefile was used to calculate square mileage, with 
# no missing values.
table("Decennial" = df_sqMilerate_county$decennial_census, "Alternative Used?" = df_sqMilerate_county$alternative_used)
df_sqMilerate_county$land_area %>% anyNA()

table("Decennial" = df_sqMilerate_state$decennial_census, "Alternative Used?" = df_sqMilerate_state$alternative_used)
df_sqMilerate_state$land_area %>% anyNA()


## --------------------
## SUBSECTION D1: Combine and Organize Tables by Geography

# Metric types to bundle for each start/end date
metrics_vector <- c(
  "closure_count",
  "reopening_count",
  "persistence",
  "closure_rate_per_10000",
  "closure_rate_per_sq_mile"
)

# Build a table of the unique date windows. "Combined" is a convenient key/label 
# like "YYYY-MM-DD_YYYY-MM-DD".
dates_table <- all_ranges %>%
  mutate(Combined = str_c(startDate, endDate, sep = "-"))

# ---- Combine county-level outputs ----
# Start with the base county table (df_county), then append:
#  1) per-10K population rates (dropping denominator + metadata columns to avoid duplicates), and
#  2) per-square-mile rates (also dropping denominator + metadata columns).
# All joins align on decennial census year and county/state identifiers.
df_combined_county <- left_join(
  df_county,
  df_10Krate_county[, colnames(df_10Krate_county) %!in% c("population", "alternative_used")],
  by = c("decennial_census", "county_fips", "state_fips")
) %>%
  (\(x) {
    left_join(
      x,
      df_sqMilerate_county[, colnames(df_sqMilerate_county) %!in% c("land_area", "alternative_used")],
      by = c("decennial_census", "county_fips", "state_fips")
    )
  })()

# ---- Combine state-level outputs ----
# Same idea as county, but join keys are just decennial census year + state id.
df_combined_state <- left_join(
  df_state,
  df_10Krate_state[, colnames(df_10Krate_state) %!in% c("population", "alternative_used")],
  by = c("decennial_census", "state_fips")
) %>%
  (\(x) {
    left_join(
      x,
      df_sqMilerate_state[, colnames(df_sqMilerate_state) %!in% c("land_area", "alternative_used")],
      by = c("decennial_census", "state_fips")
    )
  })()


# Order columns by metric bundle within each date range, then by date range.
final_county <- reorder_columns(df_combined_county, metrics_vector, dates_table)
final_state  <- reorder_columns(df_combined_state,  metrics_vector, dates_table)

# Confirm ordering was successful
colnames(final_county)
colnames(final_state)


## --------------------
## SUBSECTION D2: 

# Queries the US Census Bureau TIGER/Line Shapefile database via tigris to
# retrieve county and state geometries by decennial year, then left-joins
# the results to the county- and state-level closure results generated
# earlier by FIPS code.

combined_data_county <- combine_geocoding(final_county)
combined_data_state  <- combine_geocoding(final_state)


# The three decennial shapefiles differ slightly in column names and structure.
# Not all columns need to be retained; the following are necessary:
#   - The geography ID column denoting the state and county combination.
#   - The multipolygon geometry tracing the boundary of the census area.

# State- and county-level columns by decennial period
stateCols_2000  <- c("STATE", "AREA", "PERIMETER", "ST99_D00_", "ST99_D00_I", "NAME", "LSAD", "REGION", "DIVISION", "LSAD_TRANS", "geometry")
countyCols_2000 <- c("AREA", "PERIMETER", "STATE", "COUNTY", "CO99_D00_", "CO99_D00_I", "NAME", "LSAD", "LSAD_TRANS", "COUNTYFP", "STATEFP", "geometry")

stateCols_2010  <- c("GEO_ID", "STATE", "NAME", "LSAD", "CENSUSAREA", "geometry")
countyCols_2010 <- c("GEO_ID", "STATE", "COUNTY", "NAME", "LSAD", "CENSUSAREA", "geometry", "COUNTYFP", "STATEFP")

stateCols_2020  <- c("STATEFP", "STATENS", "AFFGEOID", "GEOID", "STUSPS", "NAME", "LSAD", "ALAND", "AWATER", "geometry")
countyCols_2020 <- c("STATEFP", "COUNTYFP", "COUNTYNS", "AFFGEOID", "GEOID", "NAME", "NAMELSAD", "STUSPS", "STATE_NAME", "LSAD", "ALAND", "AWATER", "geometry")


# Columns present in ALL three decennial periods (stable schema)
Reduce(intersect, list(stateCols_2000, stateCols_2010, stateCols_2020))
Reduce(intersect, list(countyCols_2000, countyCols_2010, countyCols_2020))

# Columns present in AT LEAST two periods (partially stable / renamed over time)
unique(c(
  intersect(stateCols_2000, stateCols_2010),
  intersect(stateCols_2000, stateCols_2020),
  intersect(stateCols_2010, stateCols_2020)
))

unique(c(
  intersect(countyCols_2000, countyCols_2010),
  intersect(countyCols_2000, countyCols_2020),
  intersect(countyCols_2010, countyCols_2020)
))

# All columns observed across any period (union; highlights one-off fields)
unique(c(stateCols_2000, stateCols_2010, stateCols_2020))
unique(c(countyCols_2000, countyCols_2010, countyCols_2020))

# Columns to drop after merging:
# keep only geometry + IDs needed for joins/traceability (GEO_ID/GEOID), drop the rest
state_remove  <- unique(c(stateCols_2000, stateCols_2010, stateCols_2020)) %>%
  .[. %!in% c("geometry", "GEO_ID", "GEOID")]
county_remove <- unique(c(countyCols_2000, countyCols_2010, countyCols_2020)) %>%
  .[. %!in% c("geometry", "GEO_ID", "GEOID")]


## --------------------
## SUBSECTION D3: Add Polygons and Save as GeoJSON

# With the correct IDs identified, the tables are ready to be joined by FIPS.
# The 2000 decennial GEOID must first be constructed for county-level data,
# as the state-level GEOID is simply the state FIPS code.

combined_data_county <- combined_data_county[combined_data_county$decennial_census %in% 2000, ] %>%
  # Generate the county GEOID for 2000 (STATEFP + COUNTYFP)
  mutate(GEOID_2000 = paste0(STATEFP, COUNTYFP)) %>%
  # Join back to the full dataset; non-2000 rows will have GEOID_2000 as NA
  left_join(combined_data_county, .)


# Generate one cohesive geolocation ID variable (geoid) across all decennial 
# periods, then reorganize columns so the outputs are consistent and ready to 
# share.

combined_data_state <- combined_data_state %>%
  # State-level geolocation ID is simply the state FIPS
  mutate(geoid = state_fips) %>%
  # Move geoid + geometry up front; keep the remaining fields in their original order
  select(c(colnames(combined_data_state)[1:2], "geoid", "geometry",
           colnames(combined_data_state)[3:682]))

combined_data_county <- combined_data_county %>%
  # Create a single county geoid across periods by taking the first non-missing ID:
  # GEO_ID (some years), GEOID (some years), then GEOID_2000 (constructed for 2000).
  mutate(geoid = coalesce(GEO_ID, GEOID, GEOID_2000)) %>%
  # Drop the original per-period ID fields now represented by geoid
  select(-GEO_ID, -GEOID, -GEOID_2000) %>%
  # Reorder key identifiers first (decennial_census, state_fips, county_fips),
  # then geoid + geometry, then the remaining variables
  select(c(colnames(combined_data_county)[c(1, 3, 2)], "geoid", "geometry",
           colnames(combined_data_county)[4:683]))



#' @description
#' Codebook for output fields generated after computing all dashboard metrics
#' across all user-selected date ranges for each decennial period, joined with
#' their respective census boundary multipolygon geometries for direct plotting.
#'
#' @field decennial_census The decennial census period: 2000, 2010, or 2020.
#'                         Each value is replicated across all rows, as rows
#'                         represent distinct census boundary metadata records.
#' @field state_fips State-level Federal Information Processing Standard (FIPS)
#'                   code representing the census boundary level to which the
#'                   data was aggregated.
#' @field county_fips County-level Federal Information Processing Standard
#'                    (FIPS) code representing the census boundary level to
#'                    which the data was aggregated. This variable is absent if 
#'                    the data were aggregated to a higher geographic level.
#' @field geoid The GEOID for the census boundary level, representing a
#'              mutually exclusive boundary designation that may include a
#'              combination of state and county FIPS, tract, block group,
#'              and block IDs.
#' @field geometry Multipolygon representation of the GEOID boundary, plotted
#'                 in the mapped component containing the relevant outcome
#'                 results.
#' @field closure_count_YYYY-YYYY Integer. The count of closures observed
#'                                between the specified start and end years.
#' @field reopening_count_YYYY-YYYY Integer. The count of reopenings observed
#'                                  between the specified start and end years.
#' @field persistence_YYYY-YYYY Integer. A binary indicator of whether
#'                              persistence was observed between the specified
#'                              start and end years, where \code{1} indicates
#'                              persistence and \code{0} indicates none.
#' @field closure_rate_per_10000_YYYY-YYYY Closure rate calculated by dividing
#'                                         the closure count by \code{population},
#'                                         scaled per 10,000.
#' @field closure_rate_per_sq_mile_YYYY-YYYY Closure rate calculated by dividing
#'                                           the closure count by \code{land_area}
#'                                           (square miles).
#' 
#' @note The suffix \code{YYYY-YYYY} represents the start and end years of
#'   possible date range selections ("SUBSECTION A1: Define the Date Ranges"), 
#'   where each year is a value between 2001 and 2021 (e.g., \code{2004_2017}).

# Saves the results as a GeoJSON file, preserving the geometry column.
# NOTE: Geometry columns are not supported in CSV format; use GeoJSON
#       (*.geojson) or Shapefile (*.shp) instead, both writable via st_write() 
#       from the sf package.
st_write(combined_data_county, "./Data/Results/Dashboard Datasets/All Metrics By County_06.04.2025.geojson")
st_write(combined_data_state, "./Data/Results/Dashboard Datasets/All Metrics By State_06.04.2025.geojson")





