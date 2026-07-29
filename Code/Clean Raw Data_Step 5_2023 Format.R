## ----------------------------------------------------------------
## Decennial Census Boundary Assignment (2000, 2010, 2020)
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 28th, 2026
## 
## Description: The data spans three decennial years: 2000, 2010, and 2020. 
##              Because users can select varying date ranges and may expect 
##              temporally accurate census boundary mappings, each address must 
##              be annotated with the census boundaries associated with all 
##              three decennial years.
## 
##              These annotations can then be aggregated to GEOIDs that translate 
##              to maps accurately reflecting changes in census boundaries over 
##              time. This is especially important for smaller census boundaries, 
##              such as block groups and tracts, which are prone to updates 
##              between decennial periods.
## 
##              Three sources were explored; ultimately, the tigris package was
##              used for this implementation. In the 2026 Formatted version of
##              this step, the decennial TIGER/Line Shapefiles were downloaded
##              directly to ensure correct decennial year retrieval and avoid
##              API lags or request failures. Census boundaries were then
##              assigned by point-in-polygon matching using each entry's
##              verified geocoordinates.
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
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Annotate with the Decennial Tract, County, and State
##        * SUBSECTION A1: tigris Algorithm
##        * SUBSECTION A2: Assess the Algorithms Performance
## 
##    - PART B: US Census Bureau Geocoder Algorithm (ARCHIVED)

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("purrr")            # Functional programming tools
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
  library("httr")             # HTTP requests (GET/POST) for web APIs
  library("jsonlite")         # JSON parsing and generation (to/from R objects)
  library("future.apply")     # Parallel processing
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 5_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))

# # Load the API key if using tidycensus
# census_key <- Sys.getenv("CENSUS_API_KEY")




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Read in previously generated results.
step_4 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 04_Cluster Addresses and Collapse By Area_06.17.2025.csv.gz",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

# Running this algorithm on the full dataset was not feasible within the
# time constraints of the Summer 2025 symposium prototype. Consequently,
# Only 1% of the results were able to be processed and used to generate the
# results for visualization across the contiguous US.
# 
# Refer to the generated results to determine which entries were retained
# for downstream processing.




## ----------------------------------------------------------------
## PART A: Annotate with the Decennial Tract, County, and State

# Three sources were explored to retrieve this information for the census tract, 
# county, and state:
# 
#                tidycensus: https://walker-data.com/tidycensus/index.html
#                    tigris: https://cran.r-project.org/web/packages/tigris/index.html
# US Census Bureau Geocoder: https://geocoding.geo.census.gov/geocoder/
# 
# All three pull information from the US Census Bureau, but each has its own 
# limitations. For example, the Geocoder was considered because it is already 
# used in Step 3, and using the same resource could allow both steps to be 
# combined into a single query. Unfortunately, this source is restricted to the 
# 2010 and 2020 decennial periods.
#
# tidycensus was another viable option but presented challenges in retrieving
# the various data points required. Finally, the tigris package was selected
# for its ease of use and straightforward implementation of the desired outcome,
# though its differences from tidycensus were minimal.
#
# The tigris implementation was used going forward; the tidycensus counterpart
# was not retained. The Geocoder algorithm is kept below for reference only.


## --------------------
## SUBSECTION A1: tigris Algorithm

# Isolate the four columns used in the algorithm for point-in-polygon spatial 
# assignments.
address_data <- step_4[, c("abi", "area", "compiled_address", "latitude", "longitude")] %>%
  rename(address = compiled_address, lat = latitude, lon = longitude)

# Entries with NA geocoordinates are isolated below. These will be filtered out
# by find_census_geographies_sf() prior to assigning spatial metadata.
no_geo <- address_data[is.na(address_data$lat) | is.na(address_data$lon), ]

# No entries lack at least one geocoordinate.
round(nrow(no_geo)/nrow(address_data) * 100, digits = 2)


years <- c(2000, 2010, 2020)   # Specify decennial years to search
census_info <- list()   # Initialize the empty lists
pb = txtProgressBar(min = 0, max = nrow(address_data), style = 3)   # Initialize progress bar

for(i in 1:nrow(address_data)) {
  census_info[[i]] <- find_census_geographies_sf(address_data[i, ], years)
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}


# Format the output in preparation for rejoining it into the main dataset.
result <- census_info %>%
  bind_rows() %>%
  st_drop_geometry() %>% 
  rename(
    compiled_address = address, 
    decennial_census = Year,
    state_fips = State, 
    county_fips = County) %>%
  rename_with(tolower) %>%
  as.data.frame()

# Join in the details and reorganize the columns for clarity.
step_5 <- left_join(step_4, result[,-3], by = c("abi", "area"), relationship = "many-to-many") %>%
  relocate(verifiedGeo, .after = longitude) %>%
  relocate(decennial_census, .after = verifiedGeo) %>%
  relocate(tract, .after = decennial_census) %>%
  relocate(tract_name, .after = tract) %>%
  relocate(state_fips, .after = tract_name) %>%
  relocate(county_fips, .after = state_fips)


#' @description
#' Codebook for new output fields produced during the data cleaning and
#' validation step. All other fields were present in the Step 4 form of
#' the data.
#'
#' @field decennial_census The census year the information represents: 2000,
#'                         2010, and 2020.
#' @field tract The full tract-level GEOID, including the state and county
#'              Federal Information Processing Standards (FIPS) codes.
#' @field tract_name The tract-specific information and name for that decennial
#'                   year and geocoordinates.
#' @field state_fips The state FIPS code for that decennial year and geocoordinates.
#' @field county_fips The county FIPS code for that decennial year and
#'                    geocoordinates.

# # Commit results.
# write.csv(step_5, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 05_Append Decennial Info Associated with an Area_05.27.2025.csv")

# Load in the pre-produced test results for evaluation.
step_5 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 05_Append Decennial Info Associated with an Area_05.27.2025.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()


## --------------------
## SUBSECTION A2: Assess the Algorithms Performance

# Only 1% of the Step 4 data was processed through this algorithm in
# preparation for the Summer 2025 Symposium.
round(nrow(step_5)/nrow(step_4)*100, digits = 2)

# All entries were associated with decennial information for each decennial year.
step_5 %>%
  group_by(decennial_census) %>%
  summarise(n_rows = dplyr::n(), .groups = "drop")

# All processed entries returned a non-NA result.
sapply(step_5[, c("tract", "tract_name", "state_fips", "county_fips")], function(x) any(is.na(x)))




## ----------------------------------------------------------------
## PART B: US Census Bureau Geocoder Algorithm (ARCHIVED)

# The Geocoder algorithm is restricted to the vintages available at the time
# of development, which for this pipeline were the 2010 and 2020 decennial
# years. This script is kept for reference only.

addresses <- step_4[, "compiled_address"]   # Addresses to geocode
vintages <- c("Census2010_Current", "Census2020_Current")   # Geocoder vintages to query

all_results <- data.frame()   # Initialize the empty lists
pb = txtProgressBar(min = 0, max = length(addresses), style = 3)   # Initialize progress bar

for(i in 1:length(vintages)) {

  census_results <- data.frame()
  for(j in 1:length(addresses)) {
    # Query Census Geocoder
    result <- get_census_tract_geocoder(addresses[j], vintage = vintages[i])

    if(!is.null(result)) {
      result <- result %>%
        rename(compiled_address := Address,
               !!paste0(str_split_1(result$Census, "_")[1], "_Tract") := Census_Tract,
               !!paste0(str_split_1(result$Census, "_")[1], "_County") := County,
               !!paste0(str_split_1(result$Census, "_")[1], "_State") := State) %>%
        # Drop vintage label after using it in column names
        select(-Census)

    } else {
      # If geocoding succeeded, reshape/rename output
      result <- data.frame(addresses[j], NA, NA, NA) %>%
        `colnames<-`(c("compiled_address",
                       paste0(str_split_1(vintages[i], "_")[1], "_Tract"),
                       paste0(str_split_1(vintages[i], "_")[1], "_County"),
                       paste0(str_split_1(vintages[i], "_")[1], "_State")))

    }

    # Append this address result
    census_results <- bind_rows(census_results, result)
  }

  # Print the for loop's progress.
  setTxtProgressBar(pb, j)

  # First vintage initializes all_results; later vintages add new columns by address
  if( nrow(all_results) == 0 ) {
    all_results <- census_results
  } else {
    all_results <- left_join(all_results, census_results, by = "compiled_address")
  }
}

# Example usage
address_str <- "1600 Amphitheatre Parkway, Suite 100, Mountain View, CA, 94043-1234"

# Get census tract information for the address for different vintages
vintage_2020_result <- get_census_tract_geocoder(address_str, vintage = "Census2020_Current")
vintage_2010_result <- get_census_tract_geocoder(address_str, vintage = "Census2010_Current")



