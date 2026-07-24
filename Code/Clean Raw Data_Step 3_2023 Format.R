## ----------------------------------------------------------------
## Verify Geocoordinates via Address-Based Geocoding
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 24th, 2026
## 
## Description: Coordinate accuracy is critical for assignment to granular
##              census boundaries such as block groups and tracts. In the
##              previous data processing and cleaning step, addresses were
##              verified against the USPS API. These geocoordinates are used
##              in Step 5 to assign each entry to its respective boundary
##              via point-in-polygon spatial assignment.
##
##              Steps 1 and 2 identified cases where entries sharing identical
##              address_line_1 values had divergent coordinates, with
##              discrepancies of up to 3 degrees, potentially misassigning
##              census boundaries by as much as two states.
##
##              With addresses verified to the extent possible, entries are
##              now processed for address-based geocoding to produce verified
##              longitude and latitude coordinates using the US Census Bureau
##              Geocoder API.
## 
##              US Census Bureau Geocoder API: https://geocoding.geo.census.gov/geocoder/
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
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Validating the Geocoordinates Using the US Census Bureau Geocoder API
##        * SUBSECTION A1: Algorithm
##        * SUBSECTION A2: Save Results
## 
##    - PART B: Assess the Algorithms Performance
##    - PART C: Subset for Next Steps

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
  library("httr")             # HTTP requests (GET/POST) for web APIs
  library("jsonlite")         # JSON parsing and generation (to/from R objects)
  library("future.apply")     # Parallel processing
  library("progress")         # Progress bars
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 3_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Read in previously generated results.
step_2 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 02_Church Wide_Convert to Preferred USPS Address_COMPLETED_06.07.2025", 
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

# Running this algorithm on the full dataset was not feasible within the
# time constraints of the Summer 2025 symposium prototype unveiling.
# Consequently, a random sample of approximately one-third of the data
# was used to produce results for visualization across the contiguous US.
#
# As the sample was drawn randomly, refer to the generated results to
# determine which entries were retained for downstream processing.




## ----------------------------------------------------------------
## PART A: Validating the Geocoordinates Using the US Census Bureau Geocoder API

## --------------------
## SUBSECTION A1: Algorithm

# Prepare the dataset for QC output:
step_3 <- step_2 %>%
  mutate(
    #   - Store latitude/longitude returned by the Census Geocoder.
    verifiedLat = NA, verifiedLon = NA, 
    #   - Flag whether the original and verified coordinates differ by less than 1 degree.
    similarLat = NA, similarLon = NA, 
    #   - Capture the geocoder messages describing matching or API interactions.
    verifiedGeo = NA)


search_space <- unique(step_3$abi)   # Define the search space
build <- list()   # Initialize the empty lists
pb = txtProgressBar(min = 0, max = length(search_space), style = 3)   # Initialize progress bar

for (i in 1:length(search_space)) {
  subset <- step_3 %>% filter(abi %in% search_space[i])
  
  build_address <- list()
  for (j in 1:nrow(subset)) {
    # Construct address string for geocoding, assuming there are NA's for entries.
    address_to_check <- str_flatten(purrr::map_chr(subset[j, 2:5], ~ifelse(is.na(.), "", as.character(.))), collapse = ", ") %>%
      # If any components are missing, remove them.
      str_replace(", , ", ", ") %>%
      # Format the zip code 5-digit and 4-digit components.
      (\(x) { str_c(x, str_flatten( purrr::map_chr(subset[j, 6:7], ~ ifelse(is.na(.), "", as.character(.))) , collapse = "-"), sep = " ") }) () %>%
      # If no 4-digit zip code is included, then remove the dash.
      str_replace("-$", "")
    
    # Initialize retry mechanism.
    max_retries <- 5
    retry_count <- 0
    success <- FALSE
    
    while (retry_count < max_retries && !success) {
      # Capture warnings and results from validation.
      check_result <- capture_warnings(quote(validate_geocoordinates_geoCoder(address_to_check)))
      
      # Only retry if the error is related to querying the API.
      if (length(check_result$warnings) > 0) {
        filtered_warnings <- check_result$warnings[grepl("Failed to fetch data from Census Geocoder API. Status code:", check_result$warnings)]
        if (length(filtered_warnings) > 0) {
          retry_count <- retry_count + 1
          Sys.sleep(1)  # Pause briefly before retrying.
          
        } else {
          success <- TRUE
        }
      } else {
        success <- TRUE
      }
    }
    
    # If no warning was given then proceed as normal.
    if (length(check_result$warnings) == 0) {
      all_results <- check_result$result
      
      # If more than one match is drawn, arbitrarily choose the first one listed.
      if (nrow(all_results) > 1) {
        #message(str_c("ABI: ", unique(subset$abi), " has more than one match for: ", address_to_check, ". Keep first one."))
        all_results <- all_results[1, ]
      }
      
      # Add the verified Lon/Lat coordinates.
      subset[j, "verifiedLat"] <- all_results$Latitude
      subset[j, "verifiedLon"] <- all_results$Longitude
      
      # Confirm if the Lon/Lat coordinates are similar (within 1 point).
      subset[j, "similarLat"] <- (subset[j, "latitude"] - all_results$Latitude) < 1
      subset[j, "similarLon"] <- (subset[j, "longitude"] - all_results$Longitude) < 1
      
      # Add note about the verification results.
      if (is.numeric(all_results$Latitude) && is.numeric(all_results$Longitude)) {
        subset[j, "verifiedGeo"] <- TRUE
      } else if (is.numeric(all_results$Latitude) && !is.numeric(all_results$Longitude)) {
        subset[j, "verifiedGeo"] <- str_c("Latitude verified only.")
      } else if (!is.numeric(all_results$Latitude) && is.numeric(all_results$Longitude)) {
        subset[j, "verifiedGeo"] <- str_c("Longitude verified only.")
      } else {
        subset[j, "verifiedGeo"] <- FALSE
      }
    
    # If a warning was given. Then capture some of its details for closer
    # inspection later.
    } else {
      # Parse the outcomes from the check_result variable.
      all_results <- check_result$result
      all_warnings <- do.call(rbind, check_result$warnings)
      
      # Change string if result was NULL or not NULL.
      if (is.null(all_results)) {
        subset[j, "verifiedGeo"] <- str_c("Result is NULL. 1 of ", nrow(all_warnings), " warnings: ", all_warnings[1, ])
      } else {
        subset[j, "verifiedGeo"] <- str_c("Result is NOT NULL. 1 of ", nrow(all_warnings), " warnings: ", all_warnings[1, ])
      }
    }
    
    build_address[[j]] <- subset[j, ]
  }
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
  
  build[[i]] <- do.call(rbind, build_address)
}

# Combine all data tables in the list into one data table.
step_3_2 <- do.call(rbind, build)


## --------------------
## SUBSECTION A2: Save Results

# The warnings are very long. Re-code these so that they are easier to table.
unique(step_3_2$verifiedGeo)

step_3_2 <- step_3_2 %>%
  mutate(
    verifiedGeo = dplyr::case_when(
      str_detect(verifiedGeo, fixed("Address not found or no matches")) ~ "No match",
      str_detect(verifiedGeo, fixed("Failed to fetch data from Census Geocoder API. Status code: 400")) ~ "API failed",
      TRUE ~ verifiedGeo
    )
  )

# Apply data formatting and ordering.
step_3 <- step_3_2 %>%
  # Sort the rows by descending ABI.
  arrange(abi) %>%
  # Search the dates columns for which year that entry first has a 1.
  mutate(First_One_Year = pmap_chr(select(., -colnames(step_2)[c(2:3, 5:7, 9, 11:14)]), find_first_one)) %>%
  # Rename the newly added column entries so the "X" added is removed.
  rename_with(~ sub("^X", "", .), starts_with("X")) %>%
  # Sort the rows so that the oldest address comes before more recent addresses.
  group_by(abi) %>%
  arrange(First_One_Year, .by_group = TRUE) %>%
  ungroup() %>%
  # Remove the column used for organizing.
  select(-First_One_Year) %>%
  as.data.frame()


#' @description 
#' Codebook for the output fields produced by the evaluation.
#'
#' @field verifiedLat The suggested latitude returned by matching the address.
#'                    If multiple matches are returned by the geocoder, the first
#'                    match is used.
#'                    
#' @field verifiedLon The suggested longitude returned by matching the address.
#'                    If multiple matches are returned by the geocoder, the first
#'                    match is used.
#'                    
#' @field similarLat  Boolean. TRUE if the absolute difference between the given
#'                    latitude and verified latitude is less than 1 degree;
#'                    otherwise FALSE.
#'                    
#' @field similarLon  Boolean. TRUE if the absolute difference between the given
#'                    longitude and verified longitude is less than 1 degree;
#'                    otherwise FALSE.
#'                    
#' @field verifiedGeo Stores the outcome of the geocoding attempt. TRUE indicates
#'                    both coordinates were returned successfully; otherwise FALSE
#'                    or a short message describing the issue (e.g., no match found,
#'                    API request failed, or other warnings raised during geocoding).

# # Commit results.
# write_csv(step_3, "./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 03_Church Wide_Verified Geolocation_06.16.2025.csv.gz")


# Load in the pre-produced test results for evaluation.
step_3 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 03_Church Wide_Verified Geolocation_06.16.2025.csv.gz") %>% as.data.frame()




## ----------------------------------------------------------------
## PART B: Assess the Algorithms Performance

# The majority of addresses (~88%) and geocoordinates (~82%) were successfully
# verified, though a substantial proportion remained unverifiable via this API.
round(prop.table(table(step_3$address_verified))*100, digits = 2)
round(prop.table(table(step_3$verifiedGeo))*100, digits = 2)

# Addresses matched by the geocoder were predominantly verified, while
# unverified addresses showed a lower match rate. API failures, potentially
# attributable to timeouts, were more common among unverified addresses,
# though the difference may fall within expected error.
round(prop.table(
  table("Geocoordinates" = step_3$verifiedGeo,
        "Address" = step_3$address_verified,
        useNA = "ifany"),
  margin = 1
  ) * 100, 
 2
)


# PO Boxes had low verification rates in the preceding step and may also
# consequentially have higher geocoder failure rates.

# Annotate presence of all PO Boxes.
step_3 <- step_3 %>%
  mutate(
    is_po_box = str_detect(
      coalesce(address_line_1, ""),
      regex("\\bP\\s*\\.?\\s*O\\s*\\.?\\s*Box\\b", ignore_case = TRUE)
    )
  )

# Of the entries that were matched, none of them were a PO Box. Interestingly,
# hardly any of these outcomes failed to interact with the API, entries that
# failed to verify came from a failuter to match with the geocoder database.
round(prop.table(
  table("Geocoordinates" = step_3$verifiedGeo,
        "PO Box" = step_3$is_po_box,
        useNA = "ifany"),
  margin = 1
  ) * 100, 
 2
)




## ----------------------------------------------------------------
## PART C: Subset for Next Steps

# Only data with a verified geocoordinates will be used for the prototype.
# Stringency for this will be reviewed with the team and more closely assessed 
# for subsequent iterations of the dashboard.

# Identify ABIs where all entries passed geocoordinate verification.
abi_all_pass <- step_3 %>%
  group_by(abi) %>%
  summarize(all(verifiedGeo %in% "TRUE")) %>%
  ungroup() %>%
  as.data.frame()

# As noted earlier, ~82% of entries were verified, corresponding to ~80% of ABI.
round(table(abi_all_pass$`all(verifiedGeo %in% "TRUE")`)/nrow(abi_all_pass)*100, digits = 2)

# Subset to ABI where all entries passed geocoordinate verification.
step_3 <- step_3 %>%
  filter(abi %in% abi_all_pass[abi_all_pass$`all(verifiedGeo %in% "TRUE")` == TRUE, "abi"]) %>%
  # Replace original coordinates with verified geocoordinates.
  rename(latitude_remove = latitude, longitude_remove = longitude) %>% 
  rename(latitude = verifiedLat, longitude = verifiedLon) %>%
  # Retain only required columns and reorder for consistency.
  relocate(verifiedGeo, .after = longitude)


