## ----------------------------------------------------------------
## 
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: April 29th, 2026
## 
## Description: During review of the raw data, it was identified that multiple
##              records are associated with the same address, attributed to
##              minor typographical inconsistencies in address entry rather
##              than representing genuinely distinct locations.
##
##              To address this, a string-clustering algorithm is applied to
##              group similar address strings together. Records within each
##              cluster are then consolidated into a single entry, such that
##              each row in the resulting dataset represents a unique address.
##
##              Following this process, the performance of the algorithm is
##              evaluated by examining the maximum difference in geolocation
##              coordinates reported across all collapsed entries. Special
##              cases are closely inspected and relevant resolutions proposed.
##              Prior to saving the final result, a comprehensive review was
##              conducted to confirm that all expected business entries were
##              retained and assess the algorithms performance.
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
##
##    - PART A: Enhancing Function Performance and Efficiency
##        * SUBSECTION A1: Optimizing Data Subsetting
##        * SUBSECTION A2: Optimizing Data Combination
##
##    - PART B: Standardize and Merge Similar Addresses
##        * SUBSECTION B1: Verify No Duplicates Got Added
##        * SUBSECTION B2: Explore the Nature of Duplications
##        * Subsection B3: Similar Addresses: Geolocation Discrepancies
## 
##    - PART C: Resolving Failed Geolocation Tests via Expansion and Overrides
##        * SUBSECTION C1: Combine All Results
## 
##    - PART D: Cleaning for Saving the Result
##        * SUBSECTION D1: Add Missing Columns
##        * SUBSECTION D2: Merge Duplicate and Singular Record Datasets
##        * SUBSECTION D3: Remove Businesses with No Recorded Physical Address
##        * SUBSECTION D4: Organize and Save the Results
## 
##    - PART E: Assess Overall Performance

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("ggplot2")          # Graphics and visualization
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")
  library("jsonlite")
  library("future.apply")     # Parallel processing
  library("progress")         # Progress bars
  library("profvis")          # Profiling visualization
  library("microbenchmark")   # Micro-benchmarking for performance
  library("data.table")       # High-performance data manipulation
})

suppressPackageStartupMessages({
  library("sf")
  library("tigris")
  library("tidycensus")
  library("tidyr")
  library("ggforce")
  library("pbapply")
  library("lubridate")
  library("usmap")
  library("dbscan")
  library("xml2")
  library("rprojroot")
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 2.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Load in the pre-produced test results for evaluation.
step_1 <- read_csv("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 01_Completed Result_04.29.2029.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.90/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)




## ----------------------------------------------------------------
## STEP 2: VALIDATE ADDRESSES

# In the second step of cleaning and validating the church_wide dataset, we
# get the USPS preferred address. The previous step did not have a mechanism
# to check for address correctness when different variations were provided.
# As noted, in the subsection above, it also did not catch address that
# are similar but their longitude and latitude listing were dissimilar,
# suggesting it is a different address.
# 
# Using the API's where erroneous duplicated addresses were compressed,
# and their longitutde and latitude are similar, we now query the USPS
# Address 3.0 API to get the preferred address. 
#   - If a preferred address is matched, then all address entries get changed. 
#   - If no preferred address is found, the "compiled_address" is changed
#     to "No address match found".
#
# USPS API Documentation: https://developers.usps.com/addressesv3
#    USPS GitHub Example: https://github.com/USPS/api-examples
# 
# NOTE: It is possible that some addresses list the street name in the address
#       line 2, but we do not worry about this designation. Either one or the
#       other is suitable. This can be changed in later iterations if need be.


# To query the USPS database, you need to setup your client key and secret
# to generate and OAuth Token that will access their database. These credentials
# CANNOT be shared, and must be therefore kept untracked by git, local (not
# published on GitHub), and each user must have their own credentials.
# 
# Directions to setting up your credentials and environment are listed below.
# 
# 1. Follow the "Getting Started" directions in the USPS GitHub page to register
#    for a USPS developer account: https://developers.usps.com/
# 
# 2. Under the "App" webpage of the developer webportal, create a new app to 
#    generate your personal API credentials. Make sure to add the project 
#    description. This will give you two credentials: "Consumer Key and" 
#    "Consumer Secret".
#    
#    As stated above, these should NOT be hard coded into the script as they
#    should be kept private to each user.
# 
# 3. In the root directory for the project, create a ".Renviron" if you do not
#    already have one. Add your credentials with no extra spaces or hidden 
#    characters in parenthesis, as shown below.
# 
#       USPS_CONSUMER_KEY="your_consumer_key"
#       USPS_CONSUMER_SECRET="your_consumer_secret"
# 
#    These will then be loaded using the code below with Sys.getenv().
# 
# 4. Ensure that the ".Renviron" file is included in your ".gitignore", and
#    that it does not show up for git tracking.
#     
# NOTE: If you are having trouble loading your environment variables, then
#       run the following code from rprojroot to explicitly set the location:
# 
#       rprojroot::find_rstudio_root_file()
#       readRenviron(rprojroot::find_rstudio_root_file(".Renviron"))

consumer_key <- Sys.getenv("USPS_CONSUMER_KEY")
consumer_secret <- Sys.getenv("USPS_CONSUMER_SECRET")


# While verifying the addresses, we want to add the address line 2, zip code
# 4-digit extension, and a boolean to verify that the address has been verified.
step_1 <- step_1 %>%
  mutate(address_line_2 = "", address_verified = NA, zipcode_ext = "") %>%
    relocate(address_line_2, .after = address_line_1) %>%
    relocate(zipcode_ext, .after = zipcode) %>%
    relocate(address_verified, .after = compiled_address)


# The following code will calculate this, but it takes a while to run. The
# results have also been saved so you can skip the function and load in those
# pre-produced results.

# Add a progress bar to show where the function is in the for loop.
pb = txtProgressBar(min = 0, max = nrow(step_1[1:10, ]), style = 3)

for (i in 1:nrow(step_1[1:10, ])) {
  # 1) Pull the i-th row into variables used by validate_usps_address()
  address1 <- step_1$address_line_1[i]
  address2 <- ""
  city     <- step_1$city[i]
  state    <- step_1$state[i]
  
  # Split ZIP into ZIP5 and ZIP+4 (blank if missing)
  zip5 <- str_extract(step_1$zipcode[i], "^[0-9]+") %>% ifelse(is.na(.) || . == "", "", .)
  zip4 <- str_extract(step_1$zipcode[i], "(?<=-)[0-9]+") %>% ifelse(is.na(.) || . == "", "", .)
  
  # 2) Attempt #1: Validate using the original inputs
  suppressWarnings({
    usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "")
  })
  
  # 3) Attempt #2: If no match, assess/correct city (try ZIP5 orientations), then retry
  if (all(dim(usps_validated) == 0)) {
    
    zip5_raw <- zip5 %>% ifelse(is.na(.) || . == "", "", .)
    zip5_raw <- ifelse(nzchar(zip5_raw), str_pad(zip5_raw, width = 5, side = "left", pad = "0"), "")
    
    # Leading/trailing zeros were stripped prior to receiving the raw data.
    # Some ZIP-to-city sources treat those edge zeros differently, so we test 
    # multiple orientations by "sliding" the same count of edge zeros between 
    # the front and back of the ZIP (still 5 digits).
    zip5_candidates <- make_zip5_candidates(zip5_raw)
    
    # Try candidates until one returns a city (then stop); otherwise do nothing
    for (z in zip5_candidates) {
      query_result <- get_city_info(z, zip_city_lookup)
      
      if (!str_detect(query_result, "No Matches")) {
        city <- query_result
        zip5 <- z
        
        suppressWarnings({
          usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = address2, city, state, zip5, zip4 = zip4
          )
        })
        
        # Stop after the first candidate that yields a city
        break
      }
    }
  }
  
  # 4) Attempt #3: if still no match, swap address lines, then retry
  if (all(dim(usps_validated) == 0)) {
    # Move address_line_1 into address2 and leave address1 blank
    address1 <- ""
    address2 <- step_1$address_line_1[i]
    
    suppressWarnings({
      usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "")
    })
  }
  
  # 5) Save results back into step_1 (single write block)
  if (all(dim(usps_validated) == 0)) {
    
    # Nothing matched after all attempts
    step_1$compiled_address[i] <- "No address match found"
    step_1$address_verified[i] <- FALSE
    
  } else {
    
    # Overwrite fields with USPS-preferred formatting
    step_1$address_line_1[i] <- usps_validated[, "address_line_1"]
    step_1$address_line_2[i] <- usps_validated[, "address_line_2"]
    step_1$city[i]           <- usps_validated[, "city"]
    step_1$state[i]          <- usps_validated[, "state"]
    step_1$zipcode[i]        <- usps_validated[, "zipcode"]
    step_1$zipcode_ext[i]    <- usps_validated[, "zipcode_ext"]
    
    # Mark as verified
    step_1$address_verified[i] <- TRUE
    
    # Build a single printable address string: "line1, line2, city, state ZIP-EXT"
    step_1$compiled_address[i] <- str_c(
      str_flatten(na.omit(unlist(usps_validated[1:4])), collapse = ", "),
      str_flatten(na.omit(unlist(usps_validated[5:6])), collapse = "-"),
      sep = " "
    ) %>%
      str_trim() %>%
      str_remove("-$") %>%
      str_remove("\\s*$")
  }
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}


# Below is a series of checks and logs that were used to progressively save
# the Step 2 results. The script takes over a week to run on a normal device
# with four computing cores.
# 
# # Inspect the last-processed ABI and which index the process got stopped at.
# same[same$address_verified %in% NA, ] %>% head()
# which(same$address_verified %in% NA) %>% head()
# 
# same[888674:888676, ]
# 
# # Saving Log:
# # Rows 1:20902, saved 6:30pm 5/22
# # Rows 1:199032, saved 10:51am 5/23
# # Rows 1:207465, saved 1:44pm 5/23
# # Rows 1:236604, saved 10:49pm 5/23
# # Rows 1:353483, saved 7:23pm 5/24
# # Rows 1:444497, saved 12:50am 5/26
# # Rows 1:494863, saved 7:50am 5/26
# # Rows 1:515924, saved 11:08am 5/26
# # Rows 1:582922, saved 11:21pm 5/26
# # Rows 1:674016, saved 2:02pm 5/27
# # Rows 1:690472, saved 4:51pm 5/27
# # Rows 1:694696, saved 8:01pm 5/28
# # Rows 1:735471, saved 9:42pm 5/28
# # Rows 1:756901, saved 9:22am 5/29
# # Rows 1:817927, saved 7:53pm 5/29
# # The process completed June 1st, but for some reason the results did not
# # save properly. The needed to be rerun starting from index = 817928.
# # Rows 1:817927, saved 10:00am 6/5
# # Rows 1:852775, saved 11:00am 6/5
# # Completed 7/1
# step_2 <- same
# 
# # Confirm adding to the previous set and no NA's.
# all(unique(step_2$abi) %in% unique(same$abi))
# which(step_2$address_verified %in% NA)
# 
# # Commit results.
# write_csv(step_2, "data/Raw KEEP LOCAL/Cleaning Wide Format Steps/Step 02_Church Wide_Convert to Preferred USPS Address_COMPLETED_06.07.2025.csv.gz")
#
# # If the environment crashes, replace the progress saved in step_2 to the same
# # variable.
# same <- bind_rows(step_2, same[same$abi %!in% step_2$abi, ])
# 
# 
# same[same$abi %in% step_2$abi, "address_verified"] %>% table()
# sum(is.na(same[same$abi %in% step_2$abi, "address_verified"]))
# 
# same[same$abi %in% step_2$abi & is.na(same$address_verified), ]


# Load in the pre-produced test results for evaluation.
step_2 <- read_csv("data/Raw KEEP LOCAL/Cleaning Wide Format Steps/Step 02_Church Wide_Convert to Preferred USPS Address_COMPLETED_06.07.2025.csv.gz") %>% 
  as.data.frame()


step_2$address_verified %>% table()

step_2[str_detect(step_2$address_line_1, "PO BOX"), "address_verified"] %>% table()


step_2[step_2$address_verified %in% FALSE, ]
step_2[step_2$abi %in% 101275956, ]


# Less than 30% of the data had a verified address from this database. Most
# entries with a PO BOX failed, but surprisingly 20% were verified as well.
# One alternative source is going to be explored to try and bolster these
# numbers, the Google Maps API. This source is also expected to verify the
# geolocation of the address, which merges with the next step.



## --------------------
## Inspect the 1.6% duplicates added in Step 1

# In Step 1, similar addresses were sometimes counted multiple times because
# the address_line_1 variable was similar, but there were differences with
# the other aspects of a address that caused the function to identify them
# as different. These were then collapsed multiple times only based on
# address_line_1 similarity, thus introducing duplicated response records for 
# relevant spans of time.

dup_verify <- step_2[step_2$abi %in% dup_added, ] %>% 
  # Keep the row associated with this subset.
  mutate(row_designation = rownames(.))

# Some addresses were over counted once and as many as three times (sums = 2 to 4).
test_no_dup %>%
  select(starts_with("20")) %>%
  as.data.frame() %>%
  as.matrix() %>% as.vector() %>% unique() %>% sort()


# 87% percent do not have a verifiable address in the USPS database.
dup_verify[dup_verify$compiled_address %in% "No address match found", "abi"] %>% 
  unique() %>% (\(x) { round(length(x)/length(dup_added)*100, digits = 2) })


# Most of the entries do not have verified addresses for each duplicate.
# Therefore we will need to be more nuanced about selecting the duplicates. 
# Below is a for loop that selects down to the most informative address 
# available for a given ABI within a nexus of longitude/latitude coordinates.

# Add the missing column where the keep/drop binary will be stored.
dup_verify <- dup_verify %>% mutate(keep_binary = NA)

# Define the search space by all unique ABI's associated with some duplication.
search_space <- unique(dup_verify$abi)

infoDropped <- c()
confirm_one_keep <- c()
for(i in 1:length(search_space)) {
  # Subset to show only the entries associated with one duplicated ABI.
  subset <- dup_verify[dup_verify$abi %in% search_space[i], ]
  
  # Identify the rows where duplicated responses were introduced in Step 1.
  col_sums <- subset %>% summarise(across(starts_with("20"), sum)) %>% as.data.frame()
  exceeding_cols <- colnames(col_sums)[col_sums[1, ] > 1]

  # Subset those rows to proceed with.
  dup_introduced <- subset %>% filter(if_any(all_of(exceeding_cols), ~ . == 1))
  
  # Those not subset as duplicated, mark to keep.
  subset[subset$row_designation %!in% dup_introduced$row_designation, "keep_binary"] <- "Keep"
  
  # We are going to assume that addresses that are meant to reflect the same
  # physical location of a church have similar longitudes and latitudes. If
  # multiple addresses were over counted for a given ABI, we need to handle them
  # separately.
  #
  # Use K-means density clustering to identify addresses with similar longitude 
  # and latitude. Add a condition that if there are missing longitude and latitudes,
  # then simply say everything is in the same cluster.
  if( sapply(dup_introduced[, c("longitude", "latitude")], is.na) %>% as.vector() %>% any() ) {
    dup_introduced$cluster <- 1
    message(sprintf("There are NA's in the coordinates. Row ID's: %s.", str_flatten(dup_introduced$row_designation, collapse = ", ")))
    problemGeo <- TRUE
  } else {
    dup_introduced$cluster <- dbscan::dbscan(dup_introduced[, c("longitude", "latitude")], eps = 1, minPts = 2)$cluster
    problemGeo <- FALSE
  }
  
  
  one_kept <- c()
  for( j in 1:length(unique(dup_introduced$cluster)) ) {
    # Subset by the selected cluster.
    one_location <- dup_introduced[dup_introduced$cluster %in% unique(dup_introduced$cluster)[j], ]
    
    # Any addresses that had no match in the USPS database get dropped.
    if( any(one_location$compiled_address %in% "No address match found") ) {
      one_location[one_location$compiled_address %in% "No address match found", "keep_binary"] <- "Drop"
      
      # If in the rare cases there were no other candidate addresses to use,
      # then use the most informative unverified address.
      if( all(one_location$compiled_address %in% "No address match found") ) {
        one_location[one_location$compiled_address %in% "No address match found", "keep_binary"] <- NA
      }
    }
    
    # Rows to keep evaluating after the first prune.
    keep_eval <- which(one_location$keep_binary %!in% "Drop")
    
    
    ## --------------------
    ## Save the duplicate with the most informative or most accurate address.
    
    # We want to select addresses by the one that provides the most information.
    # Technically, each one of these (that wasn't dropped earlier) is a valid
    # address according to the USPS database. We will therefore use the following
    # three conditions to arbitrarily select one address to collapse to:
    #   1. Has a Address Line 2
    #   2. Has the longest Address Line 1
    #   3. If multiple entries have the same Address Line 1, select the one 
    #      associated with the shortest city name that is not an acronym.
    
    selectors <- data.frame("rowIndex" = keep_eval,
                            "Line1" = str_length(one_location[keep_eval, ]$address_line_1), 
                            "Line2" = str_length(one_location[keep_eval, ]$address_line_2),
                            "city" = str_length(one_location[keep_eval, ]$city))
    
    
    # If an Address Line 2 is given, then drop all entries where there is no
    # Address Line 2 present.
    if( any(!is.na(selectors$Line2)) ) {
      one_location[keep_eval, ][is.na(selectors$Line2), "keep_binary"] <- "Drop"
      # Update the addresses to continue for evaluating.
      keep_eval <- which(one_location$keep_binary %!in% "Drop")
    }
    
    
    # We'd like to save the most informative Address Line 1, which is determined
    # to be one with numerics and is near the largest, or else is the largest
    # Address Line 1 option.
    if( any(str_detect(one_location[keep_eval, ]$address_line_1, "[0-9]")) ) {
      contains_num <- str_detect(one_location[keep_eval, ]$address_line_1, "[0-9]")
      is_also_max  <- str_detect(one_location[keep_eval, ]$address_line_1, "[0-9]") & 
        selectors[keep_eval, "Line1"] %in% max(selectors[keep_eval, "Line1"])
      
      # If the address contains a numeric and is also the max, then save that one.
      if( any(is_also_max) ) {
        one_location[keep_eval[!is_also_max], "keep_binary"] <- "Drop"
        # Update the addresses to continue for evaluating.
        keep_eval <- which(one_location$keep_binary %!in% "Drop")
        
      # If the address contains a numeric is not also the max, then save the next
      # largest, otherwise save the largest.
      } else {
        selectors <- selectors %>%
          mutate(similarLength = dbscan::dbscan(selectors[, c("Line1"), drop = FALSE], eps = 2, minPts = 2)$cluster)
        
        # If if any of the entries with a numeric are in the range of the max,
        # then drop the non-numeric ones.
        if( any(selectors[selectors$Line1 %in% max(selectors$Line1), "similarLength"] %in% selectors[contains_num, "similarLength"]) ) {
          one_location[keep_eval, ][!contains_num, "keep_binary"] <- "Drop"
          # Update the addresses to continue for evaluating.
          keep_eval <- which(one_location$keep_binary %!in% "Drop")
          
        # If the numeric entries are not in the range of the max, then drop the
        # numeric ones, and only keep the longest addresses.
        } else {
          one_location[keep_eval, ][selectors$Line1 %!in% max(selectors$Line1), "keep_binary"] <- "Drop"
          # Update the addresses to continue for evaluating.
          keep_eval <- which(one_location$keep_binary %!in% "Drop")
        }
      }
      
    # If no duplicated have numerics, then simply save the longest.
    } else {
      one_location[keep_eval, ][selectors$Line1 %!in% max(selectors$Line1), "keep_binary"] <- "Drop"
      # Update the addresses to continue for evaluating.
      keep_eval <- which(one_location$keep_binary %!in% "Drop")
    }
    
    
    # If the number of rows is reduced to one, then save the row_designation
    # associated with this address.
    if( length(keep_eval) == 1 ) {
      one_location[keep_eval, "keep_binary"] <- "Keep"
      
    # If more than one entries meet the previous two conditions, choose the
    # one with the shortest city name that is not also an acronym.
    } else if( length(keep_eval) > 1 ) {
      # Test which cities are recognized as the preferred city name.
      any_city <- sapply(one_location[keep_eval, "city"], is_acronym_or_city)
      
      if( any(any_city %in% "City") ) {
        # Capture one of the valid cities (even if more than one comes up).
        one_location[ keep_eval[which(any_city %in% "City")[1]], "keep_binary"] <- "Keep"
        
      } else if( !any(any_city %in% "City") ) {
        # Search for any city 
        search_city <- us_cities[us_cities$state_id %in% one_location$state[1] & 
                                   us_cities$zips %in% one_location$zipcode[1], "city"]
        
        # If any candidate cities are pulled.
        if( length(search_city) >= 1 ) {
          # Find the city most similar to any given entry.
          any_match <- lapply(find_similar_addresses(c(one_location[keep_eval, "city"], search_city[[1]])), function(x) any(x %in% search_city[[1]])) %>% unlist()
          
        # Otherwise, randomly save the first entry in the keep_eval vector.
        } else {
          one_location[keep_eval[1], "keep_binary"] <- "Keep"
          message(sprintf("No city match found. Random one chosen. Row ID's: %s.", str_flatten(one_location$row_designation, collapse = ", ")))
          
        }
        
        # If only one candidate city, then save the entry associated with that.
        if( sum(any_match == TRUE) == 1 ) {
          # If none of the cities matched, then randomly keep one of the addresses.
          if( any_match %>% which() %>% keep_eval[.] %>% is.na() ) {
            keep_eval <- keep_eval[1]
            message(sprintf("No city match found. Random one chosen. Row ID's: %s.", str_flatten(one_location$row_designation, collapse = ", ")))
          } else {
            keep_eval <- any_match %>% which() %>% keep_eval[.]
          }
          one_location[keep_eval, "keep_binary"] <- "Keep"
          
          # Replace the city name with the match.
          one_location[keep_eval, "city"] <- search_city[[1]][1]
          
        # If more than one candidate city, then randomly choose the first one to keep.
        } else {
          any_match %>% which() %>% keep_eval[.] %>% .[1]
          one_location[keep_eval, "keep_binary"][1] <- "Keep"
          message(sprintf("More than one city match found. Random one chosen. Row ID's: %s.", str_flatten(one_location$row_designation, collapse = ", ")))
        }
      }
      
      
    # If no addresses are left for evaluating, then choose the nearest, non-PO BOX
    # address to replace all duplicates.
    } else if( length(keep_eval) < 1 ) {
      
      candidate_replacement <- which(!str_detect(subset$compiled_address, "PO BOX") & !subset$compiled_address %in% "No address match found")
      one_location <- bind_rows(one_location, subset[candidate_replacement, ])
      
      one_location$cluster <- dbscan::dbscan(one_location[, c("longitude", "latitude")], eps = 1, minPts = 2)$cluster
      
      # Store the indices for each type of entry: duplciated or address for
      # supplementing the missing valid address.
      index_dup  <- 1:(nrow(one_location) - length(candidate_replacement))
      index_cand <- (nrow(one_location) - length(candidate_replacement) + 1):nrow(one_location)
      
      if( length(candidate_replacement) == 1 && length(unique(one_location$cluster)) == 1 ) {
        # Compress all information into the keep row.
        one_location[index_cand, colnames(select(one_location, starts_with("20")))] <- one_location %>% 
          summarise(across(starts_with("20"), sum)) %>%
          mutate(across(everything(), ~ if_else(. != 0 & . != 1, 1, .)))
        
      } else if( length(candidate_replacement) > 1 && any(unique(one_location$cluster[index_dup]) %in% unique(one_location$cluster[index_cand])) ) {
        # Which in the index_cand to keep.
        replace_with <- index_cand[which(one_location[index_cand, "cluster"] %in% unique(one_location$cluster[index_dup]))]
        
        # Reconstruct the combined dataframe.
        one_location <- bind_rows(one_location[index_dup, ], subset[replace_with, ])
        
        # Compress all information into the keep row.
        one_location[replace_with, colnames(select(one_location, starts_with("20")))] <- one_location %>% 
          summarise(across(starts_with("20"), sum)) %>%
          mutate(across(everything(), ~ if_else(. != 0 & . != 1, 1, .)))
        
      # Hard stop if no address was chosen.
      } else {
        stop(sprintf("No other non-PO BOX candidate addresses in range! Row ID's: %s.", str_flatten(one_location$row_designation, collapse = ", ")))
      }
      
    }
    
    
    # Designate all remaining keep_binary entries as "Drop"
    if( any(one_location$keep_binary %in% "Keep") ) {
      one_location[one_location$keep_binary %!in% c("Keep", "Drop"), "keep_binary"] <- "Drop"
      
      
    # Hard stop if no address was chosen.
    } else {
      stop(sprintf("No candidate addresses were chosen to keep! Row ID's: %s.", str_flatten(one_location$row_designation, collapse = ", ")))
    }
    
    # Compress all information into the keep row.
    one_location[one_location$keep_binary %in% "Keep", colnames(select(one_location, starts_with("20")))] <- one_location %>% 
      summarise(across(starts_with("20"), sum)) %>%
      mutate(across(everything(), ~ if_else(. != 0 & . != 1, 1, .)))
    
    # Commit the changes to the one geo location (one_location) subset.
    subset[subset$row_designation %in% one_location$row_designation, ] <- suppressMessages(subset[subset$row_designation %in% one_location$row_designation, ] %>%
      (\(x) { bind_cols(x[, -39], one_location[, c(38:39)]) }) () %>%
      select(-row_designation...39) %>%
      rename(row_designation = row_designation...38)
    )
    
    
    ## --------------------
    ## Quality Checks
    
    # We want to confirm that no information is getting lost in our choice
    # of entry. If all the represented columns for keep_binary = "Keep" are
    # 0 or 1 only, then if the sums over the dropped and kept subsets are
    # consistent (sum should come to not 1).
    #
    # NOTE: If dropped_all_1 == FALSE, the the consistency test might fail.
    all_represented <- rbind(one_location[one_location$keep_binary %in% "Drop", ] %>% 
                               check_all_counts_0_or_1() %>% as.data.frame(),
                             one_location[one_location$keep_binary %in% "Keep", ] %>% 
                               check_all_counts_0_or_1() %>% as.data.frame()
    ) %>% `rownames<-`(c("Dropped", "Kept"))
    
    consistent <- all_represented %>%
      # Summarize the selected year columns (columns starting with "20") by 
      # summing them.
      summarise(across(starts_with("20"), sum)) %>%
      # Ensure subsequent operations are performed row-wise.
      rowwise() %>%
      # Add a new column `all_counts_not_1`; TRUE if all counts are not 1.
      mutate(all_counts_not_1 = all(across(starts_with("20"), ~ . %!in% c(1)))) %>%
      # Remove row-wise grouping to avoid unintentional side effects.
      ungroup() %>%
      as.data.frame()
    
    # Compile the metrics used to determine if any information might have been
    # dropped. Here, "Kept" only referrs to the duplicate kept, and "Dropped"
    # only referrs to the duplicate(s) dropped.
    infoDropped <- rbind(infoDropped, c(unique(one_location$abi), all_represented[, "all_counts_0_or_1"], consistent[, "all_counts_not_1"], problemGeo)) %>%
      `rownames<-`(NULL) %>%
      `colnames<-`(c("abi", "dropped_all_1", "kept_all_1", "consistent", "geoNA"))
    
    
    # Finally, we want to confirm that only one value was saved.
    one_kept <- c(one_kept, sum(one_location$keep_binary %in% "Keep") == 1)
    
  }
  
  # Commit the changes from subset to dup_verify to save results.
  dup_verify[dup_verify$row_designation %in% subset$row_designation, ] <- suppressMessages(dup_verify[dup_verify$row_designation %in% subset$row_designation, ] %>%
    (\(x) { bind_cols(x[, -39], subset[, c(38:39)]) }) () %>%
    select(-row_designation...39) %>%
    rename(row_designation = row_designation...38)
  )
  
  # Compile the results of the QC test.
  confirm_one_keep <- rbind(confirm_one_keep, one_kept)
}

##
##
## IS THERE A BETTER WAY TO HANDLE THE ADDRESSES WHERE SOME LON/LAT ARE NA?
## GOING TO NEED TO FIX THE CONSISTENT CHECK, CAN REMOVE KEPT_ALL_1.
## MAKE SURE ALL INFORMATION FOR DUPLICATES GETS FORCED INTO KEPT

infoDropped <- as.data.frame(infoDropped)
infoDropped[, -1] <- infoDropped %>%
  (\(x) { sapply(x[, -1], as.logical) }) ()


table("Consistent" = infoDropped$consistent, "Lon/Lat NA" = infoDropped$geoNA)
table("Consistent" = infoDropped$consistent, "Dropped All 0/1" = infoDropped$dropped_all_1)

step_2[step_2$abi %in% infoDropped[infoDropped$geoNA %in% TRUE, ][2, ], ]
dup_verify[dup_verify$abi %in% infoDropped[infoDropped$geoNA %in% TRUE, ][2, ], ]


## FINISH BY CHECKING THAT FOR EACH ABI ALL INFORMATION IS UNIQUE OVER THE DATES
## FOR LON/LAT NA KEEP THE LON/LAT IF ONE IS REPORTED


