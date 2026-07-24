## ----------------------------------------------------------------
## Validate the Address and Finish Resolving Duplications
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 22nd, 2026
## 
## Description: In the previous data processing and cleaning step, erroneous
##              duplicate addresses were consolidated into single records,
##              reducing the dataset by approximately half. During that step
##              and the initial review, it was observed that some individual
##              addresses may not be valid. Additionally, the algorithm
##              randomly selected one address from a set of similar entries
##              to carry forward. Verifying these addresses is considered
##              best practice and is particularly important for the subsequent
##              step, in which geolocation will be validated using the
##              listed addresses.
## 
##              USPS API Documentation: https://developers.usps.com/addressesv3
##              USPS GitHub Example: https://github.com/USPS/api-examples
## 
##              Some notable special cases:
## 
##              1. Some consolidated records failed the geolocation similarity
##                 quality check (QC). Among these, certain records were
##                 determined to represent the same address and their QC
##                 results were manually overridden. Others were reverted to
##                 their original raw format pending further address
##                 validation. Records confirmed to share the same valid
##                 address will be consolidated, while those that cannot be
##                 validated or do not resolve to the same address will
##                 remain separate.
## 
##              2. Some addresses were assigned to different similarity
##                 clusters or joined incorrectly with unaccounted for metadata,
##                 metadata variation, introducing duplicate open/closed records 
##                 that were not present prior to running the algorithm. In the
##                 later cases, the same address_line_1 value was shared across
##                 records, but the city or zip code differed. Geolocation 
##                 values also differed despite the identical address_line_1. 
##                 Following address validation, these records will be 
##                 consolidated into a single entry using the valid address, 
##                 if matched.
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
## USPS API Keys:
## To query the USPS database, a client key and secret must be configured to
## generate an OAuth token for database access. These credentials CANNOT be
## shared and must remain private to each user. They should be kept untracked
## by Git and stored locally, and must never be published to GitHub.
## 
## Follow the steps below to set up your credentials and environment.
## 
## 1. Register for a USPS Developer account by following the "Getting Started"
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
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
##
##    - PART A: Validating the Address Using the USPS API
##        * SUBSECTION A1: Prepare Subset for HPC
##        * SUBSECTION A2: Compile the Results
## 
##    - PART B: Complete Handling of Special Cases
##        * SUBSECTION B1: ABI-Specific Duplication Confirmation
##        * SUBSECTION B2: Consolidate Duplications and Remove Difficult Cases
##        * SUBSECTION B3: Consolidate Addresses with Different City
## 
##    - PART C: Organize and Save the Results
## 
##    - PART D: Assess the Algorithms Performance

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operationsa
  library("ggplot2")          # Graphics and visualization
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests (GET/POST) for web APIs
  library("jsonlite")         # JSON parsing and generation (to/from R objects)
  library("future.apply")     # Parallel processing
  library("stringdist")       # Measuring string distances
  library("progress")         # Progress bars
  library("profvis")          # Profiling visualization
  library("microbenchmark")   # Micro-benchmarking for performance
  library("data.table")       # High-performance data manipulation
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 1_2023 Format.R")
source("./Code/Support Functions/For Step 2_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Load the raw dataset in wide format.
church_wide <- read_csv("./Data/Raw/KEEP LOCAL/church_wide_form_071723.csv") %>% as.data.frame()

# Load in the pre-produced test results for evaluation.
step_1 <- read_csv("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2023 Format/Step 01_Completed Result_04.29.2026.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

# While verifying the addresses, we want to add the address line 2, zip code
# 4-digit extension, and a boolean to verify that the address has been verified.
step_1 <- step_1 %>%
  mutate(address_line_2 = "", address_verified = NA, zipcode_ext = "") %>%
  relocate(address_line_2, .after = address_line_1) %>%
  relocate(zipcode_ext, .after = zipcode) %>%
  relocate(address_verified, .after = compiled_address) %>%
  `rownames<-`(NULL)

step_1 <- rownames_to_column(step_1, var = "rowname")




## ----------------------------------------------------------------
## PART A: Validating the Address Using the USPS API

# This process has been designed to work both locally and on a High Performance
# Computer (HPC). Two HPC methods are outlined: a live session and a batch
# array. The validation function is not included in this script. Instead, this
# section generates the data subset intended for use in the HPC and compiles
# results assuming two possible data structures.
#
# The originally provided data lacked certain features and formatting required
# to ensure a straightforward validation process. To improve the likelihood of
# successful validation, addresses are taken through a series of modifications
# if the initial query fails:
#
#     1. If the address has leading or trailing zeros, retry the query using
#        all alternative plausible arrangements of the missing zeros until
#        a successful result is obtained.
#
#     2. If changing the zeros is unsuccessful, use address_line_1 as the 
#        second address line instead.
#
# If all attempts fail, the result is saved as "No address match found".
# 
# The validation algorithm was timed locally, where approximately 875 entries
# were processed per 5 minutes (~42,000 in four hours). Based on this, the data
# was partitioned into 42,000-entry indices (listed below) to fit within the 
# HPC's 6-hour session limit.

processed_indices <- c(
  "1 to 42000", "42001 to 84000", "84001 to 126000", "126001 to 168000",
  "168001 to 210000", "210001 to 252000", "252001 to 294000", "294001 to 336000",
  "336001 to 378000", "378001 to 420000", "420001 to 462000", "462001 to 504000",
  "504001 to 546000", "546001 to 588000", "588001 to 630000", "630001 to 672000",
  "672001 to 714000", "714001 to 756000", "756001 to 798000", "798001 to 840000",
  "840001 to 882000", "882001 to 924000", "924001 to 966000", "966001 to 1008000",
  "1008001 to 1050000", "1050001 to 1092000", "1092001 to 1134000", 
  "1134001 to 1176000", "1176001 to 1210975"
)


## --------------------
## SUBSECTION A1: Prepare Subset for HPC

# Create a subset of the data only relevant to verifying the listed addresses in
# the HPC environemnt.
step_1_subset <- step_1 %>%
  select(rowname, address_line_1, address_line_2, city, state,
         zipcode, zipcode_ext, compiled_address, address_verified)

#write.csv(step_1_subset, file = "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2023 Format/Step 01_2023 Format_HPC Subset_05.01.2026.csv")

# NEXT STEP: Before continuing, pass these results through the validation
#            function located in "Clean Raw Data_Step 2 HPC_2023 Format.R". 
#            Steps on how to do this locally or in the HPC are provided in 
#            section "PART A: UTILIZING THE HPC".


## --------------------
## SUBSECTION A2: Compile the Results

# Define the base file location with the saved results.
dir_path <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/HPC Results"

# Generate all qualifying file locations.
files <- file.path(dir_path, paste0("Step 2_2023 Format_USPS Output_", processed_indices, ".csv"))

# Import all the data frames as a list.
dfs <- lapply(files, read_csv, show_col_types = FALSE,
  col_types = cols(
    rowname = col_character(),
    zipcode = col_character(),
    zipcode_ext = col_character()
  )
) %>%
  `names<-`(processed_indices)


# Prior to compiling the results, the following quality checks are performed:
#   1. Evaluate the dimensions of each subset
#   2. Confirm that the indices in each subset fully represent the expected range
#   3. Assess the number of verified addresses; a high rate of failures may 
#      warrant re-processing that index

# Generate the QC results, assuming each index range contains 42,000 indices,
# with the exception of the last range, which may differ.
qc_results <- qc_validation_results(dfs, denom = 42000)

# Not all subsets share the same number of columns, but the row counts for each 
# subset are as expected, excluding the last index range.
qc_results$dims %>% as.data.frame()
qc_results$row_representation
qc_validation_results(dfs[-29], denom = 42000)$row_representation_ratio

# All indices span the expected range, suggesting that all entries have been 
# processed and is accounted for.
#qc_results$per_row
qc_results$within_expected_table
qc_results$per_file %>% as.data.frame()

# Several index ranges yielded fewer validation matches than expected,
# with one range producing no matches at all. Re-processing will be skipped,
# as the 2026 Format dataset will be used for the final dashboard instead.
qc_results$address_verifications


# As observed in the dimensions QC results, not all tables share the same number
# of columns. This discrepancy arises because some results were produced locally
# using the full dataset, while others were produced on the HPC using a subset
# that lacks certain metadata columns.
#
# The following function automatically associates the correct metadata where
# needed before combining the rows of each subset.

# Save the table summarizing the dimensions of each subset.
dims_tbl <- qc_results$dims %>% as.data.frame()

# Combine all results.
step_2 <- combine_mixed_dfs(dfs, dims_tbl) %>% 
  # Remove the rowname columns introduced during the subset merge and reorder.
  select(colnames(step_1))

# Final confirmation that all rowname entries have been accounted for.
length(step_1$rowname[step_1$rowname %!in% step_2$rowname]) == 0 &
  length(step_2$rowname[step_2$rowname %!in% step_1$rowname]) == 0




## ----------------------------------------------------------------
## PART B: Complete Handling of Special Cases

# Address collapsing introduced duplicate year-opened and year-closed values
# that were not present prior to that step. Duplicates arose from two sources:
# (1) cross-cluster address matches that incorrectly associated records across
# groups, and (2) address matches with multiple metadata variations that
# inflated row upon joining.
# 
# Together, these resulted in year-opened and year-closed values being counted
# multiple times across different address lines.
# 
# Additionally, some entries failed the geolocation test as a consequence of
# the collapsing step. These failures fell into two categories: addresses
# incorrectly collapsed together (i.e., distinct addresses treated as one),
# and discrepancies between likely identical addresses and their attributed
# geolocations.
# 
# Failed entries were re-clustered using exact compiled address matches only, 
# and those assumed to represent the same address were marked for a geolocation 
# test override.
# 
# Recall the results codebook:

#' @description 
#' Codebook for the new output fields produced by the data cleaning and 
#' validation Step 1.
#'
#'#' @field override_duplicate Boolean. TRUE if the address was manually
#'                             identified as the same physical address,
#'                             indicating that the failed longitude and latitude 
#'                             similarity test should be overridden. FALSE if 
#'                             the failed test still applies. NA if this 
#'                             evaluation did not apply.
#'                           
#' @field same_num_clusters Expanded: addresses that were initially collapsed,
#'                          failed the longitude and latitude similarity test, 
#'                          and required expansion for individual validation. 
#'                          TRUE: addresses that also failed the similarity test 
#'                          but clustered to an exact match and were kept 
#'                          collapsed. FALSE: other addresses associated with 
#'                          the same business where expansion assessment is not 
#'                          applicable.


## --------------------
## SUBSECTION B1: ABI-Specific Duplication Confirmation

# First, confirm that duplications are only detected among entries associated
# with the expected ABI. Should duplications be detected among other entries,
# those entries will require duplication reconciliation as well.

# Review outcome distributions across duplicated and geolocation-failed entries.
table("Same Address Line 1" = step_2$override_duplicate, "Same Number of Clusters" = step_2$same_num_clusters, useNA = "ifany")


# --------------------
# Subset entries based on prior edge-case address handling.

# Isolate ABIs requiring no further address collapsing or special handling.
not_special_case <- step_2 %>% 
  group_by(abi) %>%
  filter(all(is.na(override_duplicate)) & all(is.na(same_num_clusters))) %>%
  pull(abi) %>%
  unique()

# Isolate ABIs assessed for expansion only, with no duplication override,
# as no addresses were identified as matching.
fix_expanded <- step_2 %>% 
  group_by(abi) %>%
  filter(all(is.na(override_duplicate)) & any(!is.na(same_num_clusters))) %>%
  pull(abi) %>%
  unique()

# Isolate ABIs with addresses identified as matching, regardless of expansion
# applicability, as this may affect any address in the set.
fix_diff_metadata <- step_2 %>% 
  group_by(abi) %>%
  filter(any(!is.na(override_duplicate))) %>%
  pull(abi) %>%
  unique()

# Confirm all ABI have been accounted for.
( length(unique(step_2$abi)) - length(unique(step_2$abi) %in% c(not_special_case, fix_expanded, fix_diff_metadata)) ) == 0 &
  ( length(unique(step_2$abi)) - length(c(not_special_case, fix_expanded, fix_diff_metadata) %in% unique(step_2$abi)) ) == 0

# Confirm none of the special cases are in the not special case subset.
length(not_special_case[not_special_case %in% c(fix_expanded, fix_diff_metadata)]) == 0 &
  length(c(fix_expanded, fix_diff_metadata)[c(fix_expanded, fix_diff_metadata) %in% not_special_case]) == 0

# Confirm that the special cases are themselves mutually exclusive.
length(fix_expanded[fix_expanded %in% fix_diff_metadata]) == 0 &
  length(fix_diff_metadata[fix_diff_metadata %in% fix_expanded]) == 0


# --------------------
# Verify that duplications are limited to the two "fix" ABI sets and absent
# from entries requiring no special handling. Comment or uncomment as needed
# to filter for the correct entries and result.

# NOTE: Results were already generated and saved. Load them below.


# Count the number of unique ABIs.
total_groups <- step_2 %>%

  # Comment/uncomment which ABI is being assessed.
  filter(abi %in% not_special_case) %>%
  #filter(abi %in% fix_expanded) %>%
  #filter(abi %in% fix_diff_metadata) %>%

  group_by(abi) %>% n_groups()

# Initialize progress bar
pb <- progress_bar$new(
  format = "  processing [:bar] :percent eta: :eta",
  total = total_groups,
  clear = FALSE, width = 60
)

# Run the duplication test with progress bar.
test_no_dup <- step_2 %>%

  # Comment/uncomment which ABI is being assessed.
  filter(abi %in% not_special_case) %>%
  #filter(abi %in% fix_expanded) %>%
  #filter(abi %in% fix_diff_metadata) %>%

  # Group the data by ABI to be processed separately.
  group_by(abi) %>%
  # Apply the custom function 'check_all_counts_0_or_1' with progress tracking to each group.
  group_modify(~ process_with_progress(pb, .x, check_all_counts_0_or_1)) %>%
  # Remove the grouping to return to an ungrouped data frame.
  ungroup() %>%
  # Convert the grouped data back to a standard data frame.
  as.data.frame()


#' @description 
#' Codebook for the output fields produced by the evaluation.
#'
#' @field abi Unique business identifier. Evaluation is performed over each 
#'            unique business ID.
#'
#' @field `2001:2021` Column-wise sum of all entries associated with the given 
#'                    business ID.
#'
#' @field all_counts_0_or_1 Boolean. TRUE if all date entry sums for the given
#'                          business ID are equal to 0 or 1.

# # Commit results.
# write.csv(test_no_dup, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_All Other Data_06.04.2026.csv")
# write.csv(test_no_dup, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_Fix Expanded_06.04.2026.csv")
# write.csv(test_no_dup, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_Fix Metadata_06.04.2026.csv")


# Read in previously generated results.
not_special_case_results <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_All Other Data_06.04.2026.csv", 
                                     col_types = cols(...1 = col_skip())) %>% as.data.frame()
fix_expanded_results     <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_Fix Expanded_06.04.2026.csv", 
                                     col_types = cols(...1 = col_skip())) %>% as.data.frame()
fix_metadata_results     <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 2 Subsection B_Fix Metadata_06.04.2026.csv", 
                                     col_types = cols(...1 = col_skip())) %>% as.data.frame()

# FALSE represents ABIs for which duplications were detected.
table(not_special_case_results$all_counts_0_or_1, useNA = "ifany")
table(fix_expanded_results$all_counts_0_or_1, useNA = "ifany")
table(fix_metadata_results$all_counts_0_or_1, useNA = "ifany")

# 89% of businesses will not require re-clustering.
round( nrow(not_special_case_results)/length(unique(step_2$abi)) * 100, digits = 2)


## --------------------
## SUBSECTION B2: Consolidate Duplications and Remove Difficult Cases

# Duplicate records introduced during address collapsing are reconciled using a
# staged approach. In the first stage, exact duplicates are removed by retaining
# one copy. Records that remain duplicated due to heterogeneous year-opened or
# year-closed outcomes are repaired with the original values recovered from 
# the raw data to resolve any conflicting entries.
# 
# Additional strategies to further reduce candidate entries, such as ones
# with the exact same address, are done in subsequent sections.

search_space <- c(
  not_special_case_results[not_special_case_results$all_counts_0_or_1 == FALSE, "abi"],
  fix_expanded_results[fix_expanded_results$all_counts_0_or_1 == FALSE, "abi"],
  fix_metadata_results$abi
)

supplement_build <- vector("list", length(search_space))  # Initialize an empty list
pb = txtProgressBar(min = 0, max = length(search_space), style = 3)  # Initialize progress bar

finish_build <- vector("list", length(search_space))  # Initialize an empty list
qc_tbl <- NULL # Initialize an empty variable

for (i in 1:length(search_space)) {
  # Pull business information where erroneous reduplications were detected.
  subset <- step_2 %>%
    filter(abi %in% search_space[i])
  
  # Pull related raw data for date column overwrite. Applied only if the 
  # duplication removal step fails.
  subset_raw <- church_wide %>%
    filter(abi %in% search_space[i])
  
  # Initialize all entries as corrected. If the duplication reduction step does
  # not fully resolve all entries, a direct override is attempted. If the
  # override also fails, all_corrected is set to FALSE.
  all_corrected = TRUE
  
  # Initialize the QC data table
  qc_tbl[[i]] <- data.frame(
    "abi"  = unique(subset$abi),
    "Attempt Replacement" = FALSE,
    "Lost Data" = NA,
    "No Match Found" = NA
  )
  
  
  # --------------------
  # Replace "No address match found" with the compiled form of the given address.
  
  subset <- subset %>%
    dplyr::mutate(
      compiled_address = dplyr::if_else(
        compiled_address == "No address match found",
        stringr::str_squish(paste0(
          dplyr::coalesce(address_line_1, ""), ", ",
          dplyr::coalesce(address_line_2, ""), ", ",
          dplyr::coalesce(city, ""), ", ",
          dplyr::coalesce(state, ""), " ",
          dplyr::coalesce(zipcode, ""),
          dplyr::if_else(is.na(zipcode_ext) | zipcode_ext == "", "", paste0("-", zipcode_ext))
        )),
        compiled_address
      )
    )
  
  # --------------------
  # Reconcile metadata with mismatched outcomes within assigned groups.
  
  subset = subset[, -c(1)] %>% distinct()
  
  # Assess if the duplications were removed by distinct().
  distinct_worked <- subset %>%
    # Apply the custom function 'check_all_counts_0_or_1'.
    group_modify(~ check_all_counts_0_or_1(.x)) %>%
    # Remove the grouping to return to an ungrouped data frame.
    ungroup() %>%
    # Pull the result.
    pull(all_counts_0_or_1)
  
  # If distinct() fails, replace affected entries with the original address
  # where a match is found. Two QC checks are performed to ensure no
  # information is lost and only exact matches are applied.
  if(!distinct_worked) {
    
    # QC denoting a replacement was attempted.
    attempt_replacement <- TRUE
    
    # --------------------
    # Prepare the raw dataset to match with addresses.
    
    # Add the compiled_address variable.
    subset_raw_dates <- subset_raw[, c(6, 4, 3, 5, 11:31)] %>%
      mutate(zipcode = as.character(zipcode)) %>%
      mutate(
        compiled_address = stringr::str_squish(paste0(
          coalesce(address_line_1, ""), ", , ",
          coalesce(city, ""), ", ",
          coalesce(state, ""), " ",
          coalesce(as.character(zipcode), "")
        ))
      ) %>%
      relocate(compiled_address, .after = zipcode)
    
    # Identify the unique outcomes available for collapsing.
    idx <- subset_raw_dates %>%
      mutate(.idx = row_number()) %>%
      group_by(compiled_address) %>%
      summarise(indices = list(.idx), n = n(), .groups = "drop")
    
    hits <- idx %>% filter(n >= 2) %>% pull(indices)
    no_hits <- idx %>% filter(n == 1) %>% pull(indices) %>%
      (\(x) {unlist(x, use.names = FALSE)} )()
    
    if(length(hits) != 0L) {
      # Stepwise collapse the duplicates.
      build <- NULL
      for (j in 1:length(hits)) {
        # Pull out rows that are affiliated with similar addresses.
        change_these <- hits[[j]]
        
        # Use the support function find_similar_addresses() to compare the 
        # addresses and assign them into groups based on degree of similarity.
        match <- find_similar_addresses(as.character(subset_raw_dates[change_these, "compiled_address"]), threshold = 0)
        
        if (length(match) != 1) stop("Expected exactly one match group.")
        match <- match[[1]]
        
        # Sum over the openings.
        dates <- sapply(subset_raw_dates[change_these, 6:26], function(x) sum(x, na.rm = TRUE)) %>%
          (\(x) { as.data.frame(t(x)) }) ()
        
        build <- rbind(build, cbind(
          # Deconstruct the expanded columns representation of the address.
          str_match(
            match,
            "^\\s*([^,]*?)\\s*,\\s*([^,]*?)\\s*,\\s*([^,]*?)\\s*,\\s*([A-Z]{2})\\s+(\\d{5})(?:-(\\d{4}))?\\s*$"
          ) %>%
            (\(m) {
              x <- as.data.frame(t(m[1, 2:6]), stringsAsFactors = FALSE)  # drop zip ext
              names(x) <- c("address_line_1","address_line_2","city","state","zipcode")
              x[] <- lapply(x, \(v) { v <- stringr::str_trim(v); dplyr::na_if(v, "") })
              x
            })(),
          # Store the compiled address string.
          as.data.frame(match) %>% `colnames<-`(c("compiled_address")),
          # Add the summed dates.
          dates
        ))
      }
      
      # Commit the reduced raw data.
      subset_raw_dates <- build %>%
        bind_rows(subset_raw_dates[no_hits, ]) %>%
        mutate(across(matches("^\\d{4}$"), ~ tidyr::replace_na(., 0)))
    }
    
    # --------------------
    # Two critical quality checks.

    # Setup: year columns + helper.
    date_cols <- as.character(2001:2021)
    normalize_compiled_address <- function(x) {
      x <- sub("(\\d{5})-\\d{4}\\s*$", "\\1", x)  # ZIP+4 -> ZIP5
      
      # normalize the 5-digit ZIP that follows the state abbreviation:
      # drop leading zeros and trailing zeros (only within that ZIP token)
      x <- sub("(\\b[A-Z]{2}\\s+)(\\d{5})(\\b)", "\\1\\2\\3", x, perl = TRUE)
      x <- gsub("(\\b[A-Z]{2}\\s+)0+(\\d{1,5})\\b", "\\1\\2", x, perl = TRUE)   # leading zeros
      x <- gsub("(\\b[A-Z]{2}\\s+\\d{1,5})0+\\b", "\\1", x, perl = TRUE)       # trailing zeros
      
      x
    }
    
    # Find rows that contribute to any year-column whose total > 1 in subset.
    years_mat <- subset[, date_cols, drop = FALSE]
    dup_cols  <- colSums(years_mat, na.rm = TRUE) > 1L
    dup_rows  <- which(rowSums(years_mat[, dup_cols, drop = FALSE], na.rm = TRUE) > 0L)
    
    # Match by address after stripping ZIP+4 to ZIP5.
    sub_keys <- normalize_compiled_address(subset$compiled_address)[dup_rows]
    raw_keys <- normalize_compiled_address(subset_raw_dates$compiled_address)
    idx <- match(sub_keys, raw_keys)
    
    has_match <- !is.na(idx)
    
    # QC 1:1 ONLY (ignores no-match; those are handled separately):
    # For rows that DID match, the key must be unique in raw and in the 
    # subset slice.
    sub_key_unique <- !duplicated(sub_keys) & !duplicated(sub_keys, fromLast = TRUE)
    raw_key_unique_for_row <- rep(FALSE, length(sub_keys))
    raw_key_unique_for_row[has_match] <- {
      rk <- raw_keys[idx[has_match]]
      !duplicated(rk) & !duplicated(rk, fromLast = TRUE)
    }
    
    # 1:1 among matched rows
    qc_1to1 <- has_match & sub_key_unique & raw_key_unique_for_row
    
    # Separate flag for failed-to-match
    qc_no_match <- !has_match
    
    # QC Ensure no loss of positives: 
    # Any year where subset has >0 for the rows being updated must also be 
    # >0 in subset_raw_dates.
    sub_any <- colSums(subset[dup_rows[has_match], date_cols, drop = FALSE], na.rm = TRUE) > 0L
    raw_any <- colSums(subset_raw_dates[idx[has_match], date_cols, drop = FALSE], na.rm = TRUE) > 0L
    
    missing_in_raw <- sub_any & !raw_any
    missing_cols <- names(missing_in_raw)[missing_in_raw]
    
    lost_data <- length(missing_cols) > 0L
    
    # Update the QC data.
    qc_tbl[[i]] <- data.frame(
      "abi"  = unique(subset$abi),
      "Attempt Replacement" = attempt_replacement,
      "Lost Data" = lost_data,
      "No Match Found" = any(qc_no_match) == TRUE
    )
    
    # --------------------
    # If QC passes, overwrite the year columns for matched rows in the raw data.
    
    if (all(qc_1to1) == TRUE & lost_data == FALSE) {
      subset[dup_rows[has_match], date_cols] <- subset_raw_dates[idx[has_match], date_cols]
    } else {
      all_corrected = FALSE
    }
    
  }

  # Save the result.
  finish_build[[i]] <- subset %>%
    mutate(all_duplicates_corrected = all_corrected)
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}

# Combine all data tables in the list into one data table.
finish_build <- rbindlist(finish_build, use.names = TRUE, fill = TRUE)
qc_results   <- rbindlist(qc_tbl, use.names = TRUE, fill = TRUE) %>% as.data.frame()

# Replace NA values in the date columns with zero.
finish_build[, (as.character(2001:2021)) := lapply(.SD, \(x) fifelse(is.na(x), 0, x)), .SDcols = as.character(2001:2021)]

# Duplication was resolved for most entries; 7% could not be reconciled.
round(table(finish_build$all_duplicates_corrected, useNA = "ifany") / nrow(finish_build) * 100, digits = 2)

# Most entries require patching the year from the raw dataset; 20% were 
# resolved with distinct(), and ~5% remained unresolved.
round((table("Lost Data" = qc_results$Lost.Data, "No Match Found" = qc_results$No.Match.Found, useNA = "ifany") / length(search_space)) * 100, digits = 2)


# Less than 1% of all entries (~0.09% of businesses) could not be resolved
# algorithmically and may require manual review. These failures are likely
# due to corrected addresses returned by the USPS API. These will be removed 
# from the main dataset to avoid inducing errors.

round(table(finish_build$all_duplicates_corrected)["FALSE"][[1]] / nrow(step_2) * 100, digits = 2)

finish_build %>% 
  group_by(abi) %>%
  filter(any(!all_duplicates_corrected, na.rm = TRUE)) %>%
  n_groups() %>%
  (\(x) {round(x / length(unique(step_2$abi)) * 100, digits = 2)} )()


# Confirm that all entries flagged for duplication checks were successfully 
# resolved. Reconciled entries are committed back to the main dataset.

# Count the number of unique ABIs.
total_groups <- finish_build %>%
  group_by(abi) %>%
  filter(all(all_duplicates_corrected, na.rm = TRUE)) %>%
  n_groups()

# Initialize progress bar
pb <- progress_bar$new(
  format = "  processing [:bar] :percent eta: :eta",
  total = total_groups,
  clear = FALSE, width = 60
)

# Run the duplication test with progress bar.
test_no_dup <- finish_build %>%
  group_by(abi) %>%
  filter(all(all_duplicates_corrected, na.rm = TRUE)) %>%
  # Group the data by ABI to be processed separately.
  group_by(abi) %>%
  # Apply the custom function 'check_all_counts_0_or_1' with progress tracking to each group.
  group_modify(~ process_with_progress(pb, .x, check_all_counts_0_or_1)) %>%
  # Remove the grouping to return to an ungrouped data frame.
  ungroup() %>%
  # Convert the grouped data back to a standard data frame.
  as.data.frame()

# Confirm all duplicates were corrected.
test_no_dup$all_counts_0_or_1 %>% table()

# Save entries where all duplicate instances were fully resolved for all
# addresses associated with an ABI.
corrected_dup <- finish_build %>%
  group_by(abi) %>%
  filter(all(all_duplicates_corrected, na.rm = TRUE)) %>%
  ungroup() %>%
  as.data.frame()


## --------------------
## SUBSECTION B3: Consolidate Addresses with Different City

# We assumed that identical address_line_1 within an ABI indicates the same 
# location. Geolocation mismatches for these cases were overridden. If API 
# validation produced a unique address the records were retained separately; if 
# not, the unvalidated record was linked to an available validated address in 
# the ABI, preferring temporally adjacent entries.

# Separate duplicate-corrected records to identify those overridden and 
# containing at least one validated and one unvalidated address.
fix_diff_metadata_df <- corrected_dup %>%
  filter(abi %in% fix_diff_metadata) %>%
  group_by(abi) %>%
  filter(
    any(address_verified == TRUE  & override_duplicate == TRUE, na.rm = TRUE) &
      any(address_verified == FALSE & override_duplicate == TRUE, na.rm = TRUE)
  ) %>%
  ungroup()


# Records passed for a second aggregation pass represent 0.02% of ABI entries.
round( length(unique(fix_diff_metadata_df$abi))/length(unique(step_2$abi)) * 100, digits = 2)


# The pipe associates each unvalidated entry with an available validated address, 
# replacing the unvalidated form. Note: subsequent prior/next replacement 
# behavior depends on the ordering produced by this step.

fix_diff_metadata_df <- fix_diff_metadata_df %>%
  # Sort rows by abi (ascending).
  arrange(abi) %>%
  
  # Compute the first year (2001–2021) where the row has a 1 in the year columns.
  mutate(
    First_One_Year = pmap_chr(
      select(., all_of(as.character(2001:2021))),
      find_first_one
    )
  ) %>%
  
  # If year columns are named like X2001, X2002, ... remove the "X" prefix.
  rename_with(~ sub("^X", "", .), starts_with("X")) %>%
  
  # Within each abi, sort so the oldest address record comes first.
  # This ordering defines what “immediately prior” means.
  group_by(abi) %>%
  arrange(First_One_Year, .by_group = TRUE) %>%
  ungroup() %>%
  
  # Now do override-based replacement within each (abi, address_line_1) timeline.
  group_by(abi, address_line_1) %>%
  
  # Only apply replacement logic if at least one row in the group has override_duplicate == TRUE.
  mutate(any_override = any(override_duplicate %in% TRUE, na.rm = TRUE)) %>%
  
  mutate(
    # For each field we want to copy, keep its value ONLY on verified rows; otherwise NA.
    # These become the “donor” values we carry forward/backward.
    v_compiled = if_else(address_verified %in% TRUE, compiled_address, NA_character_),
    v_line2    = if_else(address_verified %in% TRUE, address_line_2, NA_character_),
    v_city     = if_else(address_verified %in% TRUE, city, NA_character_),
    v_state    = if_else(address_verified %in% TRUE, state, NA_character_),
    v_zip      = if_else(address_verified %in% TRUE, zipcode, NA_character_),
    v_zipext   = if_else(address_verified %in% TRUE, zipcode_ext, NA_character_),
    
    # PRIOR verified values (nearest verified row ABOVE in the sorted order):
    # - fill downward (carry last verified value down through NAs)
    # - then lag() so it’s strictly “above” the current row
    prior_compiled = { x <- v_compiled; for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    prior_line2    = { x <- v_line2;    for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    prior_city     = { x <- v_city;     for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    prior_state    = { x <- v_state;    for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    prior_zip      = { x <- v_zip;      for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    prior_zipext   = { x <- v_zipext;   for (i in seq_along(x)) if (i>1 && is.na(x[i])) x[i] <- x[i-1]; lag(x) },
    
    # NEXT verified values (nearest verified row BELOW in the sorted order):
    # - fill upward (carry next verified value up through NAs)
    # - then lead() so it’s strictly “below” the current row
    next_compiled = { x <- v_compiled; for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    next_line2    = { x <- v_line2;    for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    next_city     = { x <- v_city;     for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    next_state    = { x <- v_state;    for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    next_zip      = { x <- v_zip;      for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    next_zipext   = { x <- v_zipext;   for (i in length(x):1) if (i<length(x) && is.na(x[i])) x[i] <- x[i+1]; lead(x) },
    
    # Choose replacement values:
    # prefer the prior verified row; if none exists, fall back to the next verified row.
    repl_compiled = coalesce(prior_compiled, next_compiled),
    repl_line2    = coalesce(prior_line2,    next_line2),
    repl_city     = coalesce(prior_city,     next_city),
    repl_state    = coalesce(prior_state,    next_state),
    repl_zip      = coalesce(prior_zip,      next_zip),
    repl_zipext   = coalesce(prior_zipext,   next_zipext),
    
    # Change condition:
    # - override is active for this group
    # - this row is not verified
    # - we have a donor verified row (we key off repl_compiled being present)
    will_change = any_override & !(address_verified %in% TRUE) & !is.na(repl_compiled),
    
    # Apply replacements to ALL requested fields.
    compiled_address = if_else(will_change, repl_compiled, compiled_address),
    address_line_2   = if_else(will_change, repl_line2,    address_line_2),
    city             = if_else(will_change, repl_city,     city),
    state            = if_else(will_change, repl_state,    state),
    zipcode          = if_else(will_change, repl_zip,      zipcode),
    zipcode_ext      = if_else(will_change, repl_zipext,   zipcode_ext),
    
    # Mark changed rows explicitly; keep verified rows as "TRUE".
    # (This converts address_verified to character.)
    address_verified = case_when(
      will_change ~ "Updated",
      address_verified %in% TRUE ~ "TRUE",
      TRUE ~ as.character(address_verified)
    )
  ) %>%
  ungroup() %>%
  
  # Remove helper columns created for ordering/replacement.
  select(
    -First_One_Year, -any_override,
    -starts_with("v_"),
    -starts_with("prior_"), -starts_with("next_"),
    -starts_with("repl_"),
    -will_change
  ) %>%
  
  as.data.frame()


# About 35% were able to be matched with a verified address.
round((table(fix_diff_metadata_df$address_verified, useNA = "ifany") / nrow(fix_diff_metadata_df)) * 100, digits = 2)

# The majority of flagged entries failed geolocation (85%). This was also common 
# among verified addresses, affecting 56%.
tab <- table(
  "Address Verified" = fix_diff_metadata_df$address_verified,
  "Geolocation Test" = fix_diff_metadata_df$lonLat_test,
  useNA = "ifany"
)
round(prop.table(tab, margin = 1) * 100, 2)


# Re-aggregate entries flagged for additional consolidation.

search_space2 <- fix_diff_metadata_df %>% # Isolate ABIs that need to be expanded
  pull(abi) %>%
  unique()

finish_build2 <- vector("list", length(search_space2))  # Initialize an empty list
pb = txtProgressBar(min = 0, max = length(search_space2), style = 3)  # Initialize progress bar

for (i in 1:length(search_space2)) {
  subset <- fix_diff_metadata_df %>%
    filter(abi %in% search_space2[i])
  
  # --------------------
  # Match addresses that are similar for compression.

  # Make the entire address elements into one string.
  compile_address <- subset$compiled_address

  # Use the support function find_similar_addresses() to compare the addresses
  # and assign them into groups based on exact similarity.
  if (length(compile_address) == 0) {
    stop("compile_address has length 0; cannot determine match.")
  } else if (length(compile_address) == 1) {
    match <- compile_address
  } else { # length > 1
    match <- find_similar_addresses(as.character(compile_address), threshold = 0)
  }

  # --------------------
  # Reconcile metadata with mismatched outcomes within assigned groups.

  # Retain the metadata stored from the first attempt at collapsing addresses.
  extra_naics_code <- subset %>% pull(extra_naics_code) %>% .[1]
  year_est         <- subset %>% pull(year_established) %>% .[1]

  # --------------------
  # Rebuild the dataframe and remove erroneous reduplicates

  # Define the starting structure of the metadata that will be used to build the
  # new dataframe that collapses reduplicates.
  seed <- data.frame("abi" = unique(subset$abi), "year_established" = year_est,
                     "primary_naics_code" = 813110, "extra_naics_code" = extra_naics_code,
                     "naics8_descriptions" = "Religious Organizations")

  # Stepwise collapse the duplicates.
  build <- NULL
  for (j in 1:length(match)) {
    # Pull out rows that are affiliated with the same addresses.
    change_these <- match[[j]] %>% as.vector() %>%
      (\(y) { map_lgl(subset$compiled_address, ~ str_detect(.x, regex(paste0("^", str_trim(y), "(,|$)"), ignore_case = TRUE))) })()

    # Sum over the openings.
    dates <- sapply(subset[change_these, 17:37], function(x) sum(x, na.rm = TRUE)) %>%
      (\(x) { as.data.frame(t(x)) }) ()

    # Test how similar the longitude and latitude are.
    negligible_change <- 0.002  # Change in degrees (~222 meters or 728 feet)

    lonLat_test <- if (nrow(subset[change_these, ]) == 1) {
      subset[change_these, ]$lonLat_test
    } else {
      abs(max(subset[change_these, ]$longitude) - min(subset[change_these, ]$longitude)) < negligible_change &
        abs(max(subset[change_these, ]$latitude)  - min(subset[change_these, ]$latitude))  < negligible_change
    }

    # compute the flag once (and make it 1 value, not a 1-col df with no name)
    address_verified_ok <- !any(subset$address_verified %in% FALSE, na.rm = TRUE) &&
      all(subset$address_verified %in% c(TRUE, "Updated"), na.rm = TRUE)
    
    addr_parts <- str_split(match[[j]][1], ", ", simplify = TRUE)
    # addr_parts is 1x4: address_line_1, city, state, zipcode
    
    new_row <- dplyr::bind_cols(
      seed[1, 1, drop = FALSE],  # ABI (1 row)
      tibble::tibble(
        address_line_1 = addr_parts[1, 1],
        address_line_2 = NA_character_,
        city          = addr_parts[1, 2],
        state         = addr_parts[1, 3],
        zipcode       = addr_parts[1, 4],
        zipcode_ext   = NA_character_,
        same_num_clusters   = NA,
        override_duplicate  = NA,
        compiled_address    = match[[j]][1],
        address_verified    = address_verified_ok,
        lonLat_test         = lonLat_test,
        latitude            = mean(subset[change_these, "latitude"],  na.rm = TRUE),
        longitude           = mean(subset[change_these, "longitude"], na.rm = TRUE)
      ),
      seed[1, -1, drop = FALSE],   # rest of metadata, 1 row
      dates,                       # must be 1 row
      tibble::tibble(
        all_duplicates_corrected =
          unique(subset[change_these, "all_duplicates_corrected"])[1]
      )
    ) %>%
      dplyr::relocate(address_line_2, .after = address_line_1) %>%
      dplyr::relocate(zipcode_ext, .after = zipcode) %>%
      dplyr::relocate(override_duplicate, .after = "2021") %>%
      dplyr::relocate(same_num_clusters, .after = override_duplicate)
    
    build <- dplyr::bind_rows(build, new_row)
  }

  # Store 'build' in the list.
  finish_build2[[i]] <- build %>%
    mutate(
      zipcode = as.character(zipcode),
      override_duplicate = TRUE,
      all_duplicates_corrected = TRUE
    )

  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}

# Combine all data tables in the list into one data table.
finish_build2 <- rbindlist(finish_build2, use.names = TRUE, fill = TRUE)




## ----------------------------------------------------------------
## PART C: Organize and Save the Results

# Before recompiling the datasets, verify that all components are accounted for
# and will be retained after parsing.

# Compile each component
abi_sets <- list(
  fix_diff_metadata = finish_build2$abi,
  corrected_dup     = corrected_dup$abi[corrected_dup$abi %!in% fix_diff_metadata_df$abi],
  finish_build      = finish_build$abi[finish_build$abi %!in% corrected_dup$abi],
  not_special_case  = not_special_case[not_special_case %!in% finish_build$abi],
  fix_expanded      = fix_expanded_results$abi[fix_expanded_results$abi %!in% finish_build$abi]
)

# Drop NAs and coerce to character so comparisons are consistent
abi_sets <- lapply(abi_sets, function(v) as.character(stats::na.omit(v)))

# Pairwise overlap check (mutual exclusivity requires all intersections to be empty)
overlap_counts <- outer(
  names(abi_sets), names(abi_sets),
  Vectorize(function(a, b) {
    if (a == b) return(NA_integer_)
    length(intersect(unique(abi_sets[[a]]), unique(abi_sets[[b]])))
  })
)

# Confirmed: diagonal entries are NA and all off-diagonal entries are zero
overlap_counts


# Compile all the different pieces of the dataset, including portions that
# did not require further assessment and those that were processed Only entries
# where verification was attempted but the mitigation efforts were not 
# successful were removed.

# NOTE: this is equivalent to 
#       finish_build$abi[finish_build$abi %!in% corrected_dup$abi]
failed_dup_correction <- finish_build %>%
  group_by(abi) %>%
  filter(any(!all_duplicates_corrected, na.rm = TRUE)) %>%
  ungroup() %>%
  as.data.frame()

# List the dataframes to be recompiled, excluding any ABIs that failed resolution.
df_list <- list(
  fix_diff_metadata = finish_build2,
  
  corrected_dup = corrected_dup %>%
    filter(abi %!in% fix_diff_metadata_df$abi),
  
  not_special_case = step_2 %>% 
    filter(abi %in% not_special_case[not_special_case %!in% finish_build$abi]),
  
  fix_expanded = step_2 %>%
    filter(abi %in% fix_expanded_results$abi[fix_expanded_results$abi %!in% finish_build$abi])
)

# Standardize column formatting
df_list <- lapply(df_list, \(d) d %>% mutate(address_verified = as.character(address_verified)))

# Combine all elements and remove the excess columns
combined_df <- bind_rows(df_list, .id = "source") %>%
  mutate(abi = as.character(abi)) %>%
  filter(!is.na(abi)) %>%
  `rownames<-`(NULL) %>% 
  select(-source) %>%
  relocate(rowname, .before = abi)

# Confirm all ABI are accounted for, only excluding any ABIs that failed resolution. 
( length(unique(step_2$abi)) - length(unique(step_2$abi)[unique(step_2$abi) %in% unique(combined_df$abi)]) ) == length(unique(failed_dup_correction$abi)) &
  ( length(unique(step_2$abi)) - length(unique(combined_df$abi)[unique(combined_df$abi) %in% unique(step_2$abi)]) ) == length(unique(failed_dup_correction$abi))

# Confirm all the columns are present, where all_duplicates_corrected is new
# for the final result.
all(c(colnames(step_2), "all_duplicates_corrected") %in% colnames(combined_df)) && all(colnames(combined_df) %in% c(colnames(step_2), "all_duplicates_corrected"))

# # Commit results.
# write.csv(combined_df, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 02_Completed Result_06.01.2026.csv")

# Read in previously generated results.
step_2_final <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 02_Completed Result_06.01.2026.csv", 
                         col_types = cols(...1 = col_skip())) %>% as.data.frame()




## ----------------------------------------------------------------
## PART D: Assess the Algorithms Performance

# After reconciling duplications, the final dataset has 2.5% more rows than step_1.
(length(step_2_final) - length(step_1)) / length(step_1) * 100

# 876 unique ABIs could not be fully corrected for duplication (these ABIs were 
# removed/excluded), representing 0.09% of all unique ABIs.
length(unique(failed_dup_correction$abi))
round(length(unique(failed_dup_correction$abi)) / length(unique(step_2$abi)) * 100, 2)

# Almost 6% of addresses were verified.
round(prop.table(table(step_2_final$address_verified, useNA = "ifany")) * 100, 2)

# Most addresses that were verified either passed or did not require a geolocation test.
round(prop.table(
  table("Geolocation Test" = step_2_final$lonLat_test,
        "Address Verified" = step_2_final$address_verified,
        useNA = "ifany")
) * 100, 2)


# PO Boxes require special attention to verify their geolocation.

# Isolate all PO Boxes.
poBox_all <- step_2_final %>%
  filter(str_detect(
    coalesce(address_line_1, ""),
    regex("\\bP\\s*\\.?\\s*O\\s*\\.?\\s*Box\\b", ignore_case = TRUE)
  ))

# Most PO Box entries failed to verify against the USPS API; however, most passed
# the geolocation test when consolidated.
round(prop.table(
  table("Geolocation Test" = poBox_all$lonLat_test,
        "Address Verified" = poBox_all$address_verified,
        useNA = "ifany")
) * 100, 2)










