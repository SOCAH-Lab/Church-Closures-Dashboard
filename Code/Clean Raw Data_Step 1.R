## ----------------------------------------------------------------
## Consolidate reduplicate records caused by minor address typographical variations.
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
source("./Code/Support Functions/For Step 1.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# NOTE: Individual-level data is stored in the "Data/Raw KEEP LOCAL" file
# to comply with the Data Use Agreement (DUA).

# Two datasets were provided by Professor Yusuf Ransome: one in wide format and 
# one in long format. The wide format dataset contains the raw data which has 
# not been evaluated over different date ranges. The long format dataset, on 
# the other hand, summarizes key details.
#
# Although it would be convenient to proceed with the cleaned raw data 
# represented in the long format, this format does not support calculating the 
# date ranges required for the dashboard slider tool.
# 
# For complete data exploration and the reporting that justified the steps taken
# here, please refer to the "./Code/Explore the Raw Data.R" file and the
# corresponding Review page summarizing findings at
# https://ysph-dsde.github.io/church-closures/Pages/Review.html.


# Load the raw dataset in wide format.
church_wide <- read_csv("./Data/Raw/KEEP LOCAL/church_wide_form_071723.csv") %>% as.data.frame()

# Load the evaluation results that identify duplicated ABIs.
duplicates_detected <- read_csv("./Data/Results/KEEP LOCAL/From Explore the Raw Data/ABI Duplicates Test_05.16.2025.csv", 
                                col_types = cols(...1 = col_skip())) %>% as.data.frame()




## ----------------------------------------------------------------
## PART A: Enhancing Function Performance and Efficiency

# Different methods were compared to enhance function speed and scalability, 
# identifying the most efficient approaches for both local computations and 
# high-performance computing (HPC) environments.
#
# The `profvis` package was used to pinpoint code sections that consume the 
# most memory and execution time. The `profvis` function is commented out 
# around the optimized code sections.

search_space <- duplicates_detected$abi

## --------------------
## SUBSECTION A1: Optimizing Data Subsetting

# Convert the 'church_wide' data frame to a data.table for efficient data manipulation
church_wide_dt <- as.data.table(church_wide)

# Benchmark different methods of subsetting the data frame
mb <- microbenchmark(
  # Method 1: Using base R subsetting with %in%
  baseR = { subset_base <- church_wide[church_wide$abi %in% search_space[1],] },
  
  # Method 2: Using dplyr's filter function with %in%
  dplyr_in = { subset_dplyr <- church_wide %>% filter(abi %in% search_space[1]) },
  
  # Method 3: Using dplyr's semi_join function for efficient subsetting
  dplyr_join = { subset_dplyr_join <- church_wide %>% semi_join(data.frame(abi = search_space[1]), by = "abi") },
  
  # Method 4: Using data.table's efficient subsetting with %in%
  data.table = { subset_dt <- church_wide_dt[abi %in% search_space[1]] },
  
  # Number of times to evaluate each expression
  times = 100
)

autoplot(mb)

mb_result <- data.table("expr" = mb$expr, "time" = mb$time)
write.csv(as.data.frame(mb_result), "./Data/Results/From Clean Raw Data/Step 1/Step 1 MB Timings_Subsetting.csv", row.names = FALSE)


# On average, Method 4: Using data.table's efficient subsetting with %in% was
# the most efficient.


## --------------------
## SUBSECTION A2: Optimizing Data Combination

# Create a large sample dataset for testing
set.seed(123)
n <- 1e5
church_wide_test <- data.frame(abi = sample(letters, n, replace = TRUE), value = rnorm(n))
search_space <- sample(letters, 10)

# Dummy 'build' data frame for simulation purposes
build <- church_wide_test[1:1000, ]

# Method 1: Base R rbind in a loop
base_rbind <- function() {
  finish_build <- data.frame()
  for (i in seq_along(search_space)) {
    finish_build <- rbind(finish_build, build)
  }
  return(finish_build)
}

# Method 2: dplyr bind_rows in a loop
dplyr_bind_rows <- function() {
  finish_build <- tibble()
  for (i in seq_along(search_space)) {
    finish_build <- bind_rows(finish_build, build)
  }
  return(finish_build)
}

# Method 3: data.table rbindlist in a loop
data_table_rbindlist <- function() {
  finish_build <- data.table()
  for (i in seq_along(search_space)) {
    finish_build <- rbindlist(list(finish_build, build), use.names = TRUE, fill = TRUE)
  }
  return(finish_build)
}

# Method 4: Accumulate in a list and do.call rbind once
accumulate_list <- function() {
  result_list <- vector("list", length(search_space))
  for (i in seq_along(search_space)) {
    result_list[[i]] <- build
  }
  return(do.call(rbind, result_list))
}

# Method 5: Accumulate in a list and data.table rbindlist once
data_table_accumulate_list <- function() {
  result_list <- vector("list", length(search_space))
  for (i in seq_along(search_space)) {
    result_list[[i]] <- build
  }
  return(rbindlist(result_list, use.names = TRUE, fill = TRUE))
}

# Benchmark the different methods
mb <- microbenchmark(
  base_rbind = base_rbind(),
  dplyr_bind_rows = dplyr_bind_rows(),
  data_table_rbindlist = data_table_rbindlist(),
  accumulate_list = accumulate_list(),
  data_table_accumulate_list = data_table_accumulate_list(),
  times = 10
)

autoplot(mb)

mb_result <- data.table("expr" = mb$expr, "time" = mb$time)
write.csv(as.data.frame(mb_result), "./Data/Results/From Clean Raw Data/Step 1/Step 1 MB Timings_Data Combining.csv", row.names = FALSE)


# On average, Method 5: Accumulate in a list and data.table rbindlist once was
# the most efficient.




## ----------------------------------------------------------------
## PART B: Standardize and Merge Similar Addresses

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


# search_space <- duplicates_detected$abi  # Isolate ABIs that were reduplicated
# church_wide_dt <- as.data.table(church_wide)  # Convert for efficient data manipulation
# finish_build <- vector("list", length(search_space))  # Initialize an empty list
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)  # Initialize progress bar
# 
# #profvis({
# for (i in 1:length(search_space)) {
#   # Subset to show only the entries associated with one reduplicated ABI.
#   subset <- church_wide_dt[abi %in% search_space[i]]
#   
#   # --------------------
#   # Match addresses that are similar for compression.
#   
#   # Fill in empty address lines with "None Given".
#   if (any(is.na(subset$address_line_1))) {
#     subset[is.na(subset$address_line_1), "address_line_1"] <- "None Given"
#   }
#   
#   # Make the entire address elements into one string.
#   compile_address <- str_c(subset$address_line_1, subset$city, subset$state, subset$zipcode, sep = ", ")
#   
#   # Use the support function find_similar_addresses() to compare the addresses
#   # and assign them into groups based on degree of similarity.
#   match <- find_similar_addresses(as.character(compile_address))
#   
#   # --------------------
#   # Reconcile metadata with mismatched outcomes within assigned groups.
#   
#   # The North American Industry Classification System (NAICS) code for 
#   # Religious Organizations is 813110. Any additional characters beyond the 
#   # six-digit code do not have a defined meaning in NAICS and may represent 
#   # something else. We will retain these additional values for reference.
#   #
#   # Source: https://www.census.gov/naics/?input=813110&year=2022&details=813110
#   extra_naics_code <- str_replace(subset$primary_naics_code, "813110", "") %>% unique() %>%
#     (\(x) { str_flatten(x, collapse = ", ") }) ()
#   
#   # Some entries report different years established, even though they are 
#   # associated with the same address. We will retain these differing year values 
#   # for reference.
#   year_est <- subset$year_established %>% unique() %>%
#     (\(x) { x[!is.na(x)] }) () %>% sort() %>%
#     (\(y) { str_flatten(y, collapse = ", ") }) ()
#   
#   # Fill in empty year established lines with None listed".
#   if (length(year_est) == 0) {
#     year_est <- "None listed"
#   }
#   
#   # --------------------
#   # Rebuild the dataframe and remove erroneous reduplicates
#   
#   # Define the starting structure of the metadata that will be used to build the
#   # new dataframe that collapses reduplicates.
#   seed <- data.frame("abi" = unique(subset$abi), "year_established" = year_est,
#                      "primary_naics_code" = 813110, "extra_naics_code" = extra_naics_code,
#                      "naics8_descriptions" = "Religious Organizations")
#   
#   # Stepwise collapse the duplicates.
#   build <- NULL
#   for (j in 1:length(match)) {
#     # Pull out rows that are affiliated with similar addresses.
#     change_these <- sapply(match[[j]], function(x) str_split(x, ", ")[[1]][1]) %>% as.vector() %>%
#       (\(y) { map_lgl(subset$address_line_1, ~ any(str_detect(.x, y))) }) ()
#     
#     # Sum over the openings.
#     dates <- sapply(subset[change_these, 11:31], function(x) sum(x, na.rm = TRUE)) %>%
#       (\(x) { as.data.frame(t(x)) }) ()
#     
#     # Test how similar the longitude and latitude are.
#     negligible_change <- 0.002  # Change in degrees (~222 meters or 728 feet)
#     
#     lonLat_test <- abs(max(subset[change_these, ]$longitude) - min(subset[change_these, ]$longitude)) < negligible_change &
#       abs(max(subset[change_these, ]$latitude) - min(subset[change_these, ]$latitude)) < negligible_change
#     
#     build <- rbind(build, cbind(
#       # Use the ABI as the first column.
#       seed[, 1, drop = FALSE],
#       # Without validating the correct address with the USPS, we'll arbitrarily
#       # choose the first entry. Format the address over different columns.
#       str_split(match[[j]][1], ", ") %>% unlist() %>%
#         (\(x) { as.data.frame(t(x)) }) () %>%
#         `colnames<-`(c("address_line_1", "city", "state", "zipcode")),
#       # Store the compiled address string.
#       as.data.frame(match[[j]][1]) %>% `colnames<-`(c("compiled_address")),
#       # Add the longitudes and latitude values. Add an indicator if the
#       # longitude or latitude were not similar.
#       as.data.frame(lonLat_test) %>% `colnames<-`(c("lonLat_test")),
#       as.data.frame(mean(subset[change_these, ]$latitude)) %>% `colnames<-`(c("latitude")),
#       as.data.frame(mean(subset[change_these, ]$longitude)) %>% `colnames<-`(c("longitude")),
#       # Add the rest of the metadata.
#       seed[, -1, drop = FALSE],
#       # Finally, add the summed dates.
#       dates
#     ))
#   }
#   
#   # Store 'build' in the list.
#   finish_build[[i]] <- build
#   
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# #})
# 
# # Combine all data tables in the list into one data table.
# finish_build <- rbindlist(finish_build, use.names = TRUE, fill = TRUE)
# 
# # Commit results.
# write.csv(finish_build, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B_04.22.2026.csv")

# Read in previously generated results.
finish_build <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B_04.22.2026.csv", 
                         col_types = cols(...1 = col_skip())) %>% as.data.frame()

all(finish_build$abi %in% duplicates_detected$abi) & all(duplicates_detected$abi %in% finish_build$abi)


# After this step, the dataset is collapsed down from almost 3 million rows to
# 1/3 that size.
round(nrow(finish_build)/(church_wide %>% filter(abi %in% duplicates_detected$abi) %>% nrow()) * 100, digits = 2)


## --------------------
## SUBSECTION B1: Verify No Duplicates Got Added

# Some address entries may have been grouped multiple times or mistakenly added 
# as duplicates by the previous function. This likely occurred in the "build" 
# section, where address_line_1 matched unique, similar addresses that differed 
# in other aspects like state, city, or zipcode.
#
# We use the custom function check_all_counts_0_or_1() to identify and extract 
# these instances by performing a column-wise sum over each date of entry and 
# verifying that counts are either zero or one.

# NOTE: Results were already generated and saved. Load them below.


# # Count the number of unique ABIs where duplicates were detected.
# total_groups <- finish_build %>% group_by(abi) %>% n_groups()
# 
# # Initialize progress bar
# pb <- progress_bar$new(
#   format = "  processing [:bar] :percent eta: :eta",
#   total = total_groups,
#   clear = FALSE, width = 60
# )
# 
# test_no_dup <- finish_build %>%
#   # Group the data by ABI to be processed separately.
#   group_by(abi) %>%
#   # Apply the custom function 'check_all_counts_0_or_1' with progress tracking to each group.
#   group_modify(~ process_with_progress(pb, .x, check_all_counts_0_or_1)) %>%
#   # Remove the grouping to return to an ungrouped data frame.
#   ungroup() %>%
#   # Convert the grouped data back to a standard data frame.
#   as.data.frame()
# 
# # Commit results.
# write.csv(test_no_dup, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B1_04.22.2026.csv")


# Read in previously generated results.
test_no_dup <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B1_04.22.2026.csv", 
                        col_types = cols(...1 = col_skip())) %>% as.data.frame()


# Approximately 2.6% of the ABIs examined had issues that resulted in the 
# introduction of duplications where there were none. For details on the test 
# that verifies this, refer to "Code/KEEP LOCAL/Explore the Raw Data.R".
round(table(test_no_dup$all_counts_0_or_1)/nrow(test_no_dup)*100, digits = 2)

# Save all affected ABIs.
dup_added <- test_no_dup[test_no_dup$all_counts_0_or_1 %in% FALSE, "abi"]

# Isolate one example to see how new duplicates were introduced.
finish_build[finish_build$abi %in% dup_added[6], ]


## --------------------
## SUBSECTION B2: Explore the Nature of Duplications

# As shown in "SUBSECTION B1", there were instances where duplicates were 
# erroneously added by the function. Since we are not validating addresses 
# for accuracy yet, we need to confirm that the information in these duplicates 
# is identical. If it is, we may be able to arbitrarily remove one of the 
# duplicates.


# Initialize progress bar
pb <- txtProgressBar(min = 0, max = nrow(finish_build[finish_build$abi %in% dup_added, ]), style = 3)

new_info <- mutate_with_progress(
  # Subset the data to include only ABIs with new duplicates.
  df = finish_build[finish_build$abi %in% dup_added, ],
  # Specify the columns to check for duplications.
  cols_to_convert = grep("^20", names(finish_build), value = TRUE),
  # Define the columns to group by.
  grouping_cols = c("abi", "address_line_1"),
  # Custom function to check for duplicates and set 'is_unique'.
  conversion_func = check_duplicates_unique_info,
  # Progress bar to visually track the conversion process.
  pb = pb
)


# The duplicated function will mark the first occurance as duplicated = FALSE
# and every occurrence of the same result afterwards as duplicated = TRUE.
# Therefore, if our assumption were correct, we'd expect an equal number of 
# TRUE and FALSE in "is_unique", indicating identical information for slightly 
# different addresses. However, this does not appear to be the case.
# 
# NOTE: the NA stands for any addresses that are associated with an ABI where a 
# duplication was detected, but itself was not part of the reduplication 
# captured earlier.

table(new_info$is_unique, useNA = "ifany")


# We can further isolate entries with simple duplications, which occurred 
# because address_line_1 was the same but other address details differed 
# (e.g., city, state, zipcode). The information conveyed between these 
# entries is otherwise identical.

isolate_easy_duplicates <- new_info %>%
  # Group the data by ABI to be processed separately.
  group_by(abi) %>%
  # Summarize the counts of TRUE and FALSE in "is_unique", ignoring NA's.
  summarize(
    count_TRUE = sum(is_unique, na.rm = TRUE),
    count_FALSE = sum(!is_unique, na.rm = TRUE)
  ) %>%
  # Remove the grouping to return to an ungrouped data frame.
  ungroup()

# There is a greater variety of outcomes than expected.
table("TRUE" = isolate_easy_duplicates$count_TRUE, "FALSE" = isolate_easy_duplicates$count_FALSE)


# Entries with balanced or FALSE-majority results (up to 2 TRUE and 2 FALSE,
# or 1 TRUE and 2 FALSE) are not expected to contain anomalies. The majority of 
# entries fall into these categories.

# Confirm that entries with more FALSE duplicate checks are associated with the
# same one address_line_1.
more_false <- isolate_easy_duplicates %>%
  filter(count_TRUE == 1 & count_FALSE == 2) %>%
  pull(abi)

# As expected, we see that variation arises from the city or zip code column.
new_info %>% filter(abi %in% more_false[1:5]) %>% as.data.frame()
church_wide %>% filter(abi %in% more_false[1]) %>% as.data.frame()

# As expected, each ABI has a unique state associated.
new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique)) %>%
  group_by(abi) %>%
  reframe(unique(state)) %>%
  nrow()

# Each reduplicated entry has a unique city associated.
new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique)) %>%
  group_by(abi) %>%
  reframe(unique(city)) %>%
  nrow()

# There appears to be unevenness with associated zip codes.
new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique)) %>%
  group_by(abi) %>%
  reframe(unique(zipcode)) %>%
  nrow()

# Each ABI with an address_line_1 is unique, implying these entries are the
# same address.
new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique) & address_line_1 %!in% "None Given") %>%
  (\(x) {unique(x$address_line_1)} )()

# ABI with no address_line_1 have different cities listed and all failed the
# lonLat_test.
new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique) & address_line_1 %in% "None Given") %>% 
  as.data.frame()

# Interestingly, we see that most of entries with an address failed the 
# longitude/latitude similarity test. Because the address_line_1 is exactly the 
# same, we expect that these address entries are intended to be the same, and
# this was verified earlier.

new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique)) %>%
  (\(x) {table(x$lonLat_test)} )()


# We can now examine entries where duplicates were introduced unexpectedly;
# specifically, cases where more duplicated = TRUE values were detected than 
# anticipated. The goal is to identify businesses where the same address appears 
# across entries with conflicting annual metadata.

more_true <- isolate_easy_duplicates %>%
  filter((count_TRUE == 2 & count_FALSE <= 1) | (count_TRUE == 3 & count_FALSE <= 2)) %>%
  pull(abi)

# This impacts less than 1% of the ABI reported
round((length(more_true)/length(unique(finish_build$abi))) * 100, digits = 2)

# As expected, we see that variation arises from the city or zip code column.
new_info %>% filter(abi %in% more_true[45]) %>% as.data.frame()
church_wide %>% filter(abi %in% more_true[45]) %>% as.data.frame()

# These entries suggest that the similar address clustering algorithm did not
# fully consolidate related addresses; some addresses were correctly grouped
# into the same cluster, while others were not. Based on this, we can reasonably
# assume that duplicate entries sharing the same address_line_1 represent the
# same business and should be treated as a single record.
# 
# From these outcomes, it is apparent that some similar addresses were not
# clustered, thereby avoiding the duplication error and being retained with 
# different information. While complete deduplication of all similar addresses
# is not a requirement for this analysis, there may be an opportunity to further
# collapse similar addresses once they have been validated against a reference.
# Addresses that were not clustered but represent the same location may
# ultimately be associated with the same suggested address upon validation.


# Because some `address_line_1` entries were left blank, we can examine the 
# distribution of longitude and latitude degree differences for these entries 
# and compare them against entries where an address was provided. This analysis 
# is restricted to ABIs where more duplicates than expected were detected, 
# though the insights are generalizable to the overall expected geolocation 
# accuracy across the dataset.

no_address_1 <- new_info %>% filter(abi %in% more_false) %>%
  filter(!is.na(is_unique) & address_line_1 %in% "None Given") %>%
  pull(abi)

# ABIs with an address_line_1
result <- church_wide %>%
  filter(abi %in% more_false[more_false %!in% no_address_1]) %>%
  group_by(abi) %>%
  summarise(
    lon_test    = max(longitude, na.rm = TRUE) - min(longitude, na.rm = TRUE),
    lat_test    = max(latitude,  na.rm = TRUE) - min(latitude,  na.rm = TRUE),
    lonLat_test = lon_test < 0.002 & lat_test < 0.002,
    .groups = "drop"
  ) %>%
  filter(lonLat_test %in% FALSE)

data.frame(
  a = c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max."),
  Longitude  = summary(result$lon_test) %>% as.vector(),
  Latitude = summary(result$lat_test) %>% as.vector(),
  check.names = FALSE
) %>% `names<-`(c("", "Longitude", "Latitude"))

# ABIs with no address_line_1
result2 <- church_wide %>%
  filter(abi %in% no_address_1) %>%
  group_by(abi) %>%
  summarise(
    lon_test    = max(longitude, na.rm = TRUE) - min(longitude, na.rm = TRUE),
    lat_test    = max(latitude,  na.rm = TRUE) - min(latitude,  na.rm = TRUE),
    lonLat_test = lon_test < 0.002 & lat_test < 0.002,
    .groups = "drop"
  ) %>%
  filter(lonLat_test %in% FALSE)

data.frame(
  a = c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max."),
  Longitude  = summary(result2$lon_test) %>% as.vector(),
  Latitude = summary(result2$lat_test) %>% as.vector(),
  check.names = FALSE
) %>% `names<-`(c("", "Longitude", "Latitude"))


# The span of geolocation is similar between businesses with and without an
# address_line_1, though entries lacking an address_line_1 show a slightly
# higher geolocation variance on average. Of the 12 entries that failed the 
# geolocation test, none appear to have a clearly defined address, making it 
# difficult to confidently determine whether they represent the same or distinct 
# locations. As a result, these entries are considered unverifiable and will be 
# flagged for exclusion from the final, cleaned dataset.


# Based on the results above, we will proceed under the assumption that these
# duplicates arose from differing metadata and incomplete clustering of addresses
# that are, in fact, similar. These records will be flagged for consolidation
# into a single entry, retaining any unique information introduced by incomplete
# clustering, pending address verification.

new_info <- new_info %>%
  filter(!is.na(is_unique)) %>% 
  mutate(override_duplicate = TRUE)

finish_build <- finish_build %>%
  mutate(override_duplicate = if (!"override_duplicate" %in% names(.)) NA else override_duplicate) %>%
  left_join(
    new_info %>%
      select(abi, compiled_address, override_duplicate) %>%
      rename(override_duplicate_from_new = override_duplicate),
    by = c("abi", "compiled_address")
  ) %>%
  mutate(override_duplicate = coalesce(override_duplicate, override_duplicate_from_new)) %>%
  select(-override_duplicate_from_new)

# # Commit results.
# write.csv(finish_build, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B2_04.26.2026.csv")

# Read in previously generated results.
finish_build <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection B2_04.26.2026.csv", 
                         col_types = cols(...1 = col_skip())) %>% as.data.frame()


## --------------------
## SUBSECTION B3: Similar Addresses: Geolocation Discrepancies

# The function above includes a quality check to flag combinations of addresses
# that appear similar based on the stringdist() function and the DFS algorithm,
# but are associated with different Longitude and Latitude locations (greater 
# than a 0.002-degree difference). Although we have not yet validated the 
# geolocation data, particularly given the discrepancies identified with PO 
# Boxes, this check provides a reasonable assurance when combining addresses 
# that might be separated due to typographical errors.


# Approximately 15% of the entries failed this test, and less than 1% failed 
# to complete the test.
round(table(finish_build$lonLat_test, useNA = "ifany")/nrow(finish_build)*100, digits = 2)
table(finish_build$lonLat_test, useNA = "ifany")

# All entries that failed the geolocation test have NA values in both the
# Longitude and Latitude fields of the address. We can keep these for now,
# and remove any that do not match with the geolocation referential database.
finish_build %>% 
  filter(is.na(lonLat_test)) %>% 
  select(latitude, longitude) %>% 
  table(useNA = "ifany")

# After the previous step, approximately 120K entries failed the geolocation
# test, implying an error that needs closer inspection. As shown below, however,
# we see that some of these might be correctly identified similar addresses
# and the provided Longitude and Latitude have some error range.

not_same <- finish_build[finish_build$lonLat_test %in% FALSE, ]
same     <- finish_build[finish_build$lonLat_test %!in% FALSE, ]

# Examples
finish_build[finish_build$abi %in% unique(not_same$abi)[2], ]
church_wide[church_wide$abi %in% unique(not_same$abi)[2], ]

finish_build[finish_build$abi %in% unique(not_same$abi)[106353], ]
church_wide[church_wide$abi %in% unique(not_same$abi)[106353], ]

finish_build[finish_build$abi %in% unique(not_same$abi)[10503], ]
church_wide[church_wide$abi %in% unique(not_same$abi)[10503], ]

# To get a sense of how many entries share the exact same address_line_1, the
# number of clusters generated under two conditions will be compared: one using
# relaxed string similarity settings at the same threshold as above, and one
# requiring an exact match.

original_addresses <- church_wide %>%
  filter(abi %in% not_same$abi) %>%
  mutate(compiled_address = paste(address_line_1, city, state, zipcode, sep = ", "))

# Initialize the base R text progress bar
total_abis <- original_addresses %>% distinct(abi) %>% nrow()
pb <- txtProgressBar(min = 0, max = total_abis, style = 3)

# Process each ABI group separately and update the progress bar
count_addresses <- original_addresses %>%
  group_by(abi) %>%
  group_split() %>%
  map2(1:total_abis, ~process_with_progress_txt(pb, .x, process_abi_group, .y)) %>%
  `names<-`(original_addresses %>% distinct(abi) %>% pull())

address_counts <- count_sublists(count_addresses) %>%
  mutate(abi = as.numeric(abi))

# Almost two-third of the failed records involve addresses that are exact 
# duplicates. For these, we will override the test result and mark them as 
# passed. The remaining records will be split back into separate entries and 
# re-evaluated after address validation.
# 
# Once address validation is complete, the following handling steps will be
# implemented:
#   - All records validate to distinct addresses: keep them as separate entries.
#   - Multiple records validate to the same address: override the 
#     longitude/latitude test and merge them into a single record using the 
#     validated address and the average of their latitude/longitude values.

table(address_counts$same_length, useNA = "always")
round((table("Same Number of Clusters" = address_counts$same_length)/length(count_addresses)) * 100, digits = 2)

# Commit the override results
finish_build <- finish_build %>%
  left_join(address_counts, by = "abi") %>%
  rename(same_num_clusters = override_lonLat_test)

# Some entries where different numbers of clusters were detected have opportunities
# to collapse based on exact address matches.
finish_build |> filter(abi %in% address_counts[3, 1])
church_wide |> filter(abi %in% address_counts[3, 1])

# Some of the addresses that failed the geolocation test also generated erroneous
# duplicates due to overlaps between clusters. For these, we will mark that
# a duplication was detected for all expanded values, and reconcile them down
# to one non-duplicated entry after address validation.
table("Clusters" = finish_build$same_num_clusters, "Duplicated" = finish_build$override_duplicate, useNA = "always")




## ----------------------------------------------------------------
## PART C: Resolving Failed Geolocation Tests via Expansion

# The earlier algorithm randomly selected one address to represent each cluster
# of similar addresses. In some cases, exact duplicate addresses may still remain
# and can be further collapsed; these are processed accordingly, with their
# average geolocation retained.

# NOTE: Results were already generated and saved. Load them below.


# search_space <- finish_build %>% # Isolate ABIs that need to be expanded
#   filter(abi %in% names(count_addresses) & same_num_clusters == FALSE) %>%
#   pull(abi) %>%
#   unique()
# 
# # --------------------
# # Manual override for specific cases.
# 
# # Some addresses are known to yield unfavorable string matches. This section
# # applies a manual override to handle them explicitly.
# 
# count_addresses[names(count_addresses) %in% search_space[299]][[1]][["similar"]] <- 
#   count_addresses[names(count_addresses) %in% search_space[299]][[1]][["exact"]] %>%
#   (\(x) c(list(c(x[[1]], x[[2]])), x[c(3, 4)]))()
# 
# # --------------------
# # Continue with the expanding exact addresses.
# 
# supplement_build <- vector("list", length(search_space))  # Initialize an empty list
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)  # Initialize progress bar
# 
# for (i in 1:length(search_space)) {
#   # Expand only unique addresses that failed the longitude/latitude test,
#   # excluding any ABIs approved for a longitude/latitude test override.
#   keep_entries <- finish_build %>%
#     filter(abi %in% search_space[i] & lonLat_test == TRUE) %>%
#     mutate(same_num_clusters = as.character(same_num_clusters))
#   
#   expand_out <- finish_build %>%
#     filter(abi %in% search_space[i] & lonLat_test == FALSE) %>%
#     pull(compiled_address)
#   
#   # Apply the duplication override value to all expanded values so that
#   # the result is retained.
#   override_dup_value <- finish_build %>%
#     filter(abi %in% search_space[i] & lonLat_test == FALSE) %>%
#     pull(override_duplicate)
#   
#   # Save the pattern used to match similar addresses. Addresses with a failed
#   # longitude/latitude test will be collapsed to exact matches only.
#   collapse_pattern <- count_addresses[names(count_addresses) %in% search_space[i]][[1]][["similar"]]
#   
#   # Vector of clusters containing the addresses being expanded.
#   expand_out <- str_replace(expand_out, "None Given", "NA")
#   matches    <- lapply(collapse_pattern, function(x) str_detect(str_c(expand_out, collapse = "|"), x) %>% any()) %>% unlist()
#   
#   # Subset entries that require exact address matching instead of similar matching.
#   subset <- original_addresses %>%
#     filter(abi %in% search_space[i] & compiled_address %in% unlist(collapse_pattern[matches]))
#   
#   
#   # --------------------
#   # Match addresses that are similar for compression.
#   
#   # Fill in empty address lines with "None Given".
#   if (any(is.na(subset$address_line_1))) {
#     subset[is.na(subset$address_line_1), "address_line_1"] <- "None Given"
#   }
#   
#   # Make the entire address elements into one string.
#   compile_address <- subset$compiled_address
#   
#   # Use the support function find_similar_addresses() to compare the addresses
#   # and assign them into groups based on exact similarity.
#   if (length(compile_address) == 0) {
#     stop("compile_address has length 0; cannot determine match.")
#   } else if (length(compile_address) == 1) {
#     match <- compile_address
#   } else { # length > 1
#     match <- find_similar_addresses(as.character(compile_address), threshold = 0)
#   }
#   
#   
#   # --------------------
#   # Reconcile metadata with mismatched outcomes within assigned groups.
#   
#   # Retain the metadata stored from the first attempt at collapsing addresses.
#   extra_naics_code <- finish_build %>% filter(abi %in% search_space[i]) %>% pull(extra_naics_code) %>% .[1]
#   year_est         <- finish_build %>% filter(abi %in% search_space[i]) %>% pull(year_established) %>% .[1]
#   
#   # --------------------
#   # Rebuild the dataframe and remove erroneous reduplicates
#   
#   # Define the starting structure of the metadata that will be used to build the
#   # new dataframe that collapses reduplicates.
#   seed <- data.frame("abi" = unique(subset$abi), "year_established" = year_est,
#                      "primary_naics_code" = 813110, "extra_naics_code" = extra_naics_code,
#                      "naics8_descriptions" = "Religious Organizations")
#   
#   # Stepwise collapse the duplicates.
#   build <- NULL
#   for (j in 1:length(match)) {
#     # Pull out rows that are affiliated with the same addresses.
#     change_these <- match[[j]] %>% as.vector() %>%
#       (\(y) { map_lgl(subset$compiled_address, ~ str_detect(.x, regex(paste0("^", str_trim(y), "(,|$)"), ignore_case = TRUE))) })()
#     
#     # Sum over the openings.
#     dates <- sapply(subset[change_these, 11:31], function(x) sum(x, na.rm = TRUE)) %>%
#       (\(x) { as.data.frame(t(x)) }) ()
#     
#     # Test how similar the longitude and latitude are.
#     negligible_change <- 0.002  # Change in degrees (~222 meters or 728 feet)
#     
#     lonLat_test <- if (nrow(subset[change_these, ]) == 1) {
#       NA
#     } else {
#       abs(max(subset[change_these, ]$longitude) - min(subset[change_these, ]$longitude)) < negligible_change &
#         abs(max(subset[change_these, ]$latitude)  - min(subset[change_these, ]$latitude))  < negligible_change
#     }
#     
#     build <- rbind(build, cbind(
#       # Use the ABI as the first column.
#       seed[, 1, drop = FALSE],
#       # Without validating the correct address with the USPS, we'll arbitrarily
#       # choose the first entry. Format the address over different columns.
#       str_split(match[[j]][1], ", ") %>% unlist() %>%
#         (\(x) { as.data.frame(t(x)) }) () %>%
#         `colnames<-`(c("address_line_1", "city", "state", "zipcode")),
#       # Store the compiled address string.
#       as.data.frame(match[[j]][1]) %>% `colnames<-`(c("compiled_address")),
#       # Add the longitudes and latitude values. Add an indicator if the
#       # longitude or latitude were not similar.
#       as.data.frame(lonLat_test) %>% `colnames<-`(c("lonLat_test")),
#       as.data.frame(mean(subset[change_these, ]$latitude)) %>% `colnames<-`(c("latitude")),
#       as.data.frame(mean(subset[change_these, ]$longitude)) %>% `colnames<-`(c("longitude")),
#       # Add the rest of the metadata.
#       seed[, -1, drop = FALSE],
#       # Finally, add the summed dates.
#       dates,
#       data.frame("override_duplicate" = override_dup_value)
#     ))
#   }
#   
#   # Store 'build' in the list.
#   supplement_build[[i]] <- build %>%
#     mutate(
#       zipcode = as.double(zipcode),
#       same_num_clusters = "Expanded"
#     ) %>%
#     bind_rows(keep_entries)
#   
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# 
# # Combine all data tables in the list into one data table.
# supplement_build <- rbindlist(supplement_build, use.names = TRUE, fill = TRUE)
# 
# # Commit results.
# write.csv(supplement_build, file = "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection C1_04.27.2026.csv")


# Read in previously generated results.
expanded <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1/Step 1 Subsection C1_04.27.2026.csv", 
                     col_types = cols(...1 = col_skip())) %>% as.data.frame()


## --------------------
## SUBSECTION C1: Combine All Results

# Verify that all ABI marked for expansion were processed. Output should be 
# TRUE if the test passes.
expand_these <- filter(finish_build, same_num_clusters == FALSE) %>% pull(abi)
all(expanded$abi %in% expand_these) & all(expand_these %in% expanded$abi) == TRUE

# Combine the expanded version back into the main dataset.
df_duplicates <- bind_rows(
  finish_build %>%
    # Exclude ABI that were expanded in this section.
    filter(!(abi %in% expanded$abi)) %>%
    mutate(same_num_clusters = as.character(same_num_clusters)),
  expanded %>%
    mutate(same_num_clusters = as.character(same_num_clusters))
)

# Verify that all ABI have been accounted for. Output should be TRUE if the 
# test passes.
all(df_duplicates$abi %in% finish_build$abi) & all(finish_build$abi %in% df_duplicates$abi) == TRUE

# For clarity, the cluster matching check will be renamed to a Boolean flag
# indicating whether an entry's geolocation should be overridden. This flag
# also incorporates the results identifying which entries were expanded.
df_duplicates <- df_duplicates %>%
  rename(override_lonLat_test = same_num_clusters)




## ----------------------------------------------------------------
## PART D: Cleaning for Saving the Result

# Save the part of the raw data where there are no duplicates.
no_duplicates <- church_wide %>% 
  (\(x) { x[x$abi %!in% duplicates_detected$abi, ] }) () %>% 
  `rownames<-`(NULL)


## --------------------
## SUBSECTION D1: Add Missing Columns

# The deduplicated records will be merged back with the portion of the dataset
# that contained no duplicates. Before merging, the columns of each subset are
# compared to identify and resolve any discrepancies.
colnames(df_duplicates) %>% .[. %!in% colnames(no_duplicates)]
colnames(no_duplicates) %>% .[. %!in% colnames(df_duplicates)]

# Add the missing components by unique ABI.
no_duplicates <- no_duplicates %>%
  group_by(abi) %>%
  mutate(compiled_address = str_c(address_line_1, city, state, zipcode, sep = ", "),
         lonLat_test = NA,
         # Extract the extra numerics added to the end of the NAICS code.
         extra_naics_code = str_replace(primary_naics_code, "813110", "") %>% unique() %>%
           (\(x) { str_flatten(x, collapse = ", ") }) (),
         override_duplicate = NA,
         same_num_clusters = NA,
         primary_naics_code = 813110,
         naics8_descriptions = "Religious Organizations") %>%
  ungroup() %>%
  select(colnames(df_duplicates)) %>%
  as.data.frame()


## --------------------
## SUBSECTION D2: Merge Duplicate and Singular Record Datasets

# Inspect column classes to ensure they correspond
cbind(as.data.frame(sapply(no_duplicates, class)), as.data.frame(sapply(df_duplicates, class)))

no_duplicates <- no_duplicates %>%
  mutate(zipcode = as.character(zipcode), year_established = as.character(year_established),
         same_num_clusters = as.character(same_num_clusters))

df_duplicates <- df_duplicates %>%
  mutate(zipcode = as.character(zipcode))

# Add the duplicated and singlet datasets together again.
step_1 <- bind_rows(no_duplicates, df_duplicates)


## --------------------
## SUBSECTION D3: Remove Businesses with No Recorded Physical Address

# Review how many entries this effects
step_1 %>%
  filter(is.na(address_line_1) | address_line_1 %in% "None Given") %>%
  summarise(
    "Total Entries" = n(),
    "Total Perc." = round(`Total Entries`/nrow(church_wide)*100, digits = 2),
    "Businesses" = n_distinct(abi),
    "ABI Perc." = round(Businesses/length(unique(church_wide$abi))*100, digits = 2)
  )

no_address_abi <- step_1 %>%
  filter(is.na(address_line_1) | address_line_1 %in% "None Given") %>%
  pull(abi) %>%
  unique()

# Remove businesses that at one point did not list a physical address.
step_1 <- step_1 %>%
  filter(abi %!in% no_address_abi)


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










