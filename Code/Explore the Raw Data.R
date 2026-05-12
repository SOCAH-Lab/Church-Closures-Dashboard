## ----------------------------------------------------------------
## Explore and test the validity of data assumptions.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: April 8th, 2026
## 
## Description: This script documents tests of various data assumptions and
##              evaluates the results in the context of the cleaning and
##              analytical methods previously employed by Dr. Insang Song.
##              The findings are used to justify modifications to the data
##              cleaning, harmonization, and metric calculation processes.
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
##    - OPPORTUNITIES TO USE THE LONG FORMAT
##    - AMERICAN BUSINESS IDENTIFIER (ABI) REDUPLICATES
##        * Identify Source of Reduplicated Entries
##        * Source of Reduplicated Entries - Assumptions #1
##        * Source of Reduplicated Entries - Assumptions #2
##        * Source of Reduplicated Entries - Assumptions #3
##        * Source of Reduplicated Entries - Assumptions #4
##        * Source of Reduplicated Entries - Assumptions #5
##        * Source of Reduplicated Entries - Assumptions #6
##        * Source of Reduplicated Entries - Assumptions #7
## 
##    - ASSUMPTION TO REMOVE PO BOX'S
##    - STATES REPRESENTED
## 
## NOTE: The Data Use Agreements (DUAs) with the data owner, Data Axle, prohibit
##       public distribution of the raw data. Accordingly, individual-level files
##       are stored in "~/KEEP LOCAL" directories. While most local-only files
##       are individual-level data, some code or results may also be restricted.
##
##       All publicly distributed results are summarized, and all publicly
##       distributed code has been constructed to avoid directly revealing or
##       referencing individual-level results. Executing all code below requires
##       access to the raw data and results.
## 
##       API keys are user-specific and are not publicly distributed. Where
##       applicable, instructions have been provided to help users obtain their
##       own API keys.


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
  library("data.table")       # High-performance data manipulation
  library("lubridate")        # Manipulate dates
  library("stringdist")       # Measuring string distances
  library("future.apply")     # Parallel processing
  library("progress")         # Progress bars
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For EDA.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Two files were provided by Professor Yusuf Ransome, a wide and long format. 
# The wide is the raw data that has not been evaluated over different date 
# ranges while the long format summarizes salient details.

church_wide <- read_csv("Data/Raw/KEEP LOCAL/church_wide_form_071723.csv") %>% as.data.frame()
church_long <- read_csv("Data/Raw/KEEP LOCAL/church_2001_2021_long_cleaned.csv") %>% as.data.frame()

data00 <- read_csv("Data/Raw/KEEP LOCAL/closing_all_possible_periods_county00.csv.gz")
data10 <- read_csv("Data/Raw/KEEP LOCAL/closing_all_possible_periods_county10.csv.gz")

uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.90/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)




## ----------------------------------------------------------------
## OPPORTUNITIES TO USE THE LONG FORMAT

# Data description provided by Insang can be found in:
# data_cleaning_and_relocation_history_04142023.docx

glimpse(church_wide)
glimpse(church_long)


# Notice that "year_first/last" in the long format represents the years that 
# church (identified by its American Business Identifier or "abi") was first 
# opened and closed within the 2001 and 2021 date span. Therefore, the
# metrics only encompass the occurrences within the complete time span.

# There are no reduplicated ABI entries.
duplicated(church_long$abi) %>% table()

# The cleaning description states that PO Boxes were removed early in the
# process, and were not considered in the processing. As shown below, some
# PO Boxes were the first address listed, thus disrupting the metric
# reported for some of the data. About 12% of the unique ABI's at one point
# used a PO Box.

# This justifies re-doing the data cleaning starting with the wide format,
# and saving this intermediary format. This supports further research using
# the data without loss of information. Going forward we will only use
# the "church_wide" format.




## ----------------------------------------------------------------
## AMERICAN BUSINESS IDENTIFIER (ABI) REDUPLICATES


# There are no NA values associated with an ABI.
anyNA(church_wide$abi)

# All values are numeric, and we test the code length.
class(church_wide$abi)
church_wide$abi %>% str_length() %>% table()

# A significant number of entries are reduplicated. Examining some reduplicated 
# entries imply reduplications are a result of minor variation in the listed 
# address. The date range binaries for these entries appear to be mutually 
# exclusive, supporting the idea that each entry is important to retain to a 
# degree.
duplicated(church_wide$abi) %>% table()

# We examine the reduplications in the following ways:
#     1. Confirm that the date binaries are mutually exclusive, indicating that 
#        each recorded entry provides unique information.
#     2. Confirm that the ZIP code remains consistent across entries.
#     3. Verify that the following metadata fields are consistent across entries:
#        `year_established`, `state`, `city`, `primary_naics_code`, and
#        `naics8_descriptions`.
#     4. Confirm that the longitude and latitude coordinates fall within 
#        acceptable error margins for a given business address.

# The following code will calculate this, but it takes a while to run. The
# results have also been saved so you can skip the function and load in those
# pre-produced results.

# search_space <- church_wide$abi[duplicated(church_wide$abi)] %>% unique()
# 
# # Add a progress bar to show where the function is in the for loop.
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)
# 
# result <- NULL
# for(i in 1:length(search_space)) {
#   # Subset to show only the entries associated with one reduplicated ABI.
#   subset <- church_wide[church_wide$abi %in% search_space[i], ]
# 
#   # 1. Confirm the date binaries are mutually exclusive. A passing result will
#   #    say "TRUE".
#   test_1 <- sapply(subset[, 11:31], function(x) sum(x, na.rm = TRUE)) %!in% c(0, 1) %>% any() == FALSE
# 
# 
#   # 2. Confirm the other metadata are consistent. This is excluding the
#   #    longitude and latitude values.
# 
#   # Same zip code? A passing result will say "TRUE".
#   test_2a <- subset$zipcode %>% unique() %>% length() == 1
# 
#   # All reduplicated entries metadata are same? A passing result will say "TRUE".
#   test_2b <- subset[, c("year_established", "state", "city", "primary_naics_code", "naics8_descriptions")] %>%
#     unique() %>% nrow() == 1
# 
# 
#   # 3. Confirm the longitude and latitude are within error of each other. A
#   #    passing result will say "TRUE".
#   test_3 <- max(subset$longitude) - min(subset$longitude) < 1 & max(subset$latitude) - min(subset$latitude) < 1
# 
#   result <- rbind(result, cbind(search_space[i], test_1, test_2a, test_2b, test_3))
# 
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# 
# # Commit result with reformatting.
# result <- result %>% as.data.frame() %>%
#   `colnames<-`(c("abi", "Exclusive", "Zip_Same", "Metadata_Same", "LonLat_Similar"))
# 
# # Convert results from binary back to logical.
# result[, -1] <- apply(result[, -1], 2, function(x) as.logical(x))


#' @description Codebook for the output fields produced by the evaluation.
#' 
#' @field abi Unique business identifier over which the evaluation is performed.
#'                        
#' @field Exclusive Boolean. TRUE if date columns sum to 0 or 1, indicating
#'                  that the information provided by each is mutually exclusive.
#'              
#' @field Zip_Same Boolean. TRUE if the zip code is consistent across all 
#'                 addresses associated with a given business ID.
#'                        
#' @field Metadata_Same Boolean. TRUE if the following metadata fields are
#'                      consistent across all addresses associated with a given
#'                      business ID: year_established, state, city,
#'                      primary_naics_code, and naics8_descriptions.
#'                        
#' @field LonLat_Similar Boolean. TRUE if the difference between the minimum
#'                       and maximum longitude AND latitude values are each
#'                       less than 1 degree.

# # Save the result.
# write.csv(result, "Data/Results/KEEP LOCAL/From Explore the Raw Data/ABI Duplicates Test_05.16.2025.csv")

# Load in the pre-produced test results for evaluation.
result <- read_csv("Data/Results/KEEP LOCAL/From Explore the Raw Data/ABI Duplicates Test_05.16.2025.csv", 
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()




## --------------------
## Identify Source of Reduplicated Entries

# Let's examine the results. First, lets confirm that all reduplicates report
# mutually exclusive records (sum to 0 or 1). If this is the case, than each 
# entry should come up a "TRUE".
table(result$Exclusive)

# To appropriately evaluate the remaining three conditions listed above, we need 
# to differentiate variations resulting from explainable sources, such as moves, 
# or errors in data reporting. The primary suspicion is that the detected 
# reduplications arise from alternative addresses associated with the ABIs over
# a 20-year span.
# 
# We expect the following results if certain assumptions are confirmed:
#   1. LonLat_Similar, Zip_Same, Metadata_Same = TRUE: The church has not moved, 
#      no alternative address outside of the reported zip code was used (e.g., 
#      PO Box), and the other metadata did not change. This implies that the 
#      only variation detected comes from the `address_line_1` entry, and no 
#      special considerations are required.
# 
#   2. LonLat_Similar, Zip_Same = TRUE and Metadata_Same = FALSE: The church has 
#      not significantly moved, which implies that the observed variation is 
#      driven by a field other than address_line_1 (e.g., year_established, 
#      state, city, primary_naics_code, or naics8_descriptions). These 
#      discrepancies may reflect data entry errors or the recording of 
#      alternative city locations rather than an actual relocation. Further 
#      investigation will ensure completeness.
# 
#   3. LonLat_Similar, Metadata_Same = TRUE and Zip_Same = FALSE: This issue 
#      can be attributed to two explainable scenarios: either the move was small 
#      but resulted in a new zip code, or one of the addresses used a PO Box 
#      with a different zip code but the same latitude and longitude as a nearby 
#      address. Additionally, there might have been a typographical error in the 
#      zip code.
# 
#   4. LonLat_Similar = TRUE and Zip_Same, Metadata_Same = FALSE: This 
#      discrepancy may also be attributable to small relocations that go 
#      undetected by the geolocation change threshold yet produce a change in 
#      city, zip code, or state for border locations. Variation in the recorded 
#      year established may similarly contribute. Both sources of discrepancy 
#      will be further investigated and are expected to be addressed in the same 
#      manner as the second or third assumption combinations.
# 
#   5. LonLat_Similar = FALSE, and Zip_Same and Metadata = TRUE: It is unlikely 
#      that there would be a significant move without a change in the zip code 
#      and other metadata, such as the city or state. It is also possible that 
#      non-physical addresses, like PO Boxes, are contributing to this outcome. 
#      These are rare occurrences and will be investigated individually to 
#      assess for typographical errors.
# 
#   6. LonLat_Similar, Zip_Same, Metadata = FALSE: These entries are most likely 
#      associated with a significant move out of the area and will be treated 
#      in the same manner as the fifth combination of assumptions. Their zip 
#      codes and other metadata might also contain typographical errors that 
#      will need to be assessed for completeness.
# 
#   7. LonLat_Similar, Metadata = FALSE and Zip_Same = TRUE OR LonLat_Similar, 
#      Zip_Same = FALSE and Metadata = TRUE: These results suggest significant 
#      relocations with no change in zip code or other metadata. Though a stable 
#      zip code is plausible, no change across any address field is unusual. 
#      These rare cases will be individually reviewed for typographical errors.
#
#
# The results of this test will be used to identify the dimensions of the raw 
# data, specifically to highlight different conditions that need consideration 
# and opportunities for data cleaning or validation.


# Summary table highlighting the results:
table("Metadata" = result$Metadata_Same, "Zip Code" = result$Zip_Same, "Long/Lat" = result$LonLat_Similar)
round((table("Metadata" = result$Metadata_Same, "Zip Code" = result$Zip_Same, "Long/Lat" = result$LonLat_Similar)/nrow(result))*100, 2)

# Total entries associated with a PO Box
poBox_all <- church_wide[str_which(church_wide$address_line_1, "(?i)PO Box|P O Box"), ]

# About 12% of the businesses represented have a PO Box address at some point
# in their history.
round(length(unique(poBox_all$abi))/length(unique(church_wide$abi))*100, digits = 2)




## --------------------
## Source of Reduplicated Entries - Assumptions #1

subset_1 <- result[result$Zip_Same %in% TRUE & result$LonLat_Similar %in% TRUE & result$Metadata_Same %in% TRUE, ] %>%
  `rownames<-`(NULL)

church_1 <- church_wide[church_wide$abi %in% subset_1$abi, ]

# Just over half of the results fit the first set of assumptions.
round(nrow(subset_1)/nrow(result)*100, digits = 2)

# Almost 1/3rd of all ABI's are represented in these assumptions.
round(length(unique(church_1$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_1  <- church_1[str_which(church_1$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 15% used a PO Box at some point, representing almost 5% of all ABIs and 
# 39% of all PO Box entries.
round(length(unique(poBox_1$abi))/length(unique(church_wide$abi))*100, digits = 2)
round(length(unique(poBox_1$abi))/length(unique(subset_1$abi))*100, digits = 2)

round(length(unique(poBox_1$abi))/length(unique(poBox_all$abi))*100, digits = 2)




## --------------------
## Source of Reduplicated Entries - Assumptions #2

subset_2 <- result[result$Zip_Same %in% TRUE & result$LonLat_Similar %in% TRUE & result$Metadata_Same %in% FALSE, ] %>%
  `rownames<-`(NULL)

church_2 <- church_wide[church_wide$abi %in% subset_2$abi, ]

# About 1/3 of the results fit the second set of assumptions.
round(nrow(subset_2)/nrow(result)*100, digits = 2)

# Almost 1/5th of all ABI's are represented in these assumptions.
round(length(unique(church_2$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_2  <- church_2[str_which(church_2$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 12% used a PO Box at some point, representing almost 3% of all ABIs and 
# 19% of all PO Box entries.
round(length(unique(poBox_2$abi))/length(unique(church_wide$abi))*100, digits = 2)
round(length(unique(poBox_2$abi))/length(unique(subset_2$abi))*100, digits = 2)

round(length(unique(poBox_2$abi))/length(unique(poBox_all$abi))*100, digits = 2)


# We now want to find out what metadata caused variations, when it appears to
# meet the first condition otherwise. We first see that the "naics8_descriptions"
# variable cannot be the source of any variation, and so it will be ignored.
# The code, however, does vary, notably by the last two digits in the code.
church_wide$primary_naics_code %>% unique()
church_wide$naics8_descriptions %>% unique()


# The following is commented out since it take a while to run. Load in the 
# pre-run results to review.

# search_space <- subset_2$abi
# 
# # Add a progress bar to show where the function is in the for loop.
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)
# 
# result2 <- NULL
# for(i in 1:length(search_space)) {
#   # Subset to show only the entries associated with one reduplicated ABI.
#   subset <- church_2[church_2$abi %in% search_space[i], ]
# 
#   year_unique  <- length(unique(subset$year_established)) == 1
#   state_unique <- length(unique(subset$state)) == 1
#   city_unique  <- length(unique(subset$city)) == 1
#   naics_unique <- length(unique(subset$primary_naics_code)) == 1
# 
#   result2 <- rbind(result2, cbind(search_space[i], year_unique, state_unique, city_unique, naics_unique))
# 
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# # Commit result with reformatting.
# result2 <- result2 %>% as.data.frame() %>%
#   `colnames<-`(c("abi", "Year", "State", "City", "NAICS"))
# 
# # Convert results from binary back to logical.
# result2[, -1] <- apply(result2[, -1], 2, function(x) as.logical(x))


#' @description Codebook for the output fields produced by the evaluation.
#'
#' @field abi Unique business identifier. Evaluation is performed over each
#'            unique business ID in the subset that meets the second set of
#'            reduplication assumptions.
#'
#' @field Year Boolean. TRUE if all addresses associated with the business ID
#'             share a single unique value for year_established.
#'
#' @field State Boolean. TRUE if all addresses associated with the business ID
#'              share a single unique value for state.
#'
#' @field City Boolean. TRUE if all addresses associated with the business ID
#'             share a single unique value for city.
#'
#' @field NAICS Boolean. TRUE if all addresses associated with the business ID
#'              share a single unique value for primary_naics_code.

# # Save the result.
# write.csv(result2, "Data/Results/KEEP LOCAL/From Explore the Raw Data/Metadata that Should Be the Same_04.10.2026.csv")

# Load in the pre-produced test results for evaluation.
result2 <- read_csv("Data/Results/KEEP LOCAL/From Explore the Raw Data/Metadata that Should Be the Same_04.10.2026.csv", 
                    col_types = cols(...1 = col_skip())) %>% as.data.frame()

# City are the rows, NAICS the columns, and Year Founded are the split tables.
table("City" = result2$City, "NAICS Code" = result2$NAICS, "Year Established" = result2$Year)
round((table("City" = result2$City, "NAICS Code" = result2$NAICS, "Year Established" = result2$Year)/nrow(result2))*100, 2)


# We see that most (about 70%) of the results only vary with the "Year Established"
# reporting. This is not an important variable, and so its source of variation
# will not be considered further, only noted.
# 
# Another potential source of variation is the North American Industry 
# Classification System (NAICS) code. Based on searches into standard formatting
# and nomenclature, the 8 numeric long variations do not match the expected
# formatting. Only the leading 6 numbers, 813110, are recognized as a "Religious
# Organizations": https://www.census.gov/naics/?input=813110&year=2022&details=813110
# 
# Notice that this includes different types of religions and organization affiliation
# (i.e. convents, shrines, temples, etc.). It does not include religiously affiliated
# places of education, formal medical care facilities, publishing, or broadcasting
# institutions. The following two digits have some other non-NAICS meaning,
# and need to be handled differently. For our purposes, we will simply save
# these in a separate column and note the variation; they do not appear to be
# indicating a church location change or closure.
str_replace(church_wide$primary_naics_code, "\\b813110", "") %>% unique() %>% sort()

# The trailing two digits vary from "01" to "43".
str_replace(unique(church_wide$primary_naics_code), "813110", "") %>% sort()

# This leaves variation with the city. This is unexpected, since the latitude
# and longitude do not differ between replicated entries. We will examine the
# source of these problems, more closely. Likely they are due to changes in
# the address such as through use of a PO Box, variations in city designation, 
# or alternative naming specific to a given zip code, and do not constitute a 
# "move" event.
table("City" = result2$City)

# The following is commented out since it take a while to run. Load in the 
# pre-run results to review.

# search_space <- result2[result2$City == FALSE, "abi"]
# 
# # Add a progress bar to show where the function is in the for loop.
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)
# 
# result3 <- NULL
# for(i in 1:length(search_space)) {
#   # Subset to show only the entries associated with one reduplicated ABI.
#   subset <- church_2[church_2$abi %in% search_space[i], ]
# 
#   # Preferred city for that entry.
#   zip_code <- unique(subset$zipcode)
# 
#   # Some of the entered zip codes are invalid. In most cases, it seems a leading
#   # or trailing zero was erroneously removed.
#   if(str_length(zip_code) != 5) {
#     listed_city <- str_c("Invalid Zip Code: ", zip_code)
# 
#   # If the zip code is valid, then check for the preferred city name.
#   } else if(str_length(zip_code) == 5) {
#     query_result <- get_city_info(unique(subset$zipcode), zip_city_lookup)
# 
#     # Sometimes that query might not result in a match to the API database.
#     if(is.null(query_result) ) {
#       listed_city <- str_c("Invalid Zip Code: ", zip_code)
#     } else {
#       listed_city <- query_result
#     }
#   }
# 
#   # Number of entries total.
#   num_entries <- nrow(subset)
# 
#   # Variation due to a PO Box?
#   any_poBox   <- length(str_which(subset$address_line_1, "(?i)PO Box|P O Box")) != 0
# 
#   # Report the cities that are similar to other entries, match within some
#   # threshold, and compare with those that are uniquely reported.
#   similar_entries <- find_similar_addresses(subset$city) %>% unlist() %>% unique() %>%
#     (\(x) { str_flatten(x, collapse = ", ") }) ()
# 
#   unique_entries  <- find_similar_addresses(subset$city, threshold = 0) %>% unlist() %>%
#     unique() %>% (\(x) { str_flatten(x, collapse = ", ") }) ()
# 
#   result3 <- rbind(result3, cbind(search_space[i], listed_city, num_entries, any_poBox, similar_entries, unique_entries))
# 
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# # Commit result with reformatting.
# result3  <- result3 %>% as.data.frame() %>%
#    `colnames<-`(c("abi", "Preferred City Name", "# Entries", "PO Box?", "Similar Names", "Unique Names"))
# 
# Calculate summary metrics to facilitate assessment of the results.
# result3 <- result3 %>%
#   mutate(
#     # Calculate the difference between the number of unique cities and the number
#     # of cities with minor typographical differences (i.e. similar).
#     c1 = str_split(`Unique Names`, "\\s*,\\s*"),
#     c2 = str_split(`Similar Names`, "\\s*,\\s*"),
#     `Unique-Similar` = map2_int(c1, c2, ~ length(.x) - length(.y)),
#     # Count the number of unique suggested cities.
#     c3 = if_else(
#       str_detect(`Preferred City Name`, "Invalid Zip Code:|No Matches Found:"),
#       list(NA_character_),
#       str_split(`Preferred City Name`, "\\s*,\\s*")
#     ),
#     `# Cities Suggested` = map_int(
#       c3,
#       \(x) {
#         if (is.null(x)) return(NA_integer_)
#         x <- as.character(x)
#         x <- str_trim(x)
#         x <- x[x != ""]
#         if (length(x) == 0 || (length(x) == 1 && is.na(x))) NA_integer_ else length(x)
#       }
#     ),
#     # Calculate the difference between the number of unique cities and the number
#     # of cities suggested as preferred.
#     `Unique-Preferred` = map2_int(
#       c1,
#       `# Cities Suggested`,
#       \(x, y) {
#         if (anyNA(x) || anyNA(y)) return(NA_integer_)
#         length(x) - length(y)
#       }
#     ),
#     # Verify whether the preferred address is present in the unique address vector.
#     `Preferred in Unique?` = if_else(
#       str_detect(`Preferred City Name`, "Invalid Zip Code:|No Matches Found:"),
#       NA,
#       map2_lgl(`Unique Names`, `Preferred City Name`, ~ str_detect(.x, fixed(.y)))
#     )
#   ) %>%
#   select(-c1, -c2, -c3)


#' @description Codebook for the output fields produced by the evaluation.
#' 
#' @field abi Unique business identifier. Evaluation is performed over each 
#'            unique business ID in the subset that meets the second set of 
#'            reduplication assumptions and where the city similarity evaluation 
#'            did not pass.
#'
#' @field `Preferred City Name` Preferred address matched to the business using
#'                              the zip code and the simplemaps database.
#'
#' @field `# Entries` Number of rows, or possible addresses, associated with 
#'                    each business ID.
#'
#' @field `PO Box?` Boolean. TRUE if any address associated with the business 
#'                  ID corresponds to a PO Box.
#'
#' @field `Similar Names` City names in the dataset grouped into clusters of 
#'                        similar names by string similarity.
#'
#' @field `Unique Names` Distinct city name outcomes present in the dataset 
#'                       after grouping.
#'
#' @field `Unique-Similar` Difference between the number of unique city names 
#'                         and the number of unique similar city name clusters.
#'
#' @field `# Cities Suggested` Total number of city matches suggested by the
#'                             simplemaps database for the associated zip code.
#'
#' @field `Preferred in Unique?` Boolean. TRUE if the preferred city name
#'                               returned by the simplemaps database is already
#'                               present in the dataset.
#'
#' @field `Unique-Preferred` Difference between the number of unique city names 
#'                           and the number of preferred city names.

# # Save the result.
# write.csv(result3, "Data/Results/KEEP LOCAL/From Explore the Raw Data/City Name Variation_04.10.2026.csv")

# Load in the pre-produced test results for evaluation.
result3 <- read_csv("Data/Results/KEEP LOCAL/From Explore the Raw Data/City Name Variation_04.10.2026.csv", 
                    col_types = cols(...1 = col_skip())) %>% as.data.frame()


# 6.5% of businesses have invalid zip codes associated with them.
round(table(str_detect(result3$`Preferred City Name`, "Invalid"), useNA = "ifany") / nrow(result3) * 100, digits = 1)

# 1.7% of businesses have valid zip codes but there was no match found.
round(table(str_detect(result3$`Preferred City Name`, "No Matches"), useNA = "ifany") / nrow(result3) * 100, digits = 1)

# 6.7% of businesses have city variations that involve a PO Box.
round(table(result3$`PO Box?`, useNA = "ifany") / nrow(result3) * 100, digits = 1)

# No typographical errors were detected with the recorded cities.
table(result3$`Unique-Similar`, useNA = "ifany")

# The algorithm lists all preferred cities suggested for a given five-digit zip
# code; all results returned exactly one preferred city.
table(result3$`# Cities Suggested`, useNA = "ifany")

# As a sanity check, all NA values are confirmed to be associated with invalid
# or unmatched zip codes.
table("Invalid" = str_detect(result3$`Preferred City Name`, "Invalid"), 
      "No Matches" = str_detect(result3$`Preferred City Name`, "No Matches"),
      "Preferred" = result3$`# Cities Suggested`, useNA = "ifany")

# 68% of businesses had a potential match, while nearly 25% did not contain the
# preferred city. All NA values correspond to entries where the zip code was 
# invalid or no match was found.
round(table(result3$`Preferred in Unique?`, useNA = "ifany") / nrow(result3) * 100, digits = 1)

# As a sanity check, all NA values are confirmed to be associated with invalid
# or unmatched zip codes. We see a greater number of unique addresses are
# associated with a higher likelihood of a preferred match being returned.
table("Unique-Preferred" = result3$`Unique-Preferred`, 
      "Preferred in Unique?" = result3$`Preferred in Unique?`)

# There does not appear to be a strong trend indicating that PO Box addresses
# are disproportionately associated with the absence of a preferred city.
round(table("PO Box?" = result3$`PO Box?`, 
      "Preferred in Unique?" = result3$`Preferred in Unique?`) / nrow(result3) * 100, digits = 1)


# These evaluations indicate inconsistencies in the address entries that warrant
# validation before they can be considered accurate. Accurate addresses are
# expected to be more readily matched in the reference database used for
# geolocation.




## --------------------
## Source of Reduplicated Entries - Assumptions #3

subset_3 <- result[result$Zip_Same %in% FALSE & result$LonLat_Similar %in% TRUE & result$Metadata_Same %in% TRUE, ] %>%
  `rownames<-`(NULL)

church_3 <- church_wide[church_wide$abi %in% subset_3$abi, ]

# Almost 5% of the results fit the third set of assumptions.
round(nrow(subset_3)/nrow(result)*100, digits = 2)

# Almost 3% of all ABI's are represented in these assumptions.
round(length(unique(church_3$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_3  <- church_3[str_which(church_3$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 48% used a PO Box at some point, representing almost 1.4% of all ABIs and 
# 12% of all PO Box entries.
round(length(unique(poBox_3$abi))/length(unique(church_wide$abi))*100, digits = 2)
round(length(unique(poBox_3$abi))/length(unique(subset_3$abi))*100, digits = 2)

round(length(unique(poBox_3$abi))/length(unique(poBox_all$abi))*100, digits = 2)

## Church in place but zip code changed. Could be a result of the PO Box's
church_wide[church_wide$abi %in% 9770140, ]

subset = church_3[240:245,]
compile_address <- str_c(subset$address_line_1, subset$city, subset$state, subset$zipcode, sep = ", ")
validate_geocode_nominatim(compile_address[4])




## --------------------
## Source of Reduplicated Entries - Assumptions #4

subset_4 <- result[result$Zip_Same %in% FALSE & result$LonLat_Similar %in% TRUE & result$Metadata_Same %in% FALSE, ] %>%
  `rownames<-`(NULL)

church_4 <- church_wide[church_wide$abi %in% subset_4$abi, ]

# Almost 9% of the results fit the third set of assumptions.
round(nrow(subset_4)/nrow(result)*100, digits = 2)

# Almost 5.2% of all ABI's are represented in these assumptions.
round(length(unique(church_4$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_4  <- church_4[str_which(church_4$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 36% used a PO Box at some point, representing almost 1.9% of all ABIs and 
# 15% of all PO Box entries.
round(length(unique(poBox_4$abi))/length(unique(church_wide$abi))*100, digits = 2)
round(length(unique(poBox_4$abi))/length(unique(subset_4$abi))*100, digits = 2)

round(length(unique(poBox_4$abi))/length(unique(poBox_all$abi))*100, digits = 2)




## --------------------
## Source of Reduplicated Entries - Assumptions #5

subset_5 <- result[result$Zip_Same %in% TRUE & result$LonLat_Similar %in% FALSE & result$Metadata_Same %in% TRUE, ] %>%
  `rownames<-`(NULL)

church_5 <- church_wide[church_wide$abi %in% subset_5$abi, ]

# 0.02% of the results fit the third set of assumptions.
round(nrow(subset_5)/nrow(result)*100, digits = 2)

# 0.01% of all ABI's are represented in these assumptions.
round(length(unique(church_5$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_5 <- church_5[str_which(church_5$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 31% used a PO Box at some point, representing almost 0.004% of all ABIs and 
# 0.04% of all PO Box entries.
round(length(unique(poBox_5$abi))/length(unique(church_wide$abi))*100, digits = 4)
round(length(unique(poBox_5$abi))/length(unique(subset_5$abi))*100, digits = 2)

round(length(unique(poBox_5$abi))/length(unique(poBox_all$abi))*100, digits = 2)




## --------------------
## Source of Reduplicated Entries - Assumptions #6

subset_6 <- result[result$Zip_Same %in% FALSE & result$LonLat_Similar %in% FALSE & result$Metadata_Same %in% FALSE, ] %>%
  `rownames<-`(NULL)

church_6 <- church_wide[church_wide$abi %in% subset_6$abi, ]

# 0.18% of the results fit the third set of assumptions.
round(nrow(subset_6)/nrow(result)*100, digits = 2)

# 0.11% of all ABI's are represented in these assumptions.
round(length(unique(church_6$abi))/length(unique(church_wide$abi))*100, digits = 2)

# We see that some of the variations are a result of PO Box's.
poBox_6 <- church_6[str_which(church_6$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 31% used a PO Box at some point, representing almost 0.03% of all ABIs and 
# 0.3% of all PO Box entries.
round(length(unique(poBox_6$abi))/length(unique(church_wide$abi))*100, digits = 2)
round(length(unique(poBox_6$abi))/length(unique(subset_6$abi))*100, digits = 2)

round(length(unique(poBox_6$abi))/length(unique(poBox_all$abi))*100, digits = 2)




## --------------------
## Source of Reduplicated Entries - Assumptions #7

subset_7 <- result[result$Zip_Same %in% TRUE & result$LonLat_Similar %in% FALSE & result$Metadata_Same %in% FALSE, ] %>%
  rbind(result[result$Zip_Same %in% FALSE & result$LonLat_Similar %in% FALSE & result$Metadata_Same %in% TRUE, ]) %>%
  `rownames<-`(NULL)

church_7 <- church_wide[church_wide$abi %in% subset_7$abi, ]

# 0.002% of the results fit the third set of assumptions.
nrow(subset_7)/nrow(result)*100

# 0.001% of all ABI's are represented in these assumptions.
length(unique(church_7$abi))/length(unique(church_wide$abi))*100

# We see that some of the variations are a result of PO Box's.
poBox_7 <- church_7[str_which(church_7$address_line_1, "(?i)PO Box|P O Box"), ]

# Nearly 91% used a PO Box at some point, representing almost 0.001% of all ABIs and 
# 0.01% of all PO Box entries.
round(length(unique(poBox_7$abi))/length(unique(church_wide$abi))*100, digits = 4)
round(length(unique(poBox_7$abi))/length(unique(subset_7$abi))*100, digits = 2)

round(length(unique(poBox_7$abi))/length(unique(poBox_all$abi))*100, digits = 2)




## ----------------------------------------------------------------
## ASSUMPTION TO REMOVE PO BOX'S

# Evaluating entries first listed under a PO Box and later under a street
# address reveals that the cleaned data in church_long contains at least one
# misreported church opening. Church closings and reopenings may also be
# affected.

example_po_boxes <- poBox_1[poBox_1$`2001` %in% 1, "abi"] %>% unique()

# Example #1
church_wide[2824:2829, ] %>% 
  select(-year_established, -state, -zipcode, -city, -address_line_1,
         -primary_naics_code, -naics8_descriptions, -longitude, -latitude)
church_long[762, 1:3]

# Example #2
church_wide[1877:1878, ] %>% 
  select(-year_established, -state, -zipcode, -city, -address_line_1,
         -primary_naics_code, -naics8_descriptions, -longitude, -latitude)
church_long[524, 1:3]


# These examples also show that the geolocation of a PO Box is sometimes identical
# to that of a physical address. This is not problematic if the intent is to
# closely associate the two; however, it may be problematic if multiple matches
# exist or if the PO Box location is sufficiently distant to suggest a move
# outside of the community.
# 
# The full extent of this issue warrants further assessment.

# The following is commented out since it take a while to run. Load in the 
# pre-run results to review.

# search_space <- church_wide[str_which(church_wide$address_line_1, "(?i)PO Box|P O Box"), "abi"] %>% 
#   unique() # Isolate ABIs that filed under a PO Box at one point
# church_wide_dt <- as.data.table(church_wide)  # Convert for efficient data manipulation
# result4 <- vector("list", length(search_space))  # Initialize an empty list
# pb = txtProgressBar(min = 0, max = length(search_space), style = 3)  # Initialize progress bar
# 
# for (i in 1:length(search_space)) {
#   # Subset to show only the entries associated with one reduplicated ABI.
#   subset <- church_wide_dt[abi %in% search_space[i]]
#   
#   # --------------------
#   # Identify PO Box address rows for this ABI, then compare each PO Box row’s
#   # longitude/latitude to every NON–PO Box row for the same ABI (i.e., all rows
#   # excluding the PO Box row itself and all other PO Box rows).
#   
#   index <- str_which(subset$address_line_1, "(?i)PO Box|P O Box")
#   
#   build <- vector("list", length(index))
#   for(j in 1:length(index)){
#     po_box <- index[j]
#     others <- setdiff(seq_len(nrow(subset)), union(po_box, index))
#     
#     # Test how similar the longitude and latitude are.
#     negligible_change <- 0.002  # Change in degrees (~222 meters or 728 feet)
#     
#     lon_test <- abs(subset$longitude[others] - subset$longitude[po_box])
#     lat_test <- abs(subset$latitude[others] - subset$latitude[po_box])
#     lonLat_test <- lon_test < negligible_change & lat_test < negligible_change
#     
#     # Compile the results.
#     build[[j]] <- cbind(
#       # Use the ABI as the first column.
#       unique(subset$abi),
#       # Add the PO Box name.
#       subset[po_box, "address_line_1"],
#       # Add the summary results capturing if any comparisons passed.
#       any(lonLat_test, na.rm = TRUE),
#       # Number of failed Boolean tests.
#       lonLat_test %>% (\(y) sum(!y, na.rm = TRUE)) (),
#       # Total number of comparisons.
#       lonLat_test %>% (\(y) length(y)) (),
#       # Summary statistics of the failed Boolean tests (longitude diffs).
#       lon_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else min(y,  na.rm = TRUE)) (),
#       lon_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else mean(y, na.rm = TRUE)) (),
#       lon_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else max(y,  na.rm = TRUE)) (),
#       # Summary statistics of the failed Boolean tests (latitude diffs).
#       lat_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else min(y,  na.rm = TRUE)) (),
#       lat_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else mean(y, na.rm = TRUE)) (),
#       lat_test[!lonLat_test] %>% (\(y) if (length(y) == 0) NA_real_ else max(y,  na.rm = TRUE)) ()
#     )
#   }
#   
#   # Store 'build' in the list.
#   result4[[i]] <- do.call(rbind, build) %>% 
#     `colnames<-`(c("abi","address_line_1","Summary Outcome", "n_failed","n_total",
#                    "lon_failed_min","lon_failed_mean","lon_failed_max", 
#                    "lat_failed_min","lat_failed_mean","lat_failed_max"))
#   
#   # Print the for loop's progress.
#   setTxtProgressBar(pb, i)
# }
# # Commit result with reformatting.
# result4 <- rbindlist(result4, use.names = TRUE, fill = TRUE) %>%
#   (\(DT) {
#     cols_to_round <- c("lon_failed_min", "lon_failed_mean","lon_failed_max",
#                        "lat_failed_min","lat_failed_mean","lat_failed_max")
#     DT[, (cols_to_round) := as.data.table(sapply(.SD, round, digits = 3)),
#        .SDcols = cols_to_round]
#   })()


#' @description Codebook for the output fields produced by the evaluation.
#'
#' @field abi Unique business identifier. Evaluation is performed over each 
#'            unique business ID associated with at least one PO Box address.
#'
#' @field address_line_1 PO Box address. Evaluation is performed over each
#'                       unique PO Box associated with a given business ID.
#'
#' @field `Summary Outcome` Boolean. TRUE if any entries sharing the same PO
#'                          Box number pass both the longitude and latitude
#'                          similarity tests (threshold: 0.002 degrees each).
#'
#' @field n_failed Number of entries that failed the longitude and latitude 
#'                 similarity tests.
#'
#' @field n_total Total number of occurrences of the PO Box across all entries.
#'
#' @field lon_failed_[min|mean|max] Minimum, mean, and maximum longitude values
#'                                  among entries that failed the similarity test.
#'
#' @field lat_failed_[min|mean|max] Minimum, mean, and maximum latitude values
#'                                  among entries that failed the similarity test.

# # Save the result.
# write.csv(result4, "Data/Results/KEEP LOCAL/From Explore the Raw Data/PO Box Geolocation_04.10.2026.csv")

# Load in the pre-produced test results for evaluation.
result4 <- read_csv("Data/Results/KEEP LOCAL/From Explore the Raw Data/PO Box Geolocation_04.10.2026.csv", 
                    col_types = cols(...1 = col_skip())) %>% as.data.frame()

# A total of 37244 entries contained only a PO Box and no physical address.
# The failure rate increased with the number of addresses evaluated, which is
# expected, as a PO Box should be in close proximity to only one or a small
# number of closely situated physical addresses.
table("Number of Failed Evaluations" = result4$n_failed, "Total Evaluated" = result4$n_total, useNA = "ifany")

# About 3% of ABI only listed under a PO Box, and no physical address.
result4 %>%
  filter(n_total %in% 0) %>%
  pull(abi) %>%
  unique() %>%
  length() %>%
  (\(x) { (x / length(unique(church_wide$abi))) * 100 }) ()

# 1/4 of the PO Boxes listed are the only location on record for a given 
# organization.
round((table(result4$n_total, useNA = "ifany") / nrow(result4)) * 100, digits = 3)

# All entries with zero total comparisons yield an outcome of FALSE, as expected.
round((table(result4$n_total, result4$`Summary Outcome`) / nrow(result4)) * 100, digits = 1)

# Excluding evaluations where only PO Boxes were used, 38% of the entries showed
# at least one PO Box to physical address alignment.
result4 %>%
  filter(n_total %!in% 0) %>%
  (\(x) { (round((table(x$`Summary Outcome`) / nrow(x)) * 100, digits = 1)) }) ()

# Fewer than 1% of entries failed the test, likely due to NA values in the
# longitude or latitude of one of the comparators. These entries will be
# excluded from further analysis.
result4 %>%
  filter(n_total %!in% 0) %>%
  (\(x) { round(nrow(x[x$lon_failed_min == Inf, ]) / nrow(x) * 100, digits = 3) }) ()

# Based on the summary statistics, we see that most of the distribution of
# differences are close to zero, with the 3rd quartile for each below 1
# degree longitude or latitude. However, there were some that had large
# differences, as many as 47 degrees longitude different.
result4 %>%
  filter(n_total %!in% 0, `Summary Outcome` %in% FALSE) %>%
  filter(lon_failed_min %!in% Inf & lat_failed_min %!in% Inf) %>%
  select(-abi, -address_line_1, -`Summary Outcome`, -n_failed, -n_total) %>%
  summary()




## ----------------------------------------------------------------
## STATES REPRESENTED

# All 50 states are represented, along with D.C. and five U.S. territories.
# Confirming representation of all cities is not applicable, as some cities
# may genuinely have no religious institutions rather than reflecting missing 
# data.

# Confirm all 50 U.S. states are represented.
unique(church_wide$state) %>% .[. %in% datasets::state.abb] %>% length()

# The following abbreviations represent non-state U.S. jurisdictions that are 
# also present:
#
#   DC - District of Columbia
#   PR - Puerto Rico
#   VI - U.S. Virgin Islands
#   GU - Guam
#   MP - Northern Mariana Islands
#   FM - Federated States of Micronesia
unique(church_wide$state) %>% .[. %!in% datasets::state.abb]








