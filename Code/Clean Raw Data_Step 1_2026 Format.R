## ----------------------------------------------------------------
## Data Standardization and CSV-to-Parquet Conversion
##
## NOTE: This script was designed for the 2026 raw data format and reflects
##       an updated procedure from the original version found in
##       "Clean Raw Data_Step 1_2023 Format.R", "Clean Raw Data_Step 2_2023 Format.R", 
##       and "Clean Raw Data_Step 2 HPC_2023 Format.R". Refer to 
##       "Process Data Update.R" for a description of the differences and any 
##       handling variations.
##
##
## FIX Description and load data note
## 
# During the raw data review, several nomenclature inconsistencies were
# identified. The following section applies these standardizations and saves 
# the output as a Parquet file for faster loading.
##
##
##
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: June 10th, 2026
## 
## Description: 
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Correct the NAICS Nomenclature
##    - PART B: Correct the SIC Nomenclature
##        * SUBSECTION B1: Validate SIC Code and Description Column Structure
##        * SUBSECTION B2: Validate Overflow Column Consistency
##        * SUBSECTION B3: Validate Unique Code-to-Description Mappings
##        * SUBSECTION B4: Apply Manual Corrections
##        * SUBSECTION B5: Export Validated SIC Code and Description Summaries
## 
##    - PART C: Add Missing Fields and Organize
##    - PART D: Save As Parquet

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
  library("future.apply")     # Parallel processing
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Processing New Data.R")
source("./Code/Support Functions/For Step 1_2026 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# NOTE: Individual-level data is stored in the "Data/Raw KEEP LOCAL" file
# to comply with the Data Use Agreement (DUA).


# In May 2026, an updated version of the raw data was provided in a format
# different from the version exported in July 2023 and provided in the
# summer of 2025. As a result, data processing was split into two paths:
# one for the 2023 format and one for the 2026 format.
#
# The following is a modified version of the 2023 methods, in which "Step 1"
# and "Step 2" are applied together with additional quality checks to ensure
# no duplications are introduced and validation is maximized. Once address
# uniqueness has been maximized and all addresses have been validated, the
# dataframe is converted from long to wide format, reducing entries to unique
# ABI and address combinations.
#
# Differences between the two formats were evaluated in
# "./Code/Process Data Update.R", with findings summarized on the
# corresponding Review page at:
# https://socah-lab.github.io/Church-Closures-Dashboard/Pages/Review_2026%20Format.html
#
# Insights from the 2023 format are assumed to apply to the 2026 format as
# well. For complete data exploration and the reporting that justified the
# steps taken here, please refer to "./Code/Explore the Raw Data.R" and the
# corresponding Review page at:
# https://socah-lab.github.io/Church-Closures-Dashboard/Pages/Review_2023%20Format.html

# Load the raw dataset in long format.
church_2026_form <- read_csv("Data/Raw/KEEP LOCAL/church_long_form_050926.csv")

core_fields <- read_csv("Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")




## ----------------------------------------------------------------
## PART A: Correct the NAICS Nomenclature

# The NAICS code includes the standard six-digit code and a proprietary
# two-digit suffix from Data Axle. These will be separated for clarity.

church_2026_form <- church_2026_form %>% 
  mutate(
    primary_naics_code  = as.character(primary_naics_code),
    primary_naics6_code = substr(primary_naics_code, 1, 6),
    primary_naics2_code  = substr(primary_naics_code, 7, 8)
  ) %>%
  rename(naics6_descriptions = naics8_descriptions) %>%
  select(-primary_naics_code)

# Confirm that all column names are accounted for. The archive_version_year
# variable is the only one not represented in core_fields, as it represents
# the recorded years.
colnames(church_2026_form)[colnames(church_2026_form) %!in% core_fields$Variable]
core_fields$Variable[core_fields$Variable %!in% colnames(church_2026_form)]


## ----------------------------------------------------------------
## PART B: Correct the SIC Nomenclature

# Confirm the following assumptions about the SIC encodings:
#   a. Overflow columns are filled progressively with no other discernible pattern.
#   b. The same categories appear across all columns, with the possible exception
#      of the primary SIC column, which may differ or be restricted.
#   c. Each code maps to a unique description and vice versa.
#   d. Nomenclature is consistent throughout.

# No entries contain data in the last overflow column.
church_2026_form$sic_code_4 %>% unique()
church_2026_form$sic6_descriptions_sic4 %>% unique()

# Generate the SIC code verification results.
sic_results <- sic_overflow_audit(church_2026_form)


## --------------------
## SUBSECTION B1: Validate SIC Code and Description Column Structure

# Each additional SIC encoding is assumed to represent supplementary
# classification information attributed to that address. The code columns follow
# the expected pattern; however, the description columns do not. This is likely
# due to missing descriptions for certain codes.

sic_results$overflow_summary_code
#sic_results$overflow_checked_code

sic_results$overflow_summary_desc
#sic_results$overflow_checked_desc

# Confirm that description deviations from the expected overflow pattern
# are associated with codes that have no corresponding description.

#sic_results$missing_desc_rows
#sic_results$missing_desc_code
sic_results$missing_desc_codes_are_subset_of_no_desc


## --------------------
## SUBSECTION B2: Validate Overflow Column Consistency

# If each column represents an overflow of the same underlying information, 
# a significant degree of overlap in the values present across columns would be 
# expected.

# All SIC codes/descriptions
sic_results$presence_wide

# Tabulated outcomes
sic_results$presence_tabs

# There is a high degree of overlap across the overflow SIC encodings, which 
# decreases as the overflow index increases. This is expected, as fewer entries 
# carry more than one or two additional SIC classifications. 
# 
# Only a small subset of possible SIC codes is utilized in the primary column,
# suggesting that the available selection may be more restricted at the point
# of data collection. We want to check if any of the 43 outcomes in the primary SIC are
# unique or used in overflow SIC.

# Pull columns that are logical TRUE/FALSE “sic presence” columns
sic_cols <- sic_results$presence_wide |>
  select(where(is.logical)) |>
  names()

other_cols <- setdiff(sic_cols, "primary_sic_code + sic6_descriptions")

# All Primary SIC outcomes are present in the overflow categories
sic_results$presence_wide |>
  filter(.data[["primary_sic_code + sic6_descriptions"]] & if_any(all_of(other_cols), ~ .x))


## --------------------
## SUBSECTION B3: Validate Unique Code-to-Description Mappings

# It is assumed that each code maps uniquely to a single description and vice 
# versa. To verify this assumption, a mapping table is constructed from all 
# non-null (code, description) pairs across all column pairs.

sic_results$map_tbl

# Consistency checks:
#   a) Each sic_code maps to exactly one description.
#   b) Each description maps to exactly one sic_code.
#   c) sic_codes associated with an NA description are identified and accounted for.

list(
  n_unique_pairs = sic_results$n_unique_pairs,
  code_to_desc_consistent = sic_results$code_to_desc_consistent,
  desc_to_code_consistent = sic_results$desc_to_code_consistent,
  codes_with_multiple_desc = sic_results$codes_with_multiple_desc,
  desc_with_multiple_codes = sic_results$desc_with_multiple_codes,
  desc_that_is_sometimes_na = sic_results$desc_that_is_sometimes_na
)

# It appears that some codes are associated with multiple descriptions and
# vice versa. There are also some codes that are sometimes associated with an NA
# even though other times it has a description. Each case will be corrected 
# individually.
# 
# Likely, many of these inconsistencies are attributed to variations 
# in nomenclature that can be resolved manually.


## --------------------
## SUBSECTION B4: Apply Manual Corrections

fix_desc <- sic_fix_tables(sic_results)$fix_desc   # Entries to correct description
fix_code <- sic_fix_tables(sic_results)$fix_code   # Entries to correct code
fix_na   <- sic_fix_tables(sic_results)$fix_na     # Entries to correct NA

# Correct description typographical errors
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[1, "sic_desc_2"][1], replacement = fix_desc[1, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[2, "sic_desc_2"][1], replacement = fix_desc[2, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[3, "sic_desc_2"][1], replacement = fix_desc[3, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[4, "sic_desc_2"][1], replacement = fix_desc[4, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[5, "sic_desc_2"][1], replacement = fix_desc[5, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[6, "sic_desc_2"][1], replacement = fix_desc[6, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[7, "sic_desc_1"][1], replacement = fix_desc[7, "sic_desc_2"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[8, "sic_desc_2"][1], replacement = fix_desc[8, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[9, "sic_desc_2"][1], replacement = fix_desc[9, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[10, "sic_desc_2"][1], replacement = fix_desc[10, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[11, "sic_desc_1"][1], replacement = fix_desc[11, "sic_desc_2"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[12, "sic_desc_2"][1], replacement = fix_desc[12, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[13, "sic_desc_2"][1], replacement = fix_desc[13, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[14, "sic_desc_1"][1], replacement = fix_desc[14, "sic_desc_2"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[15, "sic_desc_2"][1], replacement = fix_desc[15, "sic_desc_1"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[16, "sic_desc_1"][1], replacement = fix_desc[16, "sic_desc_2"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[17, "sic_desc_1"][1], replacement = fix_desc[17, "sic_desc_2"][1], fixed = TRUE)
church_2026_form <- clean_sic_descs(church_2026_form, pattern = fix_desc[18, "sic_desc_1"][1], replacement = fix_desc[18, "sic_desc_2"][1], fixed = TRUE)

# Correct the code redundancies
church_2026_form <- clean_sic_codes(church_2026_form, pattern = fix_code[1, "sic_code_2"], replacement = fix_code[1, "sic_code_1"], fixed = TRUE)
church_2026_form <- clean_sic_codes(church_2026_form, pattern = fix_code[2, "sic_code_2"], replacement = fix_code[2, "sic_code_1"], fixed = TRUE)

# Correct the NA description associated with a code detected
church_2026_form <- set_sic_desc_for_code(church_2026_form, fix_na[1, "sic_code"], fix_na[1, "sic_desc_1"])
church_2026_form <- set_sic_desc_for_code(church_2026_form, fix_na[2, "sic_code"], fix_na[2, "sic_desc_1"])
church_2026_form <- set_sic_desc_for_code(church_2026_form, fix_na[3, "sic_code"], fix_na[3, "sic_desc_1"])
church_2026_form <- set_sic_desc_for_code(church_2026_form, fix_na[4, "sic_code"], fix_na[4, "sic_desc_2"])


# Reprocess the normalized data frame to confirm all previously conflicting
# SIC nomenclature has been reconciled and no new conflicts were introduced.
sic_results_confirm <- sic_overflow_audit(church_2026_form)
sic_fix_tables(sic_results_confirm)


# Isolate the primary SIC codes
primary_sic <- church_2026_form %>%
  select(primary_sic_code, sic6_descriptions) %>%
  mutate(
    primary_sic_code   = as.character(primary_sic_code),
    sic6_descriptions  = as.character(sic6_descriptions)
  ) %>%
  distinct() %>%
  arrange(primary_sic_code, sic6_descriptions)


## --------------------
## SUBSECTION B5: Export Validated SIC Code and Description Summaries

# Save results the primary SIC codes only
write.csv(primary_sic, file = "Data/Results/KEEP LOCAL/From Process Data Update/Primary SIC Codes_06.10.2026.csv")

# Save results for all SIC columns
write.csv(sic_results_confirm$presence_wide, file = "Data/Results/KEEP LOCAL/From Process Data Update/Non-Primary SIC Codes_06.10.2026.csv")




## ----------------------------------------------------------------
## PART C: Add Missing Fields and Organize

# Inspect which expected SIC description columns are present (and which are missing)
# in the current data frame.
colnames(church_2026_form)[colnames(church_2026_form) %!in% core_fields$Variable]
core_fields$Variable[core_fields$Variable %!in% colnames(church_2026_form)]

# Reorganize columns and rows, and append a combined address field
church_2026_form <- church_2026_form %>%
  # Order records by ABI and then year (adjust desc() if you truly want descending).
  arrange(abi, archive_version_year) %>%
  # Keep year plus the set of core fields.
  select(archive_version_year, all_of(core_fields$Variable)) %>%
  mutate(
    # Create a single address string; include ZIP+4 when available.
    combined_address = paste(
      address_line_1, city, state,
      if_else(!is.na(zip4) & zip4 != "",
              paste0(zipcode, "-", zip4),
              as.character(zipcode)),
      sep = ", "
    )
  ) %>%
  # Place combined_address near the ZIP fields for readability.
  relocate(combined_address, .after = zip4) %>%
  as.data.frame()


## ----------------------------------------------------------------
## PART D: Save As Parquet

write_parquet(church_2026_form, "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")



