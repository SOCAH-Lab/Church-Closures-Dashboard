## ----------------------------------------------------------------
## Data Standardization and CSV-to-Parquet Conversion
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: August 3rd, 2026
## 
## Description: "Process Data Update.R" evaluated the differences between the
##              2023 and 2026 Formatted data. Notably, the 2026 Format contains
##              numerous additional metadata fields not represented in the 2023
##              Format. Most are not relevant to the immediate analysis and will
##              therefore not be processed for cleaning or validation.
## 
##              Variable columns directly relevant to the analysis, however,
##              will be processed for standardization and, where possible,
##              cleaned and validated. This script focuses on standardizing the
##              North American Industry Classification System (NAICS) and 
##              Standard Industrial Classification (SIC) code columns. It also 
##              converts the CSV to Parquet for efficient and faster data 
##              storage and loading.
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
##       This script contains the revised pipeline developed and represents the 
##       current recommended workflow.
## 
##       The steps for the 2023 Format and the 2026 Format are no longer in the 
##       same processing sequence, and are therefore not directly comparable. 
##       Adjustments were made to:
## 
##            1. Implement changes based on the findings in "Process Data Update.R",
##               which documents key differences between the two formats.
## 
##            2. Refactor the workflow to resolve errors encountered during 
##               prototype development and to improve computational performance.
## 
##            3. Increase use of the High Performance Computing (HPC) environment, 
##               which was not available during initial development and 
##               previously required more discrete steps to accommodate local 
##               limitations.
## 
##            4. Account for updated USPS API terms of use which will now
##               incur costs to users.
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

# Load the raw dataset in long format.
church_2026_form <- read_csv("Data/Raw/KEEP LOCAL/church_long_form_050926.csv")

# Load the coded representation of variables from "Process Data Update.R".
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
# of data collection. We want to check if any of the 43 outcomes in the primary 
# SIC are unique or used in overflow SIC.

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

#' @description
#' Codebook for the table of unique primary SIC codes and their descriptions
#' following nomenclature cleaning.
#'
#' @field primary_sic_code The six-digit primary SIC code.
#' @field sic6_descriptions The description associated with the SIC code.

# # Save results the primary SIC codes only
# write.csv(primary_sic, file = "./Data/Results/KEEP LOCAL/From Process Data Update/Primary SIC Codes_06.10.2026.csv")

# Load in the pre-produced results.
primary_sic <- read.csv("./Data/Results/KEEP LOCAL/From Process Data Update/Primary SIC Codes_06.10.2026.csv") %>% (\(x) x[, -1, drop = FALSE])()


#' @description
#' Codebook for the table listing all unique SIC codes with their descriptions
#' and boolean columns indicating whether each code appears in the primary SIC
#' code column or any of the four additional overflow columns. Values reflect
#' codes and descriptions following nomenclature cleaning.
#'
#' @field sic_code The six-digit SIC code.
#' @field sic_desc The description associated with the SIC code.
#' @field primary_sic_code...sic6_descriptions,
#'        `sic_code...sic6_descriptions_sic`,
#'        `sic_code_1...sic6_descriptions_sic1`,
#'        `sic_code_2...sic6_descriptions_sic2`,
#'        `sic_code_3...sic6_descriptions_sic3`
#'        Boolean. TRUE if the code and description pair appear in that column;
#'        FALSE otherwise.

# # Save results for all SIC columns
# write.csv(sic_results_confirm$presence_wide, file = "./Data/Results/KEEP LOCAL/From Process Data Update/Non-Primary SIC Codes_06.10.2026.csv")

# Load in the pre-produced results.
sic_results_confirm <- read.csv("./Data/Results/KEEP LOCAL/From Process Data Update/Non-Primary SIC Codes_06.10.2026.csv") %>% (\(x) x[, -1, drop = FALSE])()



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

#' @description
#' Codebook for new output fields produced by the data cleaning and validation
#' step. All other fields were present at the time of data import.
#'
#' @field primary_naics6_code First six digits of the eight-digit NAICS code,
#'                            representing the true NAICS classification (813110).
#'
#' @field naics6_descriptions Renamed form of the `naics8_descriptions` column.
#'
#' @field primary_naics2_code Last two digits of the eight-digit NAICS code.
#'                            These are proprietary encodings added by Data Axle;
#'                            no data dictionary was provided.

write_parquet(church_2026_form, "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")



