## ----------------------------------------------------------------
## 
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 11th, 2026
## Date Modified: May 15th, 2026
## 
## Description: This script validates addresses using the USPS API. It is designed
##              to run both locally and on Yale's High Performance Computing (HPC)
##              cluster, leveraging the parsed indices defined below.
## 
## Description: This script validates addresses using the USPS API. It is designed
##              to run both locally and on Yale's High Performance Computing (HPC)
##              cluster, leveraging the parsed indices defined below.
##
##              When running on the HPC, this script supports two execution modes:
##              a single index at a time via a live session, or as a job array
##              using the provided batch script (see SUBSECTION A1: Utilizing the
##              HPC for details).
##
##              When running locally, ensure that all code sections marked
##              "... on the HPC" are commented out and their corresponding
##              alternatives marked "... locally" are active. The HPC version
##              is given first, followed by the local version.
##
##              Results are processed in sequential sections and compiled in
##              the Step 2 main script.
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: ASSESS METADATA CONTENT DIFFERENCES
## 
##    - PART B: NAICS and SIC Encodings
##        * SUBSECTION B1: Primary NAICS and SIC Encodings
##        * SUBSECTION B2: Primary and Additional SIC Encodings
## 
##    - PART C: HANDLING ADDITIONAL METADATA
##        * SUBSECTION C1: Supplementary Location Metadata
##        * SUBSECTION C2: Metadata Not Contributive to Project Goals

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# # Initiate the package environment
# renv::activate() 
renv::restore()

# Load packages to the environment
suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("arrow")            # Reads in and handles parquet files
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("tibble")           # Manipulate data frames in tidyverse
  library("stringr")          # String operations
  library("purrr")            # Functional programming tools
  library("data.table")       # High-performance data manipulation
})

# Load in the functions
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Processing New Data.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

church_wide <- read_csv("Data/Raw/KEEP LOCAL/church_wide_form_071723.csv")
church_long_form_050926 <- read_csv("Data/Raw/KEEP LOCAL/church_long_form_050926.csv")




## ----------------------------------------------------------------
## PART A: ASSESS METADATA CONTENT DIFFERENCES

# This section analyzes differences between available columns
# and their respective data classifications. NOTE: archive_version_year
# contains the years represented in the archive.

types_wide <- sapply(church_wide, class) %>% 
  as.data.frame() %>% 
  rownames_to_column() %>% 
  `colnames<-`(c("Variable", "Type"))

# Process first 10 rows only to improve speed
types_long <- sapply(church_long_form_050926[1:10, ], class) %>% 
  as.data.frame() %>% 
  rownames_to_column() %>% 
  `colnames<-`(c("Variable", "Type"))

full_join(types_long, types_wide, by = "Variable") %>%
  `colnames<-`(c("Variable", "Long 2026", "Wide 2023"))


# Core fields (abi, address_line_1, city, and state) are consistent between
# the old and new data. The new data stores the zip code as a five-digit 
# character field, preserving leading zeros, and adds a four-digit zip code 
# extension.
#
# The new dataset also introduces additional columns covering census and location
# attributes, as well as business-level details including size, status, and SIC 
# encoding. These are categorized below:
# 
# Core business details by business ID (7):
#     - company (i.e. name)
#     - abi
#     - year_established
#     - primary_naics6_code
#     - naics6_descriptions
#     - subsidiary_number
#     - company_holding_status
# 
# Core business details by location (17):
#     - address_line_1
#     - city
#     - state
#     - zipcode
#     - zip4
#     - primary_sic_code
#     - sic6_descriptions
#     - sic_code .. _4
#     - sic6_descriptions_sic .. 4
# 
# Core business location details (10):
#     - latitude
#     - longitude
#     - census_block
#     - census_tract
#     - county_code
#     - fips_code
#     - cbsa_level
#     - cbsa_code
#     - csa_code
#     - area_code
# 
# Core business details by location and might change by year (7):
#     - site_number
#     - yellow_page_code
#     - office_size_code
#     - employee_size_location
#     - location_employee_size_code
#     - sales_volume_location
#     - location_sales_volume_code
# 
# Parent company details. These might change by year (5):
#     - parent_number
#     - parent_actual_employee_size
#     - parent_employee_size_code
#     - parent_actual_sales_volume
#     - parent_sales_volume_code
# 
# Unknown variables or code translations (8):
#     - primary_naics2_code
#     - address_type_indicator
#     - industry_specific_first_byte
#     - business_status_code
#     - population_code
#     - match_code
#     - ticker
#     - idcode
# 
# This analysis requires only the core business and location details. The remaining
# metadata will be processed simultaneously to produce a final, cleaned dataset,
# though not with any specific follow-up analysis in mind. As a result, the metadata
# may not be in a format suitable for all analyses, as the data are primarily
# prepared for the dashboard.
# 
# There are two types of metadata:
#   a) Metadata relevant to a given business or location that does not change
#      across years (e.g. year_established, fips_code)
#   b) Metadata that may change across years (e.g. employee_size_location)
# 
# Data are collapsed to unique businesses and locations, with archive_version_year
# widened into a column-wise representation of years open (1) or closed (0).
# Metadata are processed by considering change across location and year:
#   - Metadata expected to be consistent across all locations of a business
#     are applied to all locations.
#   - Metadata expected to vary by location are summarized by location.
# 
# Both types may contain multiple entries, distinguished by the date ranges they
# apply to. To account for this variation, differing values for some fields are
# retained alongside their associated date ranges. Where no date range is present,
# only one value was observed for that business or location. Not all fields with
# unique values are formatted this way; only those expected to be relevant to the
# project goals.
# 
# NOTE: When processing the previously exported 2023 data, it was identified that
# the NAICS code is eight digits long, with the trailing two digits representing
# an unknown proprietary code from Data Axle. These will be separated into a
# six-digit naics6 code with description and a two-digit naics2 code with no
# associated description.
# 
# Some variables have unknown purpose or encoding. These will be handled at
# the most granular level, parsed for changes by year and variation by location.


# Table to encode the data processing by variable
core_fields <- tibble(
  Variable = c(
    # Core business details by business ID (7)
    "company",
    "abi",
    "year_established",
    "primary_naics6_code",
    "naics6_descriptions",
    "subsidiary_number",
    "company_holding_status",
    
    # Core business details by location (17)
    "address_line_1",
    "city",
    "state",
    "zipcode",
    "zip4",
    "primary_sic_code",
    "sic6_descriptions",
    "sic_code",
    "sic6_descriptions_sic",
    "sic_code_1",
    "sic6_descriptions_sic1",
    "sic_code_2",
    "sic6_descriptions_sic2",
    "sic_code_3",
    "sic6_descriptions_sic3",
    "sic_code_4",
    "sic6_descriptions_sic4",
    
    # Core business location details (10)
    "latitude",
    "longitude",
    "census_block",
    "census_tract",
    "county_code",
    "fips_code",
    "cbsa_level",
    "cbsa_code",
    "csa_code",
    "area_code",
    
    # Core business details by location and might change by year (7)
    "site_number",
    "yellow_page_code",
    "office_size_code",
    "employee_size_location",
    "location_employee_size_code",
    "sales_volume_location",
    "location_sales_volume_code",
    
    # Parent company details. These might change by year (5)
    "parent_number",
    "parent_actual_employee_size",
    "parent_employee_size_code",
    "parent_actual_sales_volume",
    "parent_sales_volume_code",
    
    # Unknown variables or code translations (8)
    "primary_naics2_code",
    "address_type_indicator",
    "industry_specific_first_byte",
    "business_status_code",
    "population_code",
    "match_code",
    "ticker",
    "idcode"
  ),
  Group = c(
    rep("Core business details", 7),
    rep("Core business details by location", 17),
    rep("Core location details", 10),
    rep("Core business details by location", 7),
    rep("Parent company details", 5),
    rep("Unknown variables or code translations", 8)
  ),
  Metadata = c(
    TRUE, FALSE, rep(TRUE, 5),
    rep(FALSE, 5), rep(TRUE, 12),
    rep(FALSE, 10),
    rep(TRUE, 7),
    rep(TRUE, 5),
    rep(TRUE, 8)
  ),
  "By Business" = c(
    rep(TRUE, 7),
    rep(FALSE, 17),
    rep(FALSE, 10),
    rep(FALSE, 7),
    rep(TRUE, 5),
    NA, FALSE, NA, TRUE, FALSE, rep(NA,3)
  ),
  "By Year" = c(
    rep(FALSE, 7),
    rep(FALSE, 7), rep(TRUE, 10),
    rep(FALSE, 10),
    rep(TRUE, 7),
    rep(TRUE, 5),
    rep(TRUE, 8)
  )
)

# Separate naics8 into naics6 (with description) and naics2.
types_long[, "Variable"] <- str_replace(types_long[, "Variable"], "primary_naics_code", "primary_naics6_code")
types_long[, "Variable"] <- str_replace(types_long[, "Variable"], "naics8_descriptions", "naics6_descriptions")
types_long <- rbind(types_long, data.frame("Variable" = "primary_naics2_code", "Type" = "numeric"))

types_wide[, "Variable"] <- str_replace(types_wide[, "Variable"], "primary_naics_code", "primary_naics6_code")
types_wide[, "Variable"] <- str_replace(types_wide[, "Variable"], "naics8_descriptions", "naics6_descriptions")
types_wide <- rbind(types_wide, data.frame("Variable" = "primary_naics2_code", "Type" = "numeric"))


# Compile all handling procedures, variable types, and cross-references
# to their equivalents in the previous dataset.

core_fields <- core_fields %>%
  left_join(types_long, by = "Variable") %>%
  left_join(types_wide, by = "Variable") %>% 
  `colnames<-`(c("Variable", "Group", "Metadata", "By Business", "By Year", "Var Class_2026", "Var Class_2023")) %>%
  as.data.frame()


#' @description Codebook for the output fields produced by the evaluation.
#' 
#' @field Variable Variable name as it appears in the raw data; present in both
#'                 versions.
#'                        
#' @field Group Grouping of variables by relevance to the business or business
#'              location (primary or metadata).
#'              
#' @field Metadata Boolean. TRUE if the information is secondary to visualizing
#'                 business location in a given year.
#'                        
#' @field `By Business` Boolean. TRUE if the variable is expected to be
#'                      consistent across all locations of a given business.
#'                        
#' @field `By Year` Boolean. TRUE if the variable is expected to
#'                         change across years.
#'                        
#' @field `Var Class_2026` Variable class and availability in the 2026
#'                         raw data export.
#'                         
#' @field `Var Class_2023` Variable class and availability in the 2023
#'                         raw data export.

# # Save the result.
# write.csv(core_fields, file = "Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")



## ----------------------------------------------------------------
## PART B: NAICS and SIC Encodings

# NAICS and SIC encodings provide additional context about the type of
# business, religious affiliation, and function. The 2023 form of the data
# only included the NAICS encoding, while the 2026 form includes both.

## --------------------
## SUBSECTION B1: Primary NAICS and SIC Encodings

# As noted, NAICS codes are provided as eight-digit numbers, where only the
# first six digits represent the standard NAICS code. The trailing two digits
# are a proprietary encoding from Data Axle and will be separated accordingly.
#
# Both versions share the same unique NAICS encodings. The 2023 data processing
# protocol retained only the unique NAICS six-digit codes and descriptions,
# which are consistent across all entries; the trailing two digits were stored
# as a string. The method devised in PART C to retain the applicable years will
# also be applied to this field.

all(unique(church_long_form_050926$primary_naics_code) %in% unique(church_wide$primary_naics_code)) &
  all(unique(church_wide$primary_naics_code) %in% unique(church_long_form_050926$primary_naics_code))

church_long_form_050926$naics8_descriptions %>% unique()

# SIC6 refines the classification of religious organizations by capturing
# attributes such as affiliation, denomination, and operational function
# (e.g., school, retreat center). This information was not provided in the 
# previous version.
# 
# It is provided as a primary classification column accompanied by up to four
# additional overflow columns. All are examined together in the following
# subsection.

church_long_form_050926$primary_sic_code %>% unique()
church_long_form_050926$sic6_descriptions %>% unique()


## --------------------
## SUBSECTION B2: Primary and Additional SIC Encodings

# Confirm the following assumptions about the SIC encodings:
#   a. Overflow columns are filled progressively with no other discernible pattern.
#   b. The same categories appear across all columns, with the possible exception
#      of the primary SIC column, which may differ or be restricted.
#   c. Each code maps to a unique description and vice versa.
#   d. Nomenclature is consistent throughout.

# No entries contain data in the last overflow column.
church_long_form_050926$sic_code_4 %>% unique()
church_long_form_050926$sic6_descriptions_sic4 %>% unique()

# Generate the SIC code verification results.
sic_results <- sic_overflow_audit(church_long_form_050926)


# ASSUMPTION A: Each additional SIC encoding is assumed to represent supplementary
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


# ASSUMPTION B: If each column represents an overflow of the same underlying
# information, a significant degree of overlap in the values present
# across columns would be expected.

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
sic_cols <- sic_results$presence_wide %>%
  select(where(is.logical)) %>%
  names()

other_cols <- setdiff(sic_cols, "primary_sic_code + sic6_descriptions")

# All Primary SIC outcomes are present in the overflow categories
sic_results$presence_wide %>%
  filter(.data[["primary_sic_code + sic6_descriptions"]] & if_any(all_of(other_cols), ~ .x)) %>%
  nrow()


# ASSUMPTION C: It is assumed that each code maps uniquely to a single 
# description and vice versa. To verify this assumption, a mapping table is 
# constructed from all non-null (code, description) pairs across all column pairs.

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

# Some codes are associated with multiple descriptions and vice versa. 
# Additionally, some codes are sometimes associated with an NA description 
# despite having a valid description in other instances. Each case will be 
# addressed individually.
#
# ASSUMPTION D: Many of these inconsistencies are likely attributable to 
# variations in nomenclature.

# Entries to correct description
fix_desc <- sic_results$presence_wide %>%
  filter(sic_code %in% sic_results$codes_with_multiple_desc$sic_code) %>%
  select(sic_code, sic_desc) %>%
  arrange(sic_code) %>%
  group_by(sic_code) %>%
  mutate(dup_id = row_number()) %>%
  ungroup() %>%
  pivot_wider(
    id_cols = sic_code,
    names_from = dup_id,
    values_from = sic_desc,
    names_prefix = "sic_desc_"
  ) %>%
  as.data.frame()

# Entries to correct code
fix_code <- sic_results$presence_wide %>%
  semi_join(
    sic_results$presence_wide %>%
      filter(sic_desc %in% sic_results$desc_with_multiple_codes$sic_desc) %>%
      distinct(sic_code),
    by = "sic_code"
  )

# Entries to correct NA
fix_na <- sic_results$presence_wide %>%
  semi_join(
    sic_results$presence_wide %>%
      filter(sic_desc %in% sic_results$desc_that_is_sometimes_na$sic_desc) %>%
      distinct(sic_code),
    by = "sic_code"
  )


# These SIC code characteristics are expected to vary with additional years of
# data or subsequent exports of previous reports, as Data Axle may update their
# database over time. It will therefore be important to process these columns
# from a raw data export prior to beginning any analysis or data validation.
# 
# All unique SIC values for a given abi and address will be retained and 
# expanded as overflow columns. The primary encoding will be constrained to 
# ensure it remains consistent with qualifying classifiers, while all additional 
# SIC columns will represent any other unique SIC values present.




## ----------------------------------------------------------------
## PART C: HANDLING ADDITIONAL METADATA

# Several core fields are consistent with the 2023 data. The NAICS six- and
# two-digit codes and the year_established variable were treated as secondary
# metadata not directly contributive to the project goals. The 2026 data
# contains a significant number of additional variables, some of which are also
# not directly contributive to the project goals but should be retained to
# remain associated with the cleaned and validated outcomes.
# 
# In PART B, two encodings were examined: NAICS and SIC. NAICS remained
# uninformative, while SIC provided additional important outlet details
# (described above). The remaining metadata fall into two handling categories:
# 
#   a) Metadata not directly relevant to the current project. These fields will
#      be retained with no effort made to clean or validate the values.
# 
#   b) Metadata supplying additional location details, such as county code,
#      census block, and FIPS codes. These fields will be assessed for
#      validation in subsequent steps while being handled similarly to
#      category (a). Note that attributed census block and related fields may
#      not translate directly to specific decennial maps, potentially
#      necessitating reference to an external database.
# 
# As described above, metadata can vary by location and across the years a
# business was recorded. As no cleaning or validation effort is being applied
# to category (a), both location and year-level variation will be retained.
# The same approach will apply to category (b), with the addition of Boolean
# validation checks to capture:
#
#   - Whether the same location outcome spans the complete expected date range.
#   - Whether multiple location outcomes are associated with the same address.
#   - Whether multiple identified location outcomes vary by decennial period or
#     follow alternative patterns.
# 
# Source: https://www.census.gov/content/dam/Census/library/publications/2020/acs/acs_geography_handbook_2020_ch02.pdf

# Generate a subset to test the algorithms on
subset <- church_long_form_050926 %>%
  filter(abi %in% unique(church_long_form_050926$abi)[6:10]) %>%
  mutate(
    across(c(census_block, cbsa_level, area_code), as.character),
    combined_address = paste(
      address_line_1, city, state,
      if_else(!is.na(zip4) & zip4 != "",
              paste0(zipcode, "-", zip4),
              as.character(zipcode)
      ),
      sep = ", "
    )
  ) %>%
  relocate(combined_address, .after = zip4)

# # Save the result.
# write.csv(subset, file = "Data/Results/KEEP LOCAL/From Process Data Update/Summarize Metadata Method_Unprocessed Data_05.15.2026.csv")


## --------------------
## SUBSECTION C1: Supplementary Location Metadata

# The following illustrates the process of condensing additional location 
# metadata into one line per unique abi and address combination. Although some 
# of these fields are numeric, they will be coerced to character, as each 
# represents a classification regardless of whether it is encoded as a numeric 
# value. Only longitude and latitude will be retained and treated as numeric.

vars_for_loc <- c(
  # Core business location details (8)
  "census_block", "census_tract" , "county_code",  "fips_code", "cbsa_level", 
  "cbsa_code", "csa_code", "area_code"
)

loc_condensed <- summarize_many_code_ranges_dt(subset, vars_for_loc) %>%
  arrange(abi)

#' @description Codebook for the output fields produced by the evaluation. For
#'              each variable condensed to unique abi and address combination,
#'              additional checks are added.
#' 
#' @field _ranges_OK Logical. TRUE when the `*_ranges` field (e.g., 
#'                   `census_tract_ranges`) assigns a value for every year 
#'                   listed in `available_year_ranges`, has no overlapping year 
#'                   conflicts (a year mapped to more than one distinct value), 
#'                   and all available years map to exactly one unique value 
#'                   overall. NA when `available_year_ranges` is missing or 
#'                   cannot be parsed.
#' 
#' @field _ranges_decade_ok Logical or NA. Only evaluated when `*_ranges_ok` 
#'                          is FALSE. TRUE when, within each decade that has 
#'                          any mapped available years, all mapped years in that
#'                          decade share the same value (i.e., ≤ 1 distinct 
#'                          value per decade). This check ignores gaps (missing 
#'                          years) within decades. NA when `*_ranges_ok` is TRUE 
#'                          or when `available_year_ranges` is missing/unparseable.
#' 
#' @field _values_seen Character. Comma-separated list of distinct values 
#'                     observed for available years in the `*_ranges` mapping 
#'                     (restricted to years in `available_year_ranges`). Empty 
#'                     string if none are observed.
#' 
#' @field _missing_years Character. Years present in `available_year_ranges` 
#'                       that are not covered by the `*_ranges` mapping. Empty 
#'                       string if none are missing. NA when 
#'                       `available_year_ranges` is missing/unparseable.
#' 
#' @field _conflict_years Character. Years that are assigned more than one 
#'                        distinct value due to overlapping segments in the 
#'                        `*_ranges` mapping, compressed as runs in the form 
#'                        `"YYYY:YYYY"` and separated by `", "`. Empty string 
#'                        if no conflicts.

area_code_qc <- check_ranges_same_outcome(loc_condensed)

# # Save the result.
# write.csv(area_code_qc, file = "Data/Results/KEEP LOCAL/From Process Data Update/Area Code QC_Collapsed Data_05.18.2026.csv")


## --------------------
## SUBSECTION C2: Metadata Not Contributive to Project Goals

# Note that primary_naics2_code is not represented here, as it will be
# derived from the primary_naics8_code in the main dataset. This field falls
# under the "Unknown Variables or Code Translations" category.

vars_to_sum <- c(
  # Core business details by business ID (4):
  "company", "year_established", "subsidiary_number", "company_holding_status",
  # Core business details by location and might change by year (7):
  "site_number", "yellow_page_code", "office_size_code", "employee_size_location",
  "location_employee_size_code", "sales_volume_location", "location_sales_volume_code",
  # Parent company details, where some might change by year (5):
  "parent_number", "parent_actual_employee_size", "parent_employee_size_code",
  "parent_actual_sales_volume", "parent_sales_volume_code",
  # Unknown variables or code translations (8):
  #primary_naics2_code
  "address_type_indicator", "industry_specific_first_byte", 
  "business_status_code", "population_code", "match_code", "ticker", "idcode"
)

subset_condensed <- summarize_many_code_ranges_dt(subset, vars_to_sum) %>%
  arrange(abi)

# # Save the result.
# write.csv(subset_condensed, file = "Data/Results/KEEP LOCAL/From Process Data Update/Summarize Metadata Method_Collapsed Data_05.15.2026.csv")







