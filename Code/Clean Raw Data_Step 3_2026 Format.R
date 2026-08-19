## ----------------------------------------------------------------
## Standardize and Reshape Metadata to Wide Format
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: August 16th, 2026
## 
## Description: Metrics are computed at the unique ABI/address level. Therefore,
##              it is important to reshape the dataset accordingly. As discussed
##              later, not all metadata retained after data cleaning and
##              validation in Step 2 is needed going forward. The remaining
##              metadata must be standardized and annotated in preparation
##              for reshaping to unique ABI/address rows.
##
##              Most retained metadata columns from Step 2 are already unique
##              by ABI/address. SIC columns, however, require additional
##              preparation before reshaping. In addition, several new metrics
##              are derived at this stage: number of moves and a religious
##              classification of SIC codes. Years-open will be restructured
##              as binary indicator columns.
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
##    - PART A: EXCLUDE METADATA NOT CONDUCIVE TO THE ANALYSIS
## 
##    - PART B: NORMALIZE SIC CODES AND ANNOTATE WITH CLASSIFIERS
##        * SUBSECTION B1: Normalize Primary SIC Codes
##        * SUBSECTION B2: Normalize Overflow SIC Codes
##        * SUBSECTION B3: Assign Religious Classifiers
##        * SUBSECTION B4: Compile the Final SIC Wide Columns
## 
##    - PART C: GENERATE THE YEARS-OPEN BINARY COLUMNS
##        * SUBSECTION C1: Reshape Years-Open Entries from Rows to Columns
##        * SUBSECTION C2: Fill Minor Years Reported Gaps
## 
##    - PART D: IDENTIFYING AND QUANTIFYING MOVES VS. REOPENINGS AT NEW LOCATION
##    - PART E: RECONSTRUCT ALL COLUMNS AND SAVE RESULT

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("writexl")          # Writes .xlsx files (including multiple sheets) from a list
  library("openxlsx")         # Read/write Excel workbooks (.xlsx) with multiple sheets
  library("DBI")              # Standard database interface for R (dbConnect, dbWriteTable, dbGetQuery)
  library("duckdb")           # DuckDB database engine + DBI backend (local .duckdb files, in-memory DBs)
  library("arrow")            # Parquet/Feather & fast I/O (Arrow)
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("dbplyr")           # Data manipulation and transformation for DuckDB
  library("stringr")          # String operations
  library("ggplot2")          # Graphics and visualization
  library("patchwork")        # Combine multiple ggplot objects into a single layout
  library("scales")           # Scale/label helpers for plots (percent/number/date formatting, breaks)
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("data.table")       # High-performance data manipulation
  library("future.apply")     # Parallel processing
  library("progressr")        # Progress bars
  library("geosphere")        ## Geospatial distance calculations (e.g., Haversine distance in meters) 
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 3_2026 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))

# Define the "if else" for null options operation
"%||%" <- function(a, b) if (!is.null(a)) a else b




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Directory containing all Step 2 batch results.
data_root <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"

# Load cleaned and validated data.
church_2026_form_validated <- import_church_db(
  db_path = file.path(data_root, "Compiled by Batches", "church_2026_form_validated_08.03.2026.db"),
  import_data = "data"
) %>% (\(x) {x$data} )()

# Load the 6-digit SIC classifications.
sic_classifications <- read.csv("./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Code Classifications_08.15.2026.csv")




## ----------------------------------------------------------------
## PART A: EXCLUDE METADATA NOT CONDUCIVE TO THE ANALYSIS

# At this stage, the cleaned and validated data is prepared for dashboard
# metric generation. A critical structural transformation required for this
# involves reshaping records from long format (years open represented as
# rows) to wide format (years-open/years-closed encoded as binary columns), such 
# that each record is unique to a given ABI and address combination.
# 
# As demonstrated in "PART C: HANDLING ADDITIONAL METADATA" of
# "Process Data Update.R", many metadata fields are not unique to a given
# ABI/address combination. While a consolidation procedure was defined that 
# would preserve these variables as strings retaining the unique years reported 
# per ABI/address, they provide no analytical value at this stage of the project. 
# Furthermore, converting them to wide format would render them largely 
# unsuitable for usage.
# 
# Given that these variables are outside the scope of cleaning and validation 
# and do not contribute to subsequent steps, they will be excluded from further 
# processing. Users requiring distributor-provided metadata should use the 
# cleaned and validated long-form 2026 Formatted dataset generated after Step 2.
# In-scope entries will be normalized to be unique within each ABI/address 
# combination.


# Define column to exclude
exclude_cols <- c(
  # ---- company / identifiers ----
  "company", "year_established", "primary_naics6_code", "naics6_descriptions",
  "subsidiary_number", "company_holding_status",
  
  # ---- address ----
  "reported_address",
  
  # ---- industry codes / descriptions ----
  # all columns retained
  
  # ---- geocoding / verification ----
  # all columns retained
  
  # ---- census geography (current + vintages) ----
  # all columns retained
  
  # ---- CBSA/CSA (current + vintages) and ZCTA (new) ----
  # all columns retained
  
  # ---- other firmographic fields ----
  "area_code", "site_number", "yellow_page_code", "office_size_code",
  "employee_size_location", "location_employee_size_code",
  "sales_volume_location", "location_sales_volume_code",
  "parent_number", "parent_actual_employee_size", "parent_employee_size_code",
  "parent_actual_sales_volume", "parent_sales_volume_code",
  
  # ---- status / misc identifiers ----
  "primary_naics2_code", "address_type_indicator", 
  "industry_specific_first_byte", "business_status_code",
  "population_code", "match_code", "ticker", "idcode"
)


# Define columns to retain for subsequent analyses
retain_cols <- c(
  # ---- company / identifiers ----
  "archive_version_year", "abi",
  
  # ---- address ----
  "address", "address_verified", "address_matched",
  
  # ---- industry codes / descriptions ----
  "primary_sic_code", "sic6_descriptions",
  "sic_code", "sic6_descriptions_sic",
  "sic_code_1", "sic6_descriptions_sic1",
  "sic_code_2", "sic6_descriptions_sic2",
  "sic_code_3", "sic6_descriptions_sic3",
  "sic_code_4", "sic6_descriptions_sic4",
  
  # ---- geocoding / verification ----
  "latitude", "longitude", "geolocation_verified",

  # ---- census geography (current + vintages) ----
  "geoid_2000", "geoid_2010", "geoid_2020", "geoid_match",
  
  
  # ---- CBSA/CSA (current + vintages) and ZCTA (new) ----
  "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007", "zcta_2000",
  "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010", "zcta_2010",
  "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020", "zcta_2020"
  
  # ---- other firmographic fields ----
  # none retained
  
  # ---- status / misc identifiers ----
  # none retained
)


intersect(exclude_cols, retain_cols)  # Confirm the vectors are mutually exclusive
setdiff(names(church_2026_form_validated), union(exclude_cols, retain_cols))  # Confirm all columns have been represented

# Subset to columns relevant to the remainder of the analysis.
church_2026_form_analysis <- church_2026_form_validated %>%
  select(all_of(retain_cols))

church_2026_form_analysis_dt <- as.data.table(church_2026_form_analysis)  # Convert for efficient data manipulation
rm(church_2026_form_validated)  # Clear up RAM by removing the complete dataset




## ----------------------------------------------------------------
## PART B: NORMALIZE SIC CODES AND ANNOTATE WITH CLASSIFIERS

# "SUBSECTION B2: Primary and Additional SIC Encodings" in "Process Data
# Update.R" evaluated the dimensionality, nomenclature consistency, and
# other characteristics of the SIC code columns.
# 
# The primary SIC column was found to be restricted to a limited set of
# codes, while the remaining columns represented thousands of additional
# functional classifications. Each non-primary SIC column represented a
# sequential overflow of classification information. Notably, while the
# primary column contained only 42 distinct codes, those same codes
# appeared across all overflow columns as well.
# 
# Without direct assessment, it is assumed that these metadata columns,
# particularly following the data cleaning and validation steps that
# reconciled similar addresses, are not consistent across unique
# ABI/address combinations. To support downstream annotation, these
# columns will be standardized to ensure a unique value per ABI/address
# combination, consistent with all other retained variables.


## --------------------
## SUBSECTION B1: Normalize Primary SIC Codes

# Because the codes in the primary column are restricted, but are also
# represented across the SIC overflow columns, these will be handled
# separately.

# Isolate unique primary SIC outcomes by ABI/address.
primary_long <- unique(
  church_2026_form_analysis_dt[
    , .(
      abi, address,
      slot = "p",
      slot_n = 0L,
      priority = 0L,
      code = na_if_blank_chr(primary_sic_code),
      desc = na_if_blank_chr(sic6_descriptions)
    )
  ]
)


# Confirm that: a) no primary codes are NA, and b) no ABI/address
# combination is associated with more than one primary code.

# No rows have an NA primary code, description, or both.
primary_long[
  , .(
    n_rows = .N,
    na_code = sum(is.na(code)),
    na_desc = sum(is.na(desc)),
    na_both = sum(is.na(code) & is.na(desc))
  )
]

# NOTE: To avoid introducing NAs in the primary column, any NAs detected
#       at this stage should be replaced with a qualifying code drawn from
#       one of the overflow columns. Only if no qualifying code exists
#       across any overflow column should the value remain NA. As no NAs
#       were present at this stage, this process was skipped.


# Isolate ABI/address combinations with multiple primary codes.
primary_code_by_addr <- primary_long[!is.na(code),
                                     .(n_primary_codes = uniqueN(code),
                                       primary_codes = paste(sort(unique(code)), collapse = " | ")),
                                     by = .(abi, address)
]
multi_primary <- primary_code_by_addr[n_primary_codes > 1L][order(-n_primary_codes)]

# 4% of unique ABI/address entries have more than one primary SIC code associated.
round(uniqueN(multi_primary, by = c("abi", "address")) / uniqueN(primary_long, by = c("abi", "address"))*100, digits = 2)

# Notably, only 14% of these cases include a verified or matched address,
# indicating that the discrepancy predates the redundancy reduction steps
# in this pipeline. This raises the question of whether the variation
# reflects changes in religious or denominational affiliation over time.
# 
# This question falls outside the scope of the current assessment and will not 
# be explored further, only noted here.
multi_primary$abi %in% unique(pull(church_2026_form_analysis_dt[address_verified == TRUE | address_matched == TRUE, "abi"])) %>%
  (\(x) { round(prop.table(table("Verified/Matched?" = x, useNA = "ifany"))*100, digits = 2) })

# On average, these combinations included a second primary code, with some 
# having as many as five unique primary codes. As the primary codes are 
# represented across all overflow columns, these cases are easily reconciled by 
# pooling them with the overflow values.
#
# When the generalized classifiers are evaluated, they will be examined across 
# all columns simultaneously.
mean(multi_primary$n_primary_codes)
max(multi_primary$n_primary_codes)

# Confirm that none of these entries contain the NAICS code.
primary_code_by_addr[
  grepl("813110", primary_codes)
]

# Confirm that none of the entries with multiple primary SIC codes are assigned 
# the NAICS code.
multi_primary[
  grepl("(^| \\| )813110( \\| |$)", primary_codes)
]


# For ABI/address combinations with multiple primary SIC codes, one will be 
# randomly selected and the remaining codes will be moved to the overflow columns.

# 1) Pick ONE primary row per (abi,address): keep the first by code (deterministic)
keep_primary <- primary_long[
  !is.na(code)
][order(abi, address, code)][
  , .SD[1L], by = .(abi, address)
]

# 2) Everything else from primary_long gets pushed into overflow_long
demote_to_overflow <- primary_long[
  !keep_primary, on = .(abi, address, code)
]

# 3) primary_long becomes exactly one primary per (abi,address)
primary_long <- keep_primary


## --------------------
## SUBSECTION B2: Normalize Overflow SIC Codes

# Isolate unique overflow SIC outcomes by ABI/address.
overflow_long <- rbindlist(list(
  church_2026_form_analysis_dt[
    !(is.na(na_if_blank_chr(sic_code)) & is.na(na_if_blank_chr(sic6_descriptions_sic))),
    .(abi, address,
      code = na_if_blank_chr(sic_code),
      desc = na_if_blank_chr(sic6_descriptions_sic))
  ],
  church_2026_form_analysis_dt[
    !(is.na(na_if_blank_chr(sic_code_1)) & is.na(na_if_blank_chr(sic6_descriptions_sic1))),
    .(abi, address,
      code = na_if_blank_chr(sic_code_1),
      desc = na_if_blank_chr(sic6_descriptions_sic1))
  ],
  church_2026_form_analysis_dt[
    !(is.na(na_if_blank_chr(sic_code_2)) & is.na(na_if_blank_chr(sic6_descriptions_sic2))),
    .(abi, address,
      code = na_if_blank_chr(sic_code_2),
      desc = na_if_blank_chr(sic6_descriptions_sic2))
  ],
  church_2026_form_analysis_dt[
    !(is.na(na_if_blank_chr(sic_code_3)) & is.na(na_if_blank_chr(sic6_descriptions_sic3))),
    .(abi, address,
      code = na_if_blank_chr(sic_code_3),
      desc = na_if_blank_chr(sic6_descriptions_sic3))
  ],
  church_2026_form_analysis_dt[
    !(is.na(na_if_blank_chr(sic_code_4)) & is.na(na_if_blank_chr(sic6_descriptions_sic4))),
    .(abi, address,
      code = na_if_blank_chr(sic_code_4),
      desc = na_if_blank_chr(sic6_descriptions_sic4))
  ]
), use.names = TRUE, fill = TRUE)


# Append demoted primary codes into overflow pool (treat same as any overflow)
overflow_long <- rbindlist(list(
  overflow_long,
  demote_to_overflow[, .(abi, address, code, desc)]
), use.names = TRUE, fill = TRUE)

# De-duplicate
overflow_long <- unique(overflow_long, by = c("abi","address","code","desc"))


# With the data deduplicated and structured so that each entry has one primary 
# SIC code with all remaining codes expanded into overflow columns, the 
# transformation can be applied, leaving one row per unique ABI/address 
# combination.

# 1) Make a combined pool of candidates per abi/address and de-duplicate by code
#    (prevents the same SIC showing up twice across overflow sources)
pool <- rbindlist(list(
  primary_long[,  .(abi, address, priority = 0L, code, desc)],
  overflow_long[, .(abi, address, priority = 1L, code, desc)]
), use.names = TRUE, fill = TRUE)[!(is.na(code) & is.na(desc))]

# If code is the real identifier, dedupe by (abi,address,code) and keep a non-NA 
# desc if present
setorder(pool, abi, address, code, priority)  # primary preferred if duplicate code appears
pool <- pool[
  , .(desc = desc[which.max(!is.na(desc))][1L], priority = min(priority)),
  by = .(abi, address, code)
]

# 2) Force exactly one primary per abi/address (if multiple remain, take first by code)
primary_by_key <- pool[priority == 0L][order(abi, address, code)][
  , .SD[1L], by = .(abi, address)
]

# Everything else becomes overflow candidates, excluding the chosen primary code
overflow_by_key <- pool[
  !primary_by_key, on = .(abi, address, code)
]

# 3) Number overflows sequentially within each abi/address
setorder(overflow_by_key, abi, address, code)
overflow_by_key[, overflow_n := seq_len(.N), by = .(abi, address)]  # 1,2,3,...


# Most entries have one overflow SIC code, with a maximum of 11 additional 
# codes recorded.
mean(overflow_by_key$overflow_n)
max(overflow_by_key$overflow_n)


# 4) Cast to wide: overflow_sic_1, overflow_sic_2, ... (and desc columns)
overflow_wide_code <- dcast(
  overflow_by_key,
  abi + address ~ overflow_n,
  value.var = "code"
)
setnames(
  overflow_wide_code,
  old = setdiff(names(overflow_wide_code), c("abi","address")),
  new = paste0("overflow_sic_", setdiff(names(overflow_wide_code), c("abi","address")))
)

overflow_wide_desc <- dcast(
  overflow_by_key,
  abi + address ~ overflow_n,
  value.var = "desc"
)
setnames(
  overflow_wide_desc,
  old = setdiff(names(overflow_wide_desc), c("abi","address")),
  new = paste0("overflow_sic_desc_", setdiff(names(overflow_wide_desc), c("abi","address")))
)

# Cast the SIC code and description columns as wide.
sic_wide <- Reduce(function(x, y) merge(x, y, by = c("abi","address"), all = TRUE), list(
  primary_by_key[, .(abi, address,
                     primary_sic = code,
                     primary_sic_desc = desc)],
  overflow_wide_code,
  overflow_wide_desc
))

# Reorder columns so each overflow_sic_i is immediately followed by 
# overflow_sic_desc_i.
setcolorder(
  sic_wide,
  c(
    "abi", "address",
    "primary_sic", "primary_sic_desc",
    as.vector(rbind(
      paste0("overflow_sic_", 1:11),
      paste0("overflow_sic_desc_", 1:11)
    ))
  )
)


## --------------------
## SUBSECTION B3: Assign Religious Classifiers

# As noted earlier, each ABI and address entry has at least one primary SIC code 
# and, in some cases, one or more overflow SIC codes. All relevant descriptions 
# appear in the primary column, with a few additional ones found only in the 
# overflow columns.
#
# This means some ABI and address entries may be associated with more than one 
# religious category. Additionally, ABIs with multiple addresses may file under 
# different religious codes.
#
# We therefore assess their classification consistency; specifically, whether 
# any entries are filed under multiple religious categories, and how frequently 
# this occurs.


# Confirm the crossover between primary SIC codes and macro classifications.
sic_classifications$sic_desc[sic_classifications$sic_desc %!in% unique(primary_long$desc)]
primary_long$desc[primary_long$desc %!in% unique(sic_classifications$sic_desc)]

# Generate the consistency checks.
christian_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Christian Church",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

muslim_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Muslim Mosque",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

jewish_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Jewish Synagogue",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

sikh_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Sikh Gurdwara",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

hindu_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Hindu Mandir",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

buddhist_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Buddhist Temple",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)

other_results <- sic_desc_split_summary_dt(
  church_2026_form_analysis_dt = church_2026_form_analysis_dt,
  target_classification = "Other Religion",
  sic_classifications = sic_classifications,
  primary_long = primary_long,
  overflow_long = overflow_long
)


#' @description
#' Codebook for classification consistency checks across seven religious
#' categories. The goal is to assess how consistently each ABI and address
#' entry is labeled across the primary and overflow SIC columns; specifically,
#' whether any entries are filed under multiple religious categories, and
#' how frequently this occurs.
#' 
#' Each file has the following sheets:
#' @field primary_pct100 Filters the primary SIC code column for descriptions
#'                       that are consistent across all addresses filed by
#'                       an ABI.
#' @field primary_pctlt100 Filters the primary SIC code column for descriptions
#'                         that are inconsistent across all addresses filed by
#'                         an ABI. Each observed outcome is listed individually,
#'                         rather than as pairs.
#' @field overflow_pct100 Same as primary_pct100, but applied to all 
#'                        deduplicated overflow column outcomes.
#' @field overflow_pctlt100 Same as primary_pctlt100, but applied to all
#'                          deduplicated overflow column outcomes.
#' 
#' Each sheet has the following variables:
#' @field x The SIC description.
#' @field Freq The percentage of times the description appeared across all
#'             search hits.

sheets <- 
  #christian_results %>%
  #muslim_results %>%
  #jewish_results %>%
  #sikh_results %>%
  #hindu_results %>%
  #buddhist_results %>%
  other_results %>%
  (\(x) {list(
  "primary_pct100"      = x$primary$pct100,
  "primary_pctlt100"    = x$primary$pctlt100,
  "overflow_pct100"     = x$overflow$pct100,
  "overflow_pctlt100"   = x$overflow$pctlt100
)})

write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Christian_08.15.2026.xlsx")
write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Muslim_08.15.2026.xlsx")
write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Jewish_08.15.2026.xlsx")

write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Sikh_08.15.2026.xlsx")
write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Hindu_08.15.2026.xlsx")
write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Buddhist_08.15.2026.xlsx")

write_xlsx(sheets, path = "./Data/Results/From Clean Raw Data/Step 3_2026 Format/SIC Codes Consistency Check_Other_08.15.2026.xlsx")


# Classifiers were assigned based on clear attribution to a specific religion. 
# However, a few descriptions showed a surprising degree of overlap across 
# religions. The most notable example is the SIC description "CHURCHES". Others, 
# such as "RELIGIOUS ORGANIZATIONS", "PLACES OF WORSHIP", and "CONVENTS & 
# MONASTERIES", were too vague to attribute to a single religion and were 
# therefore labeled "Interfaith".
#
# In the following section, classifiers are assigned based on the primary and 
# overflow SIC codes. If, at the ABI level, only one religious classifier 
# appears alongside "Interfaith" entries, all entries for that ABI will be 
# assigned that religion. This override is only applied when a single religion 
# is identified; ABIs with multiple religions listed will retain their original 
# classifications.
# 
# Some descriptions, such as "CHURCHES" and "CHURCH ORGANIZATIONS", are 
# primarily associated with Christian organizations and will therefore be 
# attributed to "Christian Church" in the absence of other clear religious 
# identifiers. This approach may miss cases where these descriptions apply to 
# interfaith ministries, but will reduce the overattribution of interfaith 
# classifications, which prior results suggest is likely to occur.
#
# An exception is made for entries with the SIC description "SYNAGOGUES 
# MESSIANIC", which will be attributed to both "Christian Church" and "Jewish 
# Synagogue".


# Convert to a data table.
primary_dt   <- as.data.table(primary_long)
overflow_dt  <- as.data.table(overflow_long)
sic_classifications_dt <- as.data.table(sic_classifications)

# Ensure codes are stored as characters.
primary_dt[,  code_chr := as.character(code)]
overflow_dt[,  code_chr := as.character(code)]
sic_classifications_dt[, sic_code_chr := as.character(sic_code)]

# Join the primary SIC table with the classifiers.
dt_p <- merge(
  primary_dt[, .(abi, address, code = code_chr)],
  sic_classifications_dt,
  by.x = "code", by.y = "sic_code_chr",
  all.x = TRUE,
  allow.cartesian = TRUE
) %>%
  select(-code)

# Join the overflow SIC table with the classifiers and remove unmatched records.
dt_o <- merge(
  overflow_dt[, .(abi, address, code = code_chr)],
  sic_classifications_dt,
  by.x = "code", by.y = "sic_code_chr",
  all.x = TRUE,
  allow.cartesian = TRUE
) %>% 
  filter(!is.na(classification)) %>%
  select(-code)

# Combine the primary and overflow results.
dt_all <- bind_rows(dt_p, dt_o)


# These SIC/classification records can be messy: the same address may show 
# multiple labels, and some “Christian Church” SICs are frequently used as a 
# generic or interfaith-style tag rather than a true Christian identifier.
#
# What we do here:
# - For each ABI+address, collect any explicit religion labels from `religions`.
# - If at least one explicit religion is present, we do NOT keep "Interfaith"
#   (Interfaith is only used when no explicit religion labels are found).
# - Handle “soft Christian” SICs (866104, 866125, 866107, 866108):
#     These SICs sometimes trigger a "Christian Church" label in a 
#     generic/interfaith way. We only drop "Christian Church" when there is 
#     exactly ONE non-Christian religion present at the address and the 
#     Christian evidence comes ONLY from these soft SICs. If Christian is the 
#     only religion present (i.e., no other religion labels appear), we keep 
#     "Christian Church".
# 
#     The “soft Christian” rule triggers only when exactly one non-Christian 
#     religion is present; if there are 2+ non-Christian religions plus 
#     soft-Christian, "Christian Church" will remain included (by design per 
#     this rule).
#
# Output:
# `abi_addr_religion` is long: one row per ABI+address+religion. "Interfaith" 
# appears only when the address has no explicit religion labels.

religions <- c(
  "Hindu Mandir", "Christian Church", "Other Religion",
  "Jewish Synagogue", "Muslim Mosque", "Buddhist Temple", "Sikh Gurdwara"
)
soft_christian_sic <- c(866104L, 866125L, 866107L, 866108L)

# Clean the classification variables.
dt_all[, classification := trimws(classification)]

abi_addr_religion <- dt_all[
  , {
    # Collapse to the distinct SIC/classification signals observed at this 
    # ABI+address
    u <- unique(.SD[, .(sic_code, classification)])
    
    # Collect explicit religion labels (only those in `religions`; excludes 
    # "Interfaith")
    rel <- intersect(u$classification, religions)
    
    # Identify any non-Christian religions present at this ABI+address
    rel_non_chr <- setdiff(rel, "Christian Church")
    
    # Soft-Christian override:
    # If there is exactly one non-Christian religion present, and any 
    # "Christian Church" evidence comes only from the soft_christian_sic codes, 
    # then treat the address as that single non-Christian religion (drop 
    # "Christian Church" from rel).
    if (length(rel_non_chr) == 1L) {
      # SIC codes contributing to the "Christian Church" label at this address 
      # (if any)
      christian_sic <- unique(u[classification == "Christian Church", sic_code])
      
      # Only override when Christian appears AND all its SIC codes are 
      # soft-Christian
      if (length(christian_sic) > 0L && all(christian_sic %in% soft_christian_sic)) {
        rel <- rel_non_chr
      }
    }
    
    # De-duplicate and stabilize ordering for consistent downstream behavior
    rel <- sort(unique(rel))
    
    # Emit long-format output:
    # - one row per explicit religion when present
    # - otherwise a single "Interfaith" row when no religion labels are found
    if (length(rel) > 0L) {
      data.table(religion = rel)         # one row per religion
    } else {
      data.table(religion = "Interfaith")
    }
  },
  by = .(abi, address)                    # compute separately for each ABI+address group
]


# After building address-level religion labels, we apply a conservative 
# ABI-level cleanup:
# - If an ABI has exactly ONE distinct non-"Interfaith" religion across all its 
#   addresses, we treat that as the ABI’s “consensus” religion.
# - We then replace "Interfaith" at the address level ONLY for that ABI, using 
#   the consensus religion. This fills in likely missing/unspecified religions 
#   at Interfaith-only addresses.
# 
# Guardrails (what we do NOT change):
# - If an ABI has multiple non-Interfaith religions, we do not propagate 
#   anything—leave all address-level labels as-is (including any "Interfaith" rows).
# - If an ABI has no non-Interfaith religion (all Interfaith), nothing changes.
# - We never overwrite a specific religion label at an address; only 
#   "Interfaith" can be replaced.

# Build ABI consensus table (as before)
abi_single <- abi_addr_religion[
  religion != "Interfaith",
  .(abi_religion = if (uniqueN(religion) == 1L) unique(religion) else NA_character_),
  by = abi
]

# Start from full table, then update (never drops rows)
abi_addr_religion_fixed <- copy(abi_addr_religion)

# Add abi_religion via lookup (unmatched ABIs become NA)
abi_addr_religion_fixed[
  abi_single, on = "abi", abi_religion := i.abi_religion
]

# Replace only Interfaith when abi_religion is defined
abi_addr_religion_fixed[
  religion == "Interfaith" & !is.na(abi_religion),
  religion := abi_religion
][
  , abi_religion := NULL
]


# Collapse to ABI+address with “any” flags.
classify_long <- abi_addr_religion_fixed[!is.na(religion),
            .(present = TRUE),
            by = .(abi, address, religion)]

# Cast the classifiers column to wide as boolean.
classify_wide <- dcast(
  classify_long,
  abi + address ~ religion,
  value.var = "present",
  fill = FALSE
)

# Replace NA with FALSE in classification columns
cls_cols <- setdiff(names(classify_wide), c("abi", "address"))
for (cc in cls_cols) set(classify_wide, which(is.na(classify_wide[[cc]])), cc, FALSE)


# Prior classifications labeled ambiguous entries as "Interfaith" because they
# could not be disambiguated alone, but may be resolvable in the presence of
# other, clearer classifiers. At this juncture, it is important to clarify that
# these remaining cases are actually unspecified. An "Interfaith" column will
# also be created to represent all cases where more than one religion is TRUE
# for a given entry.

# Rename existing Interfaith -> Non-specific Religious
setnames(classify_wide, old = "Interfaith", new = "Unspecified")

# 2) Define the religion flag columns to check for multi-TRUE on each row
relig_cols <- c(
  "Buddhist Temple", "Christian Church", "Hindu Mandir", "Jewish Synagogue",
  "Muslim Mosque", "Sikh Gurdwara", "Other Religion", "Unspecified"
)

# keep only columns that actually exist (avoids name-mismatch errors)
relig_cols <- intersect(relig_cols, names(classify_wide))

# 3) New Interfaith: TRUE if 2+ of those columns are TRUE on that row
classify_wide[, Interfaith := Reduce(`+`, lapply(.SD, function(x) x %in% TRUE)) >= 2L,
              .SDcols = relig_cols]

# Rename columns with spaces so they are easier to manage in parquet.
colnames(classify_wide)[-c(1:2)] <-
  tolower(gsub(" ", "_", colnames(classify_wide)[-c(1:2)]))

setcolorder(
  classify_wide,
  c(
    "abi", "address",
    "buddhist_temple", "christian_church", "hindu_mandir",
    "jewish_synagogue", "muslim_mosque", "sikh_gurdwara",
    "other_religion", "interfaith", "unspecified"
  )
)


## --------------------
## SUBSECTION B4: Compile the Final SIC Wide Columns

# Combine the SIC code, description, and classifier columns.
final_sic_classified_wide <- merge(
  sic_wide,
  classify_wide,
  by = c("abi", "address"),
  all.x = TRUE,
  allow.cartesian = TRUE
)


# Count the number of entries associated with each religious category.

# Collapse to one row per ABI: did this ABI ever have TRUE?
abi_flag <- final_sic_classified_wide[, .(
  buddhist_temple  = any(buddhist_temple %in% TRUE),
  christian_church = any(christian_church %in% TRUE),
  hindu_mandir     = any(hindu_mandir %in% TRUE),
  jewish_synagogue = any(jewish_synagogue %in% TRUE),
  muslim_mosque    = any(muslim_mosque %in% TRUE),
  sikh_gurdwara    = any(sikh_gurdwara %in% TRUE),
  other_religion   = any(other_religion %in% TRUE),
  interfaith       = any(interfaith %in% TRUE),
  unspecified      = any(unspecified %in% TRUE)
), by = abi]

# Generate tables where one row represents one variable's results.
out <- melt(abi_flag, measure.vars = colnames(abi_flag)[-1],
            variable.name = "field", value.name = "val")[,
                                                         .N, by = .(field, val)
            ][,
              pct := 100 * N / sum(N), by = field
            ][,
              dcast(.SD, field ~ val, value.var = "pct", fill = 0)
            ]

# Ensure consistent column names/order
setnames(out, old = c("FALSE", "TRUE"), new = c("FALSE", "TRUE"), skip_absent = TRUE)

# Cleanup
out[, `:=`(
   `FALSE` = fifelse(is.na(`FALSE`), 0, round(`FALSE`, 2)),
   `TRUE`  = fifelse(is.na(`TRUE`),  0, round(`TRUE`,  2))
)]

out[]

# As anticipated, most entries were associated with a Christian church (76%).
# The next largest category was Unspecified at approximately 22%, followed by
# Judaism at 1.7%. All other religions fell below 1%.




## ----------------------------------------------------------------
## PART C: GENERATE THE YEARS-OPEN BINARY COLUMNS

## --------------------
## SUBSECTION C1: Reshape Years-Open Entries from Rows to Columns

# To support downstream metric calculations, the data must be reshaped from 
# long format (years as rows) to wide format (years as binary columns).

# Verify that the year column contains no NA values.
any(is.na(church_2026_form_analysis_dt$archive_version_year))

# Isolate unique presence per abi/address/year.
yr_long <- unique(
  church_2026_form_analysis_dt[
    !is.na(archive_version_year),
    .(abi, address, archive_version_year)
  ]
)

# As expected, each unique ABI/address/year combination is represented exactly 
# once in the current data.
nrow(church_2026_form_analysis_dt) == nrow(yr_long)

# Cast the year column to wide as binary.
yr_wide <- dcast(
  yr_long,
  abi + address ~ archive_version_year,
  fun.aggregate = length,
  value.var = "archive_version_year",
  fill = 0L
)

# Verify that the two transformed metadata columns have the same number of rows, 
# each representing a unique ABI/address combination.
nrow(final_sic_classified_wide) == nrow(yr_wide)


## --------------------
## SUBSECTION C2: Fill Minor Years Reported Gaps

# A closure is defined as an event in which four or more consecutive years have 
# no filings under any address. To simplify metric calculations, these missing 
# years will be filled in. Care must be taken to ensure that this process does 
# not induce duplicate records in the event of temporary relocations within 
# intervening years.

dt_filled <- copy(yr_wide)  # Create a copy to avoid modifying the original DataFrame.
yr <- names(dt_filled)[grepl("^\\d{4}$", names(dt_filled))]  # Identify year columns.


# The first pass will do a simple fill of all 0-gaps of length <= k = 3 between
# 1's, ignoring any induced duplications.

handlers("txtprogressbar")  # Choose console progress bar style
dt_filled <- with_progress({
  fill_gaps_leq_k(dt_filled, yr = yr, k = 3L,
                  add_filled_col = TRUE,
                  add_gap_stats  = TRUE,
                  show_progress  = TRUE,
                  progress_every = 200L)
})

# Find ABIs where the general fill created any year with >1 addresses open.
abi_year_sums <- dt_filled[, lapply(.SD, sum), by = abi, .SDcols = yr]
all_zero_or_one <- abi_year_sums[
  , .(all_zero_or_one = all(unlist(.SD) %in% c(0L, 1L))),
  by = abi
]


# About 3% of ABIs had an induced duplicate record from the gap filling.
round(prop.table(table(all_zero_or_one$all_zero_or_one, useNA = "ifany"))*100, digits = 2)

# Extract the affected ABIs for correction in the next step.
bad_abi <- all_zero_or_one[all_zero_or_one == FALSE, abi]


# 9.2% of ABI/address entries required gap filling.
round(prop.table(table(dt_filled$gap_filled, useNA = "ifany"))*100, digits = 2)

# For most ABIs, only one qualifying gap was filled (89%), of which 62% were 
# 1-year gaps, 25% were 2-year gaps, and 8% were 3-year gaps.
round(prop.table(table(dt_filled[gap_filled == TRUE, ]$n_gaps_filled))*100, digits = 4)
round(prop.table(table(dt_filled[gap_filled == TRUE, ]$avg_gap_len_filled))*100, digits = 4)

# As the number of gaps filled increases, so does the average length of those gaps.
round(prop.table(table(
  "Avg Length Filled" = dt_filled[gap_filled == TRUE, ]$avg_gap_len_filled,
  "# Gaps Filled" = dt_filled[gap_filled == TRUE, ]$n_gaps_filled
), margin = 2)*100, digits = 2) %>%
  (\(x) { x[x == 0] <- NA; x })()


# Apply a special gap fill to the affected ABIs where a duplicate was induced; 
# if an address is already observed (= 1) in a given year, do not overwrite 
# that value.
with_progress({
  final_yr_wide <- fix_bad_abi_flagfill(yr_wide, dt_filled, bad_abi, yr, flag = 50L)
})

# Assess the performance of the secondary filling procedure to confirm
# that no ABI has more than one address open in any given year.
abi_year_sums <- final_yr_wide[, lapply(.SD, sum), by = abi, .SDcols = yr]
abi_zero_or_one <- abi_year_sums[
  , .(all_zero_or_one = all(unlist(.SD) %in% c(0L, 1L))),
  by = abi
]

# No duplicate records remain across the year columns.
round(prop.table(table(abi_zero_or_one$all_zero_or_one))*100, digits = 2)
final_yr_wide[abi %in% abi_zero_or_one[all_zero_or_one == FALSE, abi], ]




## ----------------------------------------------------------------
## PART D: IDENTIFYING AND QUANTIFYING MOVES VS. REOPENINGS AT NEW LOCATION

# Moves are defined as any change of address, including returns to a previous 
# address. However, this excludes cases where more than four years elapsed 
# between point a and point b. In such cases, the event is considered a 
# reopening at a new location rather than a move.


dist_long <- church_2026_form_analysis_dt %>%
  select(archive_version_year, abi, address, latitude, longitude)

setorder(dist_long, abi, archive_version_year)

# 1) Create an episode id that increments when address changes within an abi
dist_long[, episode_id := rleid(address), by = abi]

# 2) Collapse each episode to first/last year + one set of coords
episodes <- dist_long[, .(
  from_year = min(archive_version_year),
  to_year   = max(archive_version_year),
  address   = address[1],
  latitude  = latitude[1],
  longitude = longitude[1]
), by = .(abi, episode_id)]

setorder(episodes, abi, from_year)

# 3) Previous episode info (this is address A, with its LAST year = prev_to_year)
episodes[, `:=`(
  prev_address = shift(address),
  prev_to_year = shift(to_year),
  prev_lat     = shift(latitude),
  prev_lon     = shift(longitude)
), by = abi]

# 4) Gap you actually want: first year of B minus last year of A
episodes[, year_gap := from_year - prev_to_year]

# 5) Calculate the missing years = year_gap - 1, so threshold 4 => year_gap >= 5
gap_threshold_missing_years <- 4L
episodes[, transition_type := fifelse(
  is.na(prev_address), NA_character_,
  fifelse((year_gap - 1L) >= gap_threshold_missing_years, "Reopened (New Location)", "Moved")
)]

# 6) Distance between episode endpoints (A -> B)
episodes[, calculatable := !(is.na(prev_lat) | is.na(prev_lon) | is.na(latitude) | is.na(longitude))]

# dist_km first
episodes[, dist_km := fifelse(
  is.na(prev_address), NA_real_,
  fifelse(calculatable,
          round(geosphere::distHaversine(
            cbind(prev_lon, prev_lat),
            cbind(longitude, latitude)
          ) / 1000, 2),
          NA_real_)
)]

# then your move-threshold flags
episodes[, `:=`(
  move_gt_5mi  = !is.na(dist_km) & dist_km >  5 * 1.609344,
  move_gt_10mi = !is.na(dist_km) & dist_km > 10 * 1.609344,
  move_gt_25mi = !is.na(dist_km) & dist_km > 25 * 1.609344
)]

# then dist_flag (or any other labels)
episodes[, dist_flag := fifelse(
  is.na(prev_address), NA_character_,
  fifelse(calculatable, "Calculatable", "Uncalculatable")
)]

# 7) Step-to-step output
abi_step_dist <- episodes[!is.na(prev_address),
                          .(abi,
                            from_address = prev_address,
                            to_address   = address,
                            from_last_year = prev_to_year,   # last year at A
                            to_first_year  = from_year,      # first year at B
                            year_gap,
                            missing_years = pmax(year_gap - 1L, 0L),
                            dist_km,
                            move_gt_5mi,
                            move_gt_10mi,
                            move_gt_25mi,
                            dist_flag,
                            transition_type)
]


# Approximately 26% of entries involved a move or reopening at a new location.
round(nrow(abi_step_dist)/nrow(yr_wide)*100, digits = 2)

# Most moves were less than 5 miles, with only ~14% exceeding 5 miles. Most 
# relocations were also less than 5 miles, with ~17% exceeding 5 miles. As 
# expected, the share of addresses beyond 10 and 25 miles decreased 
# incrementally: ~5% and ~1%, respectively.
round(prop.table(table(
  abi_step_dist$transition_type, 
  "> 5 mi" = abi_step_dist$move_gt_5mi, 
  useNA = "ifany"
), margin = 1)*100, digits = 2)
round(prop.table(table(abi_step_dist$transition_type, "> 10 mi" = abi_step_dist$move_gt_10mi, useNA = "ifany"))*100, digits = 2)
round(prop.table(table(abi_step_dist$transition_type, "> 25 mi" = abi_step_dist$move_gt_25mi, useNA = "ifany"))*100, digits = 2)


# Detect common PO Box patterns: "PO Box", "P.O. Box", "P O BOX", "Post Office Box"
po_pat <- "(?i)\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b|\\bPOST\\s+OFFICE\\s+BOX\\b"

# row-level flags (if you still want them)
abi_step_dist[, `:=`(
  from_is_pobox = !is.na(from_address) & grepl(po_pat, from_address, perl = TRUE),
  to_is_pobox   = !is.na(to_address)   & grepl(po_pat, to_address, perl = TRUE)
)]
abi_step_dist[, any_pobox := from_is_pobox | to_is_pobox]

# per-ABI summary
pobox_by_abi <- abi_step_dist[, .(
  any_pobox_abi = any(any_pobox, na.rm = TRUE),            # did this ABI ever involve a PO Box?
  n_steps       = .N,
  n_any_pobox   = sum(any_pobox, na.rm = TRUE),            # how many transitions involved a PO Box?
  n_from_pobox  = sum(from_is_pobox, na.rm = TRUE),
  n_to_pobox    = sum(to_is_pobox, na.rm = TRUE)
), by = abi]

# About 40% of businesses included a move that involved a PO Box.
round(prop.table(table(pobox_by_abi$any_pobox, useNA = "ifany"))*100, digits = 2)


#' @description
#' Codebook for the data frame quantifying move events. Some addresses appear
#' more than once due to intervening alternative addresses. This may result
#' from duplicate address records that were not collapsed during cleaning, the 
#' presence of PO Boxes, or potentially temporary relocations. If the gap 
#' between filings at two distinct addresses exceeds 4 years, the transition is 
#' reclassified as a reopening.
#'
#' @field abi The business ID to which the results relate.
#' @field from_address The address from which the entry relocated. This is also
#'                     listed as the primary address for that entry.
#' @field to_address The address to which the entry relocated. These are the 
#'                   addresses quantified by the move variables.
#' @field from_last_year The last year on file for the origin address.
#' @field to_first_year The first year on file for the destination address.
#' @field year_gap The number of years between the two filings.
#' @field missing_years The number of years within the gap where no address was 
#'                      filed.
#' @field dist_km The haversine distance between the two address coordinates, 
#'                in km.
#' @field move_gt_[5mi|10mi|25mi] Boolean. TRUE if the move distance exceeds
#'                                5, 10, or 25 miles, respectively.
#' @field dist_flag Boolean. TRUE if the distance can be calculated, i.e., 
#'                  coordinates are non-missing and sufficient address 
#'                  information is available.
#' @field transition_type Classifies the address change as "Moved" or
#'                        "Reopening (New Location)" if the gap between filings 
#'                        exceeds 4 years.
#' @field from_is_pobox Boolean. TRUE if the origin address is a PO Box; FALSE 
#'                      otherwise.
#' @field to_is_pobox Boolean. TRUE if the destination address is a PO Box; 
#'                    FALSE otherwise.
#' @field any_pobox Boolean. TRUE if either address is a PO Box; FALSE otherwise.

# # Save result.
# write_parquet(abi_step_dist, "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 3_2026 Format/Moves and Reopenings at New Address_08.16.2026.parquet")

# Load in the pre-produced results.
abi_step_dist <- read_parquet("./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 3_2026 Format/Moves and Reopenings at New Address_08.16.2026.parquet") %>%
  as.data.table()


## ----------------------------------------------------------------
## PART E: RECONSTRUCT ALL COLUMNS AND SAVE RESULT

# Over the prior sections three metadata were generated: standardized and
# classified SIC codes, reshaped years-open information as binary columns,
# and quantifying moves.

# Coerce to data.table (doesn't copy if already DT)
a <- as.data.table(final_sic_classified_wide)[, .(abi, address)]
b <- as.data.table(final_yr_wide)[, .(abi, address)]
c <- as.data.table(abi_step_dist)[, .(abi, from_address)]

# (Recommended) normalize types/whitespace to avoid false mismatches
a[, `:=`(abi = as.character(abi), address = trimws(as.character(address)))]
b[, `:=`(abi = as.character(abi), address = trimws(as.character(address)))]
c[, `:=`(abi = as.character(abi), address = trimws(as.character(from_address)))]

c <- c[, !"from_address"]

# 1) Same set of (abi,address) pairs (order doesn't matter)?
same_pairs  <- setequal(a, b)
same_pairs2 <- setequal(a, c)

same_pairs
same_pairs2

# 2) Diagnostics: what’s in one but not the other?
only_in_a <- fsetdiff(a, b)   # pairs in final_sic_classified_wide not in final_yr_wide
only_in_b <- fsetdiff(b, a)   # pairs in final_yr_wide not in final_sic_classified_wide

only_in_ca <- fsetdiff(c, a)  # pairs in final_sic_classified_wide not in abi_step_dist
only_in_ac <- fsetdiff(a, c)  # pairs in abi_step_dist not in final_sic_classified_wide

list(
  n_pairs_a = nrow(unique(a)),
  n_pairs_b = nrow(unique(b)),
  n_pairs_c = nrow(unique(c)),
  n_only_in_a = nrow(only_in_a),
  n_only_in_b = nrow(only_in_b),
  n_only_in_ca = nrow(only_in_ca),  # only in a not in c
  n_only_in_ac = nrow(only_in_ac)   # only in c not in a
)

# This confirms that all ABI and address combinations in sets a and b are 
# identical, while those in set c differ. This is expected, as the process of 
# quantifying moves focuses only on entries where a move occurred.

# Reconstruct with all the metadata and expanded columns, join by abi/address
metadata <- church_2026_form_analysis_dt[
  , !c("archive_version_year", "latitude", "longitude",
       names(church_2026_form_analysis_dt)[grepl("sic", names(church_2026_form_analysis_dt), ignore.case = TRUE)]),
  with = FALSE
] %>%
  distinct(abi, address, .keep_all = TRUE)

# Summarize abi_step_dist at the origin (from_address) level.
# Result: one row per (abi, from_address). We keep:
# - to_address: all destination addresses seen from that origin (collapsed into 
#   one string)
# - transition_type: all unique transition types from that origin (collapsed)
# - dist_km: maximum distance observed from that origin
# - move_gt_*: TRUE if any move from that origin exceeds the threshold
# - any_pobox: TRUE if any step from that origin was flagged as PO Box-related
abi_step_from_by_addr <- abi_step_dist %>%
  transmute(abi, address = from_address, to_address, transition_type,
            move_gt_5mi, move_gt_10mi, move_gt_25mi, dist_km, any_pobox) %>%
  filter(!is.na(address)) %>%
  group_by(abi, address) %>%
  summarise(
    n_moves = dplyr::n(),
    to_address = paste(sort(unique(na.omit(to_address))), collapse = "; "),
    transition_type = paste(sort(unique(na.omit(transition_type))), collapse = "; "),
    mean_dist_km = suppressWarnings(mean(dist_km, na.rm = TRUE)),
    max_dist_km = suppressWarnings(max(dist_km, na.rm = TRUE)),
    move_gt_5mi  = any(move_gt_5mi  %in% TRUE, na.rm = TRUE),
    move_gt_10mi = any(move_gt_10mi %in% TRUE, na.rm = TRUE),
    move_gt_25mi = any(move_gt_25mi %in% TRUE, na.rm = TRUE),
    any_pobox    = any(any_pobox    %in% TRUE, na.rm = TRUE),
    .groups = "drop"
  )

# Most entries were classified as a move (98%), with only 0.06% containing 
# multiple categories.
round(prop.table(table(abi_step_from_by_addr$transition_type, abi_step_from_by_addr$any_pobox))*100, digits = 2)

# Most ABI/address entries had 1 associated move event (95%), with the next 
# most common being 2 move events. The maximum number of moves detected per 
# ABI/address was 5.
round(prop.table(table(abi_step_from_by_addr$n_moves))*100, digits = 3)


#' @description
#' Codebook for new output fields produced during the data cleaning and
#' validation step. All other fields were present in the Step 2 form of
#' the data.
#'
#' @field 2000:2025 Binary columns indicating years open and closed.
#' @field gap_filled Boolean. TRUE if a qualifying gap between flanking
#'                   1's has been filled; FALSE otherwise.
#' @field n_gaps_filled The number of gaps filled for that address.
#' @field avg_gap_len_filled The average length, in years, of filled
#'                           gaps; ranges from 1 to 3 sequential years.
#' @field n_moves Number of times a given ABI/address was identified
#'                as a from_address, indicating a new move event.
#' @field to_address The address to which the current entry relocated.
#'                   These are the addresses quantified by the move
#'                   variables.
#' @field transition_type Classifies the type of address transition.
#'                        Gaps exceeding four consecutive years are
#'                        classified as reopenings at a new location
#'                        rather than moves. Addresses appearing
#'                        multiple times temporally with a temporary
#'                        new address may have multiple transitions
#'                        associated per ABI/address.
#' @field mean_dist_km The mean haversine distance (km) across all address
#'                     transitions. For entries appearing more than once,
#'                     this reflects the average distance across all moves;
#'                     otherwise, it is the distance of the single move.
#' @field max_dist_km  The maximum haversine distance (km) across all address
#'                     transitions. For entries appearing more than once,
#'                     this reflects the largest single-move distance observed;
#'                     otherwise, it is the distance of the single move.
#' @field move_gt_[5mi|10mi|25mi] Boolean. TRUE if any of the moves exceeds
#'                                5, 10, or 25 miles, respectively.
#' @field any_pobox Boolean. TRUE if a PO Box is present for either
#'                  the from_ or to_address; FALSE otherwise.
#' @field primary_sic...primary_sic_desc,
#'        `overflow_sic_[1:11]...overflow_sic_desc_[1:11]`
#'        Standardized primary and overflow SIC columns, made
#'        consistent across all ABI/address entries.
#' @field buddhist_temple, `christian_church`,
#'        `hindu_mandir`, `interfaith`,
#'        `jewish_synagogue`, `muslim_mosque`,
#'        `other_religion`, `sikh_gurdwara`
#'        Boolean. TRUE if the address is classified under the given
#'        religion; FALSE otherwise.


# Combine all four separate tables
combined <- final_yr_wide %>%
  full_join(metadata, by = c("abi","address")) %>%
  full_join(abi_step_from_by_addr, by = c("abi","address")) %>%
  full_join(final_sic_classified_wide, by = c("abi","address")) %>%
  relocate(address_verified, .after = address) %>%
  relocate(address_matched, .after = address_verified)

# Save result.
write_parquet(combined, "./Data/Results/KEEP LOCAL/From Clean Raw Data/Step 3_2026 Format/church_2026_form_wide_annotated_08.16.2026.parquet")


