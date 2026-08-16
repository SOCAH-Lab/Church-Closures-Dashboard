## ----------------------------------------------------------------
## 
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 12th, 2026
## Date Modified: August 15th, 2026
## 
## Description: 
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
##    - PART A: Exclude Metadata Not Conducive to the Analysis
## 
##    - PART B: Exclude Metadata Not Conducive to the Analysis
##        * SUBSECTION B1: Primary SIC Codes Across Unique ABI/Address Combinations
##        * SUBSECTION B2: Overflow SIC Codes Across Unique ABI/Address Combinations
##        * SUBSECTION B3: 
##        * SUBSECTION B4: Compile the Final SIC Wide Columns
## 
##    - PART C: Generate the Years-Open Binary Columns
##        * SUBSECTION C1: Reshape Years-Open Entries from Rows to Columns
##        * SUBSECTION C2: Fill Minor Years Reported Gaps
## 
##    - PART D: 
##    - PART E: Reconstruct All Columns and Save Result

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
## PART A: Exclude Metadata Not Conducive to the Analysis

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
## PART B: Normalize SIC Codes Across Unique ABI/Address Combinations

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
## SUBSECTION B1: Primary SIC Codes Across Unique ABI/Address Combinations

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
## SUBSECTION B2: Overflow SIC Codes Across Unique ABI/Address Combinations

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


## --------------------
## SUBSECTION B3: 

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


#' @description
#' Codebook for classification consistency checks across six religious
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
  buddhist_results %>%
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


# One key finding is that "Churches" appear across all religions and should 
# therefore be labeled as "Interfaith". Additionally, while classifications
# will be generated over primary SIC codes only, they will be generated over
# overflow as well.

# Isolate the SIC codes that are overflow only.
in_primary    <- sic_classifications[sic_classifications$sic_desc %!in% c("SYNAGOGUES MESSIANIC", "TEMPLES BUDDHIST EDUCATIONAL INSTITUTION"), ]
overflow_only <- sic_classifications[sic_classifications$sic_desc %in% c("SYNAGOGUES MESSIANIC", "TEMPLES BUDDHIST EDUCATIONAL INSTITUTION"), ]




# If your description column in primary_dt is named something else, change desc_col
code_col <- "code"   # e.g., set to "sic_desc" or whatever primary_long uses

primary_dt  <- as.data.table(primary_long)
primary_sic <- as.data.table(in_primary)

primary_dt[,  code_chr := as.character(code)]
primary_sic[, sic_code_chr := as.character(sic_code)]

# Join by sic_desc (primary_dt desc -> primary_sic sic_desc)
dtj <- merge(
  primary_dt[, .(abi, address, code = code_chr)],
  primary_sic,
  by.x = "code", by.y = "sic_code_chr",
  all.x = TRUE,
  allow.cartesian = TRUE
) %>%
  select(-code)


overflow_dt  <- as.data.table(overflow_long)
overflow_sic <- as.data.table(overflow_only)

overflow_dt[,  code_chr := as.character(code)]
overflow_sic[, sic_code_chr := as.character(sic_code)]

dtk <- merge(
  overflow_dt[, .(abi, address, code = code_chr)],
  overflow_sic,
  by.x = "code", by.y = "sic_code_chr",
  all.x = TRUE,
  allow.cartesian = TRUE
) %>% 
  filter(!is.na(classification)) %>%
  select(-code)


dt <- bind_rows(dtj, dtk)


# 3) Collapse to ABI+address “any” flags
classify_long <- dt[!is.na(classification),
            .(present = TRUE),
            by = .(abi, address, classification)]

classify_wide <- dcast(
  classify_long,
  abi + address ~ classification,
  value.var = "present",
  fill = FALSE
)


# Replace NA with FALSE in classification columns
cls_cols <- setdiff(names(classify_wide), c("abi", "address"))
for (cc in cls_cols) set(classify_wide, which(is.na(classify_wide[[cc]])), cc, FALSE)


## --------------------
## SUBSECTION B4: Compile the Final SIC Wide Columns

final_sic_wide <- Reduce(function(x, y) merge(x, y, by = c("abi","address"), all = TRUE), list(
  primary_by_key[, .(abi, address,
                     primary_sic = code,
                     primary_sic_desc = desc)],
  overflow_wide_code,
  overflow_wide_desc
))

# Reorder columns so each overflow_sic_i is immediately followed by 
# overflow_sic_desc_i
setcolorder(
  final_sic_wide,
  c(
    "abi", "address",
    "primary_sic", "primary_sic_desc",
    as.vector(rbind(
      paste0("overflow_sic_", 1:11),
      paste0("overflow_sic_desc_", 1:11)
    ))
  )
)




## ----------------------------------------------------------------
## PART C: Generate the Years-Open Binary Columns

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
nrow(final_sic_wide) == nrow(yr_wide)


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
## PART D:


# Annotate with moves

# Values
#   - Distance to walk depreciated
#   - Distance to take public transit depreciated
#   - Distance to drive depreciated
#   - Within 1 mile
#   - Within 1-5 miles
#   - Within 5-10 miles
#   - Within 10-50 miles

# Just calculate the closure and summarize the movement
# i.e.: 10% moved more than 1-5 miles away, 5% had a drop of walk ability.
# i.e.: 4% detected more than 1 community (80% two and 20% three)
# 
# Only closed due to a move if it moved outside of the community. Also note
# if later addresses fell into previously identified community.




## ----------------------------------------------------------------
## PART E: Reconstruct All Columns and Save Result

#Reconstruct with all the metadata and expanded columns, join by abi/address
church_2026_form_analysis_dt[
  , !c("archive_version_year",
       names(church_2026_form_analysis_dt)[grepl("sic", names(church_2026_form_analysis_dt), ignore.case = TRUE)]),
  with = FALSE
]


final_sic_wide
final_yr_wide







