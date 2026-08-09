## ----------------------------------------------------------------
## Define functions used in the Step 1 script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: June 23rd, 2026
## 
## Description: This script defines functions specific to Step 1 of the data
##              cleaning and validation process. These supplement the
##              general-purpose functions defined in a separate script, and
##              were developed in response to findings from the initial
##              exploratory data analysis, improvements identified from
##              processing the 2023 Formatted data, and variations
##              encountered in the process data update script.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions:
##    1. sic_fix_tables:  Build SIC “fix” tables from sic_results. Creates 
##       three helper tables used to review/repair SIC code/description issues:
##            1) fix_desc: rows are sic_code; columns list the multiple sic_desc 
##               values seen for that code
##            2) fix_code: rows are sic_desc; columns list the multiple sic_code 
##               values seen for that desc
##            3) fix_na: rows are sic_code; columns list unique sic_desc values, 
##               treating NA as the string "NA"

## ----------------------------------------------------------------
## FUNCTIONS

sic_fix_tables <- function(sic_results) {
  #' Build SIC “fix” tables from sic_results. Creates three helper tables used 
  #' to review/repair SIC code/description issues:
  #' 1) fix_desc: rows are sic_code; columns list the multiple sic_desc values seen for that code
  #' 2) fix_code: rows are sic_desc; columns list the multiple sic_code values seen for that desc
  #' 3) fix_na:   rows are sic_code; columns list unique sic_desc values, treating NA as the string "NA"
  #'
  #' @param sic_results A list containing, at minimum:
  #'   - presence_wide (data.frame/tibble) with columns sic_code and sic_desc
  #'   - codes_with_multiple_desc (data.frame/tibble) with column sic_code
  #'   - desc_with_multiple_codes (data.frame/tibble) with column sic_desc
  #'   - desc_that_is_sometimes_na (data.frame/tibble) with column sic_desc
  #'
  #' @return A named list with elements:
  #'   - fix_desc (data.frame)
  #'   - fix_code (data.frame)
  #'   - fix_na   (data.frame)
  
  # ---- basic checks ---------------------------------------------------------
  stopifnot(is.list(sic_results), "presence_wide" %in% names(sic_results))
  
  presence_wide <- sic_results$presence_wide
  
  # ---- 1) Entries to correct DESCRIPTION -----------------------------------
  # Goal: For sic_codes that appear with multiple descriptions, make one row per
  # sic_code and spread the multiple descriptions across columns sic_desc_1, _2, ...
  fix_desc <- presence_wide %>%
    # keep only codes that are known to map to multiple descriptions
    dplyr::filter(sic_code %in% sic_results$codes_with_multiple_desc$sic_code) %>%
    # keep just the mapping columns
    dplyr::select(sic_code, sic_desc) %>%
    # order for readability/reproducibility
    dplyr::arrange(sic_code) %>%
    # create an index within each sic_code to label duplicates
    dplyr::group_by(sic_code) %>%
    dplyr::mutate(dup_id = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    # widen: sic_desc_1, sic_desc_2, ...
    tidyr::pivot_wider(
      id_cols      = sic_code,
      names_from   = dup_id,
      values_from  = sic_desc,
      names_prefix = "sic_desc_"
    ) %>%
    as.data.frame()
  
  # ---- 2) Entries to correct CODE ------------------------------------------
  # Goal: For descriptions that appear with multiple codes, make one row per
  # sic_desc and spread associated codes across columns sic_code_1, _2, ...
  fix_code <- presence_wide %>%
    # keep only descriptions known to map to multiple codes
    dplyr::filter(sic_desc %in% sic_results$desc_with_multiple_codes$sic_desc) %>%
    # keep just the mapping columns
    dplyr::select(sic_desc, sic_code) %>%
    # remove duplicates so we only list unique code<->desc links
    dplyr::distinct() %>%
    # create an index within each description
    dplyr::group_by(sic_desc) %>%
    dplyr::mutate(code_idx = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    # widen: sic_code_1, sic_code_2, ...
    tidyr::pivot_wider(
      names_from   = code_idx,
      values_from  = sic_code,
      names_prefix = "sic_code_"
    ) %>%
    as.data.frame()
  
  # ---- 3) Entries to correct NA DESCRIPTION --------------------------------
  # Goal: For descriptions that are sometimes missing (NA), identify all sic_codes
  # involved and show their unique descriptions, treating NA explicitly as "NA".
  fix_na <- presence_wide %>%
    # restrict to sic_codes that appear under a description known to be sometimes NA
    dplyr::semi_join(
      presence_wide %>%
        dplyr::filter(sic_desc %in% sic_results$desc_that_is_sometimes_na$sic_desc) %>%
        dplyr::distinct(sic_code),
      by = "sic_code"
    ) %>%
    # keep just the mapping columns
    dplyr::select(sic_code, sic_desc) %>%
    # unique links only
    dplyr::distinct() %>%
    # make NA explicit so it shows up as a value in the wide output
    dplyr::mutate(sic_desc = dplyr::if_else(is.na(sic_desc), "NA", as.character(sic_desc))) %>%
    # index within each sic_code to spread across columns
    dplyr::group_by(sic_code) %>%
    dplyr::mutate(desc_idx = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    # widen: sic_desc_1, sic_desc_2, ... (including "NA" if present)
    tidyr::pivot_wider(
      names_from   = desc_idx,
      values_from  = sic_desc,
      names_prefix = "sic_desc_"
    ) %>%
    as.data.frame()
  
  # ---- return ---------------------------------------------------------------
  list(
    fix_desc = fix_desc,
    fix_code = fix_code,
    fix_na   = fix_na
  )
}



