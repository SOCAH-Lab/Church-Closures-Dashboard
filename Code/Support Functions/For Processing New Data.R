## ----------------------------------------------------------------
## Define functions used to assess the raw data update.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 11th, 2026
## Date Modified: May 11th, 2026
## 
## Description: In addition to the general-purpose functions defined in another
##              script, the following functions are used to evaluate the
##              contents of additional data provided by Professor Yusuf Ransome
##              May 9th, 2026.
## 
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions:
##    1. overflow_check: Verifies that SIC overflow columns follow a strict 
##       left-justified pattern within each row. Concretely, if the last 
##       (right-most) non-missing SIC value for a row is in position *k*, then 
##       columns 1..k must all be non-`NA`, and columns (k+1)..N must all be 
##       `NA`. Rows with all SIC columns `NA` are treated as valid.
## 
##    2. summarize_code_ranges: Summarize contiguous year ranges for a code by 
##       ABI + address (data.table)
## 
##       For each ABI/address pair, this creates a single string describing each
##       distinct `code_col` value and the contiguous year ranges in which it 
##       appears, e.g. `"A (2018-2020), B (2022)"`.
## 
##       Rows are deduplicated at the ABI/address/year/code level before 
##       computing runs. ABI/address pairs with no non-missing 
##       `code_col`+`year_col` observations are retained and return `NA` in the 
##       output column.
## 
##    3. summarize_many_code_ranges_dt: Summarize contiguous year ranges for 
##       many code columns (with progress bar)
## 
##       Calls `summarize_code_ranges()` for each variable in `vars` and merges 
##       results by ABI/address. A console progress bar is displayed using 
##       `utils::txtProgressBar()`.
## 
##    4. sic_overflow_audit: Audits a dataset that stores SIC classifications 
##       in a primary column plus multiple “overflow” columns (e.g., 
##       `sic_code_1` ... `sic_code_4`) with a corresponding set of description 
##       columns. The function:
##            1) runs an overflow-pattern check on codes and descriptions,
##            2) identifies rows where the description overflow pattern fails 
##               (often due to missing descriptions),
##            3) quantifies overlap in (code, desc) pairs across sources,
##            4) builds a unified (sic_code -> sic_desc) mapping table and 
##               checks one-to-one consistency, and
##            5) identifies SIC codes that appear sometimes with NA descriptions 
##               and sometimes with non-NA descriptions.
## 
##    5. clean_sic_descs: Clean SIC description columns with a single string 
##       replacement. Applies `stringr::str_replace()` to a set of SIC 
##       description columns, replacing the first match of `pattern` in each 
##       value with `replacement`.
## 
##    6. clean_sic_codes: Clean SIC code columns with a single string 
##       replacement. Applies `stringr::str_replace()` to a set of SIC *code* 
##       columns (e.g., `sic_code`, `sic_code_1`, ...), replacing the first 
##       match of `pattern` in each value with `replacement`.
##       
##       By default, matching is literal (`fixed = TRUE`). Set `fixed = FALSE` 
##       to treat `pattern` as a regular expression.
## 
##    7. set_sic_desc_for_code: Set/overwrite SIC descriptions for a given SIC 
##       code (across paired columns)
##       
##       For each paired (code, description) column set—e.g.,
##       `sic_code` <-> `sic6_descriptions_sic`, `sic_code_1` <-> `sic6_descriptions_sic1`, etc.—
##       this function overwrites the description with `new_desc` wherever the 
##       SIC code equals `target_code`.
##       
##       Column names listed in `sic_code_cols` / `sic_desc_cols` that are not 
##       present in `df` are ignored; only existing, aligned pairs are updated.
## 
##    8. avail_years: Expand an "available years" string into a sorted unique 
##       vector of years. Parses strings like `"2002-2005, 2007, 2010-2012"` 
##       into an integer vector. Used by [coverage_checks()] (and therefore 
##       indirectly by [check_ranges_same_outcome()]).
## 
##    9. normalize_year: Normalize a year token into a 4-digit integer year and 
##       keeps digits only. If more than 4 digits are present, uses the *last 4* 
##       digits (e.g., `"20011"` -> `2011`). Used by [year_value_map()] (and 
##       therefore indirectly by [coverage_checks()] and [check_ranges_same_outcome()]).
## 
##   10. year_value_map: Parse a ranges string into a year -> value map (and 
##       detect conflicting overlaps). Parses `"A (2002), B (2003-2005)"` into 
##       a mapping from each year to a value label. Overlapping years with 
##       different labels are returned as conflicts. Used by [coverage_checks()] 
##       (and therefore indirectly by [check_ranges_same_outcome()]).
## 
##   11. compress_years: Compress a set of years into "start:end" runs.
##       `c(2003,2004,2005,2010,2012,2013)` -> `"2003:2005, 2010, 2012:2013"`.
##       Used by [coverage_checks()] (and therefore indirectly by 
##       [check_ranges_same_outcome()]).
## 
##   12. coverage_checks: Compute coverage/uniqueness and "decade consistency" 
##       checks for one row. Used by [check_ranges_same_outcome()] to evaluate 
##       each `(available_year_ranges, *_ranges)` pair row-by-row.
## 
##   13. check_ranges_same_outcome: Check all `*_ranges` columns against an 
##       availability column. Main entry point. For every column ending in 
##       `ranges_suffix`, computes: `{col}_ok`, `{col}_decade_ok`, 
##       `{col}_missing_years`, `{col}_values_seen`,  `{col}_conflict_years`.
##       Internally, it calls [coverage_checks()] for each row/column pair.


## ----------------------------------------------------------------
## FUNCTIONS


overflow_check <- function(df, sic_cols = sic_code_cols) {
  #' @description
  #' Verifies that SIC overflow columns follow a strict left-justified pattern
  #' within each row. Concretely, if the last (right-most) non-missing SIC value
  #' for a row is in position *k*, then columns 1..k must all be non-`NA`, and
  #' columns (k+1)..N must all be `NA`. Rows with all SIC columns `NA` are treated
  #' as valid.
  #'
  #' This is useful when SIC values are stored across multiple “overflow” columns
  #' (e.g., `sic_code`, `sic_code_1`, ..., `sic_code_4`) and you want to confirm
  #' that later overflow slots are never filled unless all earlier slots are filled.
  #'
  #' @param df A data frame containing the SIC overflow columns.
  #' @param sic_cols A character vector of column names (ordered left-to-right)
  #'   representing the overflow sequence to validate.
  #'
  #' @return The input `df` with two additional columns:
  #' \describe{
  #'   \item{last_pos}{Integer. The 1-based index of the last non-`NA` overflow
  #'     column in the row (0 if all SIC columns are `NA`).}
  #'   \item{overflow_ok}{Logical. `TRUE` if the row satisfies the left-justified
  #'     overflow pattern, otherwise `FALSE`.}
  #' }
  
  # Extract only the SIC columns, keeping them in the specified order, and
  # coerce to a matrix for fast, vectorized row/column operations.
  m <- as.matrix(df[, sic_cols, drop = FALSE])
  
  # Logical matrix: TRUE where a value is present, FALSE where it's NA.
  not_na <- !is.na(m)
  
  # (Not strictly needed below, but often useful for clarity/debugging.)
  n <- ncol(m)
  
  # Compute the position of the last non-NA entry in each row.
  # max.col(..., ties.method="last") returns the last column index of the
  # maximum value (TRUE > FALSE), so it effectively finds the last TRUE.
  # Note: for rows with all FALSE (all NA), max.col returns 1, so we fix to 0.
  last_pos <- max.col(not_na, ties.method = "last")
  last_pos[rowSums(not_na) == 0] <- 0L
  
  # Count how many SIC columns are non-NA in each row.
  n_non_na <- rowSums(not_na)
  
  # Check that all non-NA values form a contiguous block from the left:
  # count TRUEs that appear in columns <= last_pos; this should equal last_pos.
  prefix_ok <- rowSums(not_na & (col(not_na) <= last_pos)) == last_pos
  
  # A row is valid if:
  # - it has exactly last_pos non-NAs (so there are no “holes” before last_pos),
  # - and prefix_ok is TRUE (so everything up to last_pos is filled).
  df %>%
    dplyr::mutate(
      last_pos = last_pos,
      overflow_ok = (n_non_na == last_pos) & prefix_ok
    )
}




summarize_code_ranges <- function(df, code_col, abi_col = "abi", address_col = "combined_address", year_col = "archive_version_year") {
  #' @description
  #' Summarize contiguous year ranges for a code by ABI + address (data.table)
  #'
  #' For each ABI/address pair, this creates a single string describing each
  #' distinct `code_col` value and the contiguous year ranges in which it appears,
  #' e.g. `"A (2018-2020), B (2022)"`.
  #'
  #' Rows are deduplicated at the ABI/address/year/code level before computing
  #' runs. ABI/address pairs with no non-missing `code_col`+`year_col` observations
  #' are retained and return `NA` in the output column.
  #'
  #' @param df A data.frame/data.table containing ABI, address, code, and year fields.
  #' @param code_col Character scalar. Column name of the code/value to summarize.
  #' @param abi_col Character scalar. Column name for ABI. Default `"abi"`.
  #' @param address_col Character scalar. Column name for address. Default `"combined_address"`.
  #' @param year_col Character scalar. Column name for the year field. Default `"archive_version_year"`.
  #'
  #' @return A data.frame with columns `{abi_col}`, `{address_col}`, and
  #'   `{paste0(code_col, "_ranges")}`.
  #' @export
  
  stopifnot(is.character(code_col), length(code_col) == 1)
  out_col <- paste0(code_col, "_ranges")
  
  # Convert to data.table (no copy if df already is one)
  DT <- data.table::as.data.table(df)
  
  # Build the ABI/address key set we always want to keep in the output
  keys <- unique(
    DT[!is.na(get(abi_col)) & !is.na(get(address_col)),
       .(abi = get(abi_col), address = get(address_col))]
  )
  
  # ---- Overall "available" contiguous year ranges per ABI/address (ignores code_col) ----
  Y <- DT[
    !is.na(get(abi_col)) &
      !is.na(get(address_col)) &
      !is.na(get(year_col)),
    .(
      abi     = get(abi_col),
      address = get(address_col),
      .year   = suppressWarnings(as.integer(get(year_col)))
    )
  ][!is.na(.year)]  # drop rows where year can't be parsed
  
  if (nrow(Y) == 0L) {
    # No valid years anywhere: everyone gets NA for availability
    avail <- keys[, .(available_year_ranges = NA_character_)]
  } else {
    # Sort + de-duplicate years within ABI/address
    data.table::setorder(Y, abi, address, .year)
    Y <- unique(Y, by = c("abi", "address", ".year"))
    
    # Mark runs of consecutive years (new run when year != previous + 1)
    Y[, .run := cumsum(.year != data.table::shift(.year, type = "lag", fill = .year[1L]) + 1L),
      by = .(abi, address)]
    
    # Summarize each run to start/end
    avail_runs <- Y[, .(start_year = min(.year), end_year = max(.year)),
                    by = .(abi, address, .run)]
    
    # Format run as "YYYY" or "YYYY-YYYY"
    avail_runs[, date_range := data.table::fifelse(
      start_year == end_year,
      as.character(start_year),
      paste0(start_year, "-", end_year)
    )]
    
    # Collapse all runs to one string per ABI/address
    data.table::setorder(avail_runs, abi, address, start_year)
    avail <- avail_runs[, .(available_year_ranges = paste(date_range, collapse = ", ")),
                        by = .(abi, address)]
  }
  
  # ---- Code-specific contiguous year ranges per ABI/address/code ----
  X <- DT[
    !is.na(get(abi_col)) &
      !is.na(get(address_col)) &
      !is.na(get(code_col)) &
      !is.na(get(year_col)),
    .(
      abi     = get(abi_col),
      address = get(address_col),
      .year   = suppressWarnings(as.integer(get(year_col))),
      .code   = as.character(get(code_col))
    )
  ][!is.na(.year)]
  
  if (nrow(X) == 0L) {
    # No valid code+year rows: return keys + availability + NA code ranges
    out <- merge(keys, avail, by = c("abi", "address"), all.x = TRUE, sort = FALSE)
    out[, (out_col) := NA_character_]
  } else {
    # Sort + de-duplicate at ABI/address/year/code
    data.table::setorder(X, abi, address, .code, .year)
    X <- unique(X, by = c("abi", "address", ".year", ".code"))
    
    # Mark runs of consecutive years within each ABI/address/code
    X[, .run := cumsum(.year != data.table::shift(.year, type = "lag", fill = .year[1L]) + 1L),
      by = .(abi, address, .code)]
    
    # Summarize each run to start/end
    runs <- X[, .(start_year = min(.year), end_year = max(.year)),
              by = .(abi, address, .code, .run)]
    
    # Format run as "YYYY" or "YYYY-YYYY"
    runs[, date_range := data.table::fifelse(
      start_year == end_year,
      as.character(start_year),
      paste0(start_year, "-", end_year)
    )]
    
    # Create "CODE (range)" items and collapse per ABI/address
    data.table::setorder(runs, abi, address, start_year, .code)
    runs[, .item := paste0(.code, " (", date_range, ")")]
    
    res <- runs[, .(val = paste(.item, collapse = ", ")), by = .(abi, address)]
    data.table::setnames(res, "val", out_col)
    
    # Merge everything back to full key set
    out <- merge(keys, avail, by = c("abi", "address"), all.x = TRUE, sort = FALSE)
    out <- merge(out,  res,  by = c("abi", "address"), all.x = TRUE, sort = FALSE)
    
    # Normalize empty strings to NA
    out[, (out_col) := data.table::fifelse(get(out_col) == "", NA_character_, get(out_col))]
  }
  
  # Restore original column names
  data.table::setnames(out, c("abi", "address"), c(abi_col, address_col))
  
  # Put available_year_ranges right after combined_address (address_col)
  data.table::setcolorder(
    out,
    c(
      abi_col, address_col, "available_year_ranges",
      setdiff(names(out), c(abi_col, address_col, "available_year_ranges"))
    )
  )
  
  as.data.frame(out)
}




summarize_many_code_ranges_dt <- function(df, vars, abi_col = "abi", address_col = "combined_address", year_col = "archive_version_year") {
  #' @description
  #' Summarize contiguous year ranges for many code columns (with progress bar)
  #'
  #' Calls `summarize_code_ranges()` for each variable in `vars` and merges results by ABI/address.
  #' A console progress bar is displayed using `utils::txtProgressBar()`.
  #'
  #' @param df A data.frame/data.table containing ABI, address, and year fields.
  #' @param vars Character vector of column names to summarize (each becomes `*_ranges`).
  #' @param abi_col Character scalar. Column name for ABI. Default `"abi"`.
  #' @param address_col Character scalar. Column name for address. Default `"combined_address"`.
  #' @param year_col Character scalar. Column name for the year field. Default `"archive_version_year"`.
  #'
  #' @return A data.frame with one row per ABI/address and one `*_ranges` column per
  #'   element of `vars`.
  #' @export
  
  stopifnot(is.character(vars), length(vars) >= 1L)
  
  DT <- as.data.table(df)
  
  # Compute the base output ONCE (includes available_year_ranges)
  base <- summarize_code_ranges(
    df,
    code_col = vars[[1]],
    abi_col = abi_col,
    address_col = address_col,
    year_col = year_col
  )
  
  # Convert base to data.table and standardize key names for merging
  out <- as.data.table(base)
  setnames(out, c(abi_col, address_col), c("abi", "address"))
  
  pb <- utils::txtProgressBar(min = 0, max = length(vars), style = 3)
  on.exit(close(pb), add = TRUE)
  
  for (i in seq_along(vars)) {
    v <- vars[[i]]
    newcol <- paste0(v, "_ranges")
    
    tmp <- summarize_code_ranges(
      df,
      code_col = v,
      abi_col = abi_col,
      address_col = address_col,
      year_col = year_col
    )
    
    tmpDT <- as.data.table(tmp)
    setnames(tmpDT, c(abi_col, address_col), c("abi", "address"))
    
    # Drop availability column(s) from tmp (we keep the one from base)
    tmpDT[, grep("^available_year_ranges(\\.|$)", names(tmpDT), value = TRUE) := NULL]
    
    # If out already has this *_ranges column, don't merge a second copy
    if (newcol %in% names(out)) {
      tmpDT[, (newcol) := NULL]
    }
    
    # Merge once (your original code merged twice, which guarantees duplicates)
    out <- merge(out, tmpDT, by = c("abi", "address"), all.x = TRUE, sort = FALSE)
    
    utils::setTxtProgressBar(pb, i)
  }
  
  # Restore original ABI/address column names
  setnames(out, c("abi", "address"), c(abi_col, address_col))
  
  # Ensure available_year_ranges is right after the address column
  setcolorder(
    out,
    c(
      abi_col, address_col, "available_year_ranges",
      setdiff(names(out), c(abi_col, address_col, "available_year_ranges"))
    )
  )
  
  as.data.frame(out)
}




sic_overflow_audit <- function(df,
                               sic_code_cols = c("primary_sic_code","sic_code","sic_code_1","sic_code_2","sic_code_3","sic_code_4"),
                               sic_desc_cols = c("sic6_descriptions","sic6_descriptions_sic","sic6_descriptions_sic1",
                                                 "sic6_descriptions_sic2","sic6_descriptions_sic3","sic6_descriptions_sic4"),
                               id_cols = "abi") {
  #' @description
  #' Audits a dataset that stores SIC classifications in a primary column plus
  #' multiple “overflow” columns (e.g., `sic_code_1` ... `sic_code_4`) with a
  #' corresponding set of description columns. The function:
  #' (1) runs an overflow-pattern check on codes and descriptions,
  #' (2) identifies rows where the description overflow pattern fails (often due to
  #'     missing descriptions),
  #' (3) quantifies overlap in (code, desc) pairs across sources,
  #' (4) builds a unified (sic_code -> sic_desc) mapping table and checks one-to-one
  #'     consistency, and
  #' (5) identifies SIC codes that appear sometimes with NA descriptions and sometimes
  #'     with non-NA descriptions.
  #'
  #' @param df A data.frame/tibble containing SIC code and description columns.
  #' @param sic_code_cols Character vector of SIC code column names, ordered from
  #'   primary to most-overflowed.
  #' @param sic_desc_cols Character vector of SIC description column names aligned
  #'   1:1 with `sic_code_cols`.
  #' @param id_cols Optional character vector of identifier columns to retain when
  #'   returning problematic rows (default: `"abi"`). Only columns present in `df`
  #'   are used.
  #'
  #' @details
  #' This function assumes you have an `overflow_check(df, cols)` helper available
  #' in your environment that returns a data frame with (at least) an `overflow_ok`
  #' logical column describing whether each row follows the expected overflow pattern.
  #'
  #' @return A named list containing:
  #' \describe{
  #'   \item{sic_code_cols_used}{Aligned SIC code columns actually used (present in `df`).}
  #'   \item{sic_desc_cols_used}{Aligned SIC description columns actually used (present in `df`).}
  #'   \item{overflow_checked_code}{Row-level output of `overflow_check()` for code columns.}
  #'   \item{overflow_checked_desc}{Row-level output of `overflow_check()` for description columns.}
  #'   \item{overflow_summary_code}{Counts of rows that follow the progressive overflow pattern (OK vs not OK) for codes.}
  #'   \item{overflow_summary_desc}{Counts of rows that follow the progressive overflow pattern (OK vs not OK) for descriptions.}
  #'   \item{missing_desc_rows}{Subset of rows where description overflow check fails.}
  #'   \item{missing_desc_code}{Unique SIC codes that appear with NA descriptions among failing rows.}
  #'   \item{missing_desc_codes_are_subset_of_no_desc}{Logical check: confirm if all SIC codes correspond to NA description.}
  #'   \item{presence_wide}{Unique (sic_code, sic_desc) pairs widened to show presence by column.}
  #'   \item{presence_tabs}{Tabulations of presence (TRUE/FALSE/NA) by source column.}
  #'   \item{map_tbl}{Unified unique mapping of non-NA (sic_code, sic_desc) pairs.}
  #'   \item{n_unique_pairs}{Number of unique non-NA pairs in `map_tbl`.}
  #'   \item{code_to_desc_consistent}{TRUE if each sic_code maps to exactly one sic_desc.}
  #'   \item{desc_to_code_consistent}{TRUE if each sic_desc maps to exactly one sic_code.}
  #'   \item{codes_with_multiple_desc}{SIC codes mapping to != 1 description (if any).}
  #'   \item{desc_with_multiple_codes}{Descriptions mapping to != 1 code (if any).}
  #'   \item{codes_with_mixed_na_desc}{Non-NA mappings for codes that are NA somewhere else.}
  #' }
  #'
  #' @export
  
  # ---- Basic input validation ----
  stopifnot(
    is.data.frame(df),
    is.character(sic_code_cols),
    is.character(sic_desc_cols),
    length(sic_code_cols) == length(sic_desc_cols)
  )
  
  # ---- Align code/desc pairs to columns that actually exist in df ----
  # Keep only those indices where BOTH the code col and the paired desc col exist.
  code_present <- intersect(sic_code_cols, names(df))
  desc_present <- intersect(sic_desc_cols, names(df))
  
  idx_code <- match(code_present, sic_code_cols)
  idx_desc <- match(desc_present, sic_desc_cols)
  keep_idx <- intersect(idx_code, idx_desc)
  
  sic_code_cols <- sic_code_cols[keep_idx]
  sic_desc_cols <- sic_desc_cols[keep_idx]
  
  # Keep only ID columns that exist.
  id_cols <- intersect(id_cols, names(df))
  
  # ---- Overflow checks (requires overflow_check() in your environment) ----
  overflow_checked_code <- overflow_check(df, sic_code_cols)
  overflow_checked_desc <- overflow_check(df, sic_desc_cols)
  
  # Summaries: how many rows follow the expected pattern vs not.
  overflow_summary_code <- overflow_checked_code %>%
    dplyr::summarise(
      n_rows = dplyr::n(),
      n_ok   = sum(overflow_ok, na.rm = TRUE),
      n_bad  = sum(!overflow_ok, na.rm = TRUE)
    )
  
  overflow_summary_desc <- overflow_checked_desc %>%
    dplyr::summarise(
      n_rows = dplyr::n(),
      n_ok   = sum(overflow_ok, na.rm = TRUE),
      n_bad  = sum(!overflow_ok, na.rm = TRUE)
    )
  
  # Rows where the *description* overflow pattern fails
  # (often because a code exists but its paired description is missing).
  missing_desc <- overflow_checked_desc %>%
    dplyr::filter(overflow_ok == FALSE) %>%
    dplyr::select(dplyr::any_of(c(id_cols, sic_code_cols, sic_desc_cols)))
  
  # ---- Presence/overlap across sources (code+desc pairs) ----
  # Each "source" is a column pairing such as "sic_code_1 + sic6_descriptions_sic1"
  all_sources <- paste0(sic_code_cols, " + ", sic_desc_cols)
  
  # Long table of unique pairs per source (keeping NA on either side so we can see gaps).
  long <- purrr::map2_dfr(sic_code_cols, sic_desc_cols, \(cc, dc) {
    df %>%
      dplyr::transmute(
        sic_code = as.character(.data[[cc]]),
        sic_desc = as.character(.data[[dc]])
      ) %>%
      dplyr::filter(!is.na(sic_code) | !is.na(sic_desc)) %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        source  = paste0(cc, " + ", dc),
        present = TRUE
      )
  })
  
  # Widen to one row per unique (sic_code, sic_desc), with TRUE/FALSE presence by source.
  presence_wide <- long %>%
    dplyr::mutate(source = factor(source, levels = all_sources)) %>%
    tidyr::pivot_wider(
      names_from  = source,
      values_from = present,
      values_fill = FALSE
    )
  
  # Tabulate presence by the *source columns that actually exist* in presence_wide.
  # (Some expected sources may not appear if there were zero observations.)
  source_cols_present <- intersect(all_sources, names(presence_wide))
  presence_tabs <- lapply(source_cols_present, \(nm) table(presence_wide[[nm]], useNA = "ifany"))
  presence_tabs <- do.call(rbind, presence_tabs)
  rownames(presence_tabs) <- source_cols_present
  
  # ---- SIC codes that are missing descriptions (robust to any number of pairs) ----
  # Build all (sic_code, sic_desc) pairs from the failing rows and capture codes with NA desc.
  missing_desc_code <- character(0)
  if (nrow(missing_desc) > 0 && length(sic_code_cols) > 0) {
    
    miss_long <- purrr::map2_dfr(sic_code_cols, sic_desc_cols, \(cc, dc) {
      missing_desc %>%
        dplyr::transmute(
          sic_code = as.character(.data[[cc]]),
          sic_desc = as.character(.data[[dc]])
        )
    })
    
    missing_desc_code <- miss_long %>%
      dplyr::filter(is.na(sic_desc), !is.na(sic_code)) %>%
      dplyr::distinct(sic_code) %>%
      dplyr::pull(sic_code)
  }
  
  # All SIC codes paired with NA descriptions anywhere in the presence table.
  no_desc <- presence_wide %>%
    dplyr::filter(is.na(sic_desc)) %>%
    dplyr::pull(sic_code)
  
  # Sanity check: codes flagged via missing_desc should be a subset of the no-desc codes.
  missing_desc_codes_are_subset_of_no_desc <- all(missing_desc_code %in% no_desc)
  
  # ---- Build a unified mapping table and run consistency checks ----
  # Only non-NA pairs go into the mapping table.
  map_tbl <- purrr::map2_dfr(sic_code_cols, sic_desc_cols, \(cc, dc) {
    df %>%
      dplyr::transmute(
        sic_code = as.character(.data[[cc]]),
        sic_desc = as.character(.data[[dc]])
      ) %>%
      dplyr::filter(!is.na(sic_code), !is.na(sic_desc)) %>%
      dplyr::distinct()
  }) %>%
    dplyr::distinct()
  
  # a) Each sic_code should map to exactly one sic_desc
  code_to_desc <- map_tbl %>%
    dplyr::count(sic_code, name = "n_desc") %>%
    dplyr::filter(n_desc != 1)
  
  # b) Each sic_desc should map to exactly one sic_code
  desc_to_code <- map_tbl %>%
    dplyr::count(sic_desc, name = "n_code") %>%
    dplyr::filter(n_code != 1)
  
  # c) SIC codes that appear with NA descriptions in some source and non-NA in another.
  desc_that_is_sometimes_na <- map_tbl %>%
    dplyr::filter(sic_code %in% no_desc)
  
  # ---- Return a single list output (all artifacts + checks) ----
  list(
    sic_code_cols_used = sic_code_cols,
    sic_desc_cols_used = sic_desc_cols,
    
    overflow_checked_code = overflow_checked_code,
    overflow_checked_desc = overflow_checked_desc,
    overflow_summary_code = overflow_summary_code,
    overflow_summary_desc = overflow_summary_desc,
    
    missing_desc_rows = missing_desc,
    missing_desc_code = missing_desc_code,
    missing_desc_codes_are_subset_of_no_desc = missing_desc_codes_are_subset_of_no_desc,
    
    presence_wide = presence_wide,
    presence_tabs = presence_tabs,
    
    map_tbl = map_tbl,
    n_unique_pairs = nrow(map_tbl),
    
    code_to_desc_consistent = (nrow(code_to_desc) == 0),
    desc_to_code_consistent = (nrow(desc_to_code) == 0),
    codes_with_multiple_desc = code_to_desc,
    desc_with_multiple_codes = desc_to_code,
    
    desc_that_is_sometimes_na = desc_that_is_sometimes_na
  )
}




clean_sic_descs <- function(df,
                            sic_desc_cols = c("sic6_descriptions",
                                              "sic6_descriptions_sic",
                                              "sic6_descriptions_sic1",
                                              "sic6_descriptions_sic2",
                                              "sic6_descriptions_sic3",
                                              "sic6_descriptions_sic4"),
                            pattern, replacement = "", fixed = TRUE) {
  #' @description
  #' Clean SIC description columns with a single string replacement. Applies 
  #' `stringr::str_replace()` to a set of SIC description columns, replacing
  #' the first match of `pattern` in each value with `replacement`.
  #'
  #' @param df A data.frame (or tibble) containing SIC description columns.
  #' @param sic_desc_cols Character vector of column names to clean. Any names not
  #'   present in `df` are ignored.
  #' @param pattern Pattern to match (string or regex) passed to `stringr::str_replace()`.
  #' @param replacement Replacement string passed to `stringr::str_replace()`.
  #'
  #' @return A data.frame with the specified SIC description columns modified.
  
  # Validate basic input types early (fail fast with a clear error)
  stopifnot(is.data.frame(df), is.character(sic_desc_cols))
  
  # Only modify columns that actually exist in `df` (silently ignore missing names)
  cols <- intersect(sic_desc_cols, names(df))
  
  # Apply the replacement across all selected description columns
  pat <- if (fixed) stringr::fixed(pattern) else pattern
  
  dplyr::mutate(
    df,
    dplyr::across(
      dplyr::all_of(cols),
      ~ stringr::str_replace(as.character(.x), pattern = pat, replacement = replacement)
    )
  )
}




clean_sic_codes <- function(df,
                            sic_code_cols = c("primary_sic_code",
                                              "sic_code",
                                              "sic_code_1",
                                              "sic_code_2",
                                              "sic_code_3",
                                              "sic_code_4"),
                            pattern, replacement = "", fixed = TRUE) {
  #' @description
  #' Clean SIC code columns with a single string replacement. Applies 
  #' `stringr::str_replace()` to a set of SIC *code* columns (e.g., `sic_code`, 
  #' `sic_code_1`, ...), replacing the first match of `pattern` in each
  #' value with `replacement`.
  #'
  #' By default, matching is literal (`fixed = TRUE`). Set `fixed = FALSE` to treat
  #' `pattern` as a regular expression.
  #'
  #' @param df A data.frame (or tibble) containing SIC code columns.
  #' @param sic_code_cols Character vector of SIC code column names to clean. Any
  #'   names not present in `df` are ignored.
  #' @param pattern Pattern to match (string or regex) passed to `stringr::str_replace()`.
  #' @param replacement Replacement string passed to `stringr::str_replace()`.
  #' @param fixed Logical. If `TRUE`, treat `pattern` as a literal string via
  #'   `stringr::fixed()`. If `FALSE`, treat `pattern` as a regex.
  #'
  #' @return A data.frame with the specified SIC code columns modified.
  #' @export
  
  # Validate basic input types early (fail fast with a clear error)
  stopifnot(is.data.frame(df), is.character(sic_code_cols))
  
  # Only modify columns that actually exist in `df` (silently ignore missing names)
  cols <- intersect(sic_code_cols, names(df))
  
  # Choose literal vs regex matching for the pattern, and coerce inputs to character
  pat <- if (fixed) stringr::fixed(as.character(pattern)) else as.character(pattern)
  rep <- as.character(replacement)
  
  # Apply the replacement across all selected SIC code columns
  dplyr::mutate(
    df,
    dplyr::across(
      dplyr::all_of(cols),
      ~ stringr::str_replace(as.character(.x), pattern = pat, replacement = rep)
    )
  )
}




set_sic_desc_for_code <- function(df, target_code, new_desc,
                                  sic_code_cols = c("primary_sic_code", "sic_code","sic_code_1","sic_code_2","sic_code_3","sic_code_4"),
                                  sic_desc_cols = c("sic6_descriptions",
                                                    "sic6_descriptions_sic",
                                                    "sic6_descriptions_sic1",
                                                    "sic6_descriptions_sic2",
                                                    "sic6_descriptions_sic3",
                                                    "sic6_descriptions_sic4")) {
  #' @description
  #' Set/overwrite SIC descriptions for a given SIC code (across paired columns)
  #'
  #' For each paired (code, description) column set—e.g.,
  #' `sic_code` <-> `sic6_descriptions_sic`, `sic_code_1` <-> `sic6_descriptions_sic1`, etc.—
  #' this function overwrites the description with `new_desc` wherever the SIC code
  #' equals `target_code`.
  #'
  #' Column names listed in `sic_code_cols` / `sic_desc_cols` that are not present
  #' in `df` are ignored; only existing, aligned pairs are updated.
  #'
  #' @param df A data.frame (or tibble) containing SIC code/description columns.
  #' @param target_code The SIC code whose associated description should be updated.
  #' @param new_desc The description to write wherever the SIC code equals `target_code`.
  #' @param sic_code_cols Character vector of SIC code column names (in pairing order).
  #' @param sic_desc_cols Character vector of SIC description column names (in pairing order).
  #'
  #' @return A data.frame with updated SIC description columns.
  #' @export
  
  # Validate basic input types and that the code/desc vectors define one-to-one pairs
  stopifnot(is.data.frame(df),
            is.character(sic_code_cols), is.character(sic_desc_cols),
            length(sic_code_cols) == length(sic_desc_cols))
  
  # Identify which requested columns actually exist in df
  code_cols_present <- intersect(sic_code_cols, names(df))
  desc_cols_present <- intersect(sic_desc_cols, names(df))
  
  # Keep only aligned (code, desc) pairs that both exist in df
  idx_code <- match(code_cols_present, sic_code_cols)
  idx_desc <- match(desc_cols_present, sic_desc_cols)
  keep_idx <- intersect(idx_code, idx_desc)
  
  code_cols <- sic_code_cols[keep_idx]
  desc_cols <- sic_desc_cols[keep_idx]
  
  # Coerce match key and replacement to character for stable comparisons/assignment
  target_code <- as.character(target_code)
  new_desc    <- as.character(new_desc)
  
  # For each paired column set: if the code matches target_code, overwrite its description
  for (k in seq_along(code_cols)) {
    cc <- code_cols[k]  # code column name
    dc <- desc_cols[k]  # description column name
    
    df[[dc]] <- dplyr::if_else(
      as.character(df[[cc]]) == target_code,
      new_desc,
      as.character(df[[dc]])
    )
  }
  
  # Return the modified data.frame
  df
}




avail_years <- function(x) {
  #' @description
  #' Expand an "available years" string into a sorted unique vector of years.
  #' Parses strings like `"2002-2005, 2007, 2010-2012"` into an integer vector.
  #' Used by [coverage_checks()] (and therefore indirectly by [check_ranges_same_outcome()]).
  #'
  #' @param x Character scalar. Comma-separated list of years or year ranges in the form
  #'   `"YYYY"` or `"YYYY-YYYY"`. Whitespace is allowed.
  #'
  #' @return Integer vector of years (may be length 0 if `x` is empty/NA/unparseable).
  
  # Treat NA/blank as no years
  if (is.na(x) || !nzchar(trimws(x))) return(integer(0))
  
  # Split into tokens on commas (allow whitespace around commas)
  parts <- stringr::str_split(x, "\\s*,\\s*")[[1]]
  
  # Convert each token to years
  yrs <- purrr::map(parts, function(p) {
    # Match YYYY or YYYY-YYYY (with optional whitespace)
    m <- stringr::str_match(p, "^\\s*(\\d{4})(?:\\s*-\\s*(\\d{4}))?\\s*$")
    
    # Ignore unparseable token
    if (anyNA(m)) return(integer(0))
    
    # Start year
    s <- as.integer(m[2])
    
    # End year (or start if missing)
    e <- as.integer(dplyr::if_else(is.na(m[3]), m[2], m[3]))
    
    # Expand inclusive range
    seq.int(s, e)
  })
  
  # Flatten, unique, sort
  sort(unique(unlist(yrs)))
}




normalize_year <- function(y) {
  #' @description
  #' Normalize a year token into a 4-digit integer year and keeps digits only. If 
  #' more than 4 digits are present, uses the *last 4* digits (e.g., `"20011"` -> 
  #' `2011`). Used by [year_value_map()] (and therefore indirectly by
  #' [coverage_checks()] and [check_ranges_same_outcome()]).
  #'
  #' @param y A value coercible to character containing a year.
  #'
  #' @return Integer year (e.g., `2011`) or `NA_integer_` if no digits are found.
  
  # Keep digits only
  y <- gsub("\\D", "", as.character(y))
  
  # No digits => NA
  if (!nzchar(y)) return(NA_integer_)
  
  # If too long, keep last 4 digits (e.g., 20011 -> 2011)
  if (nchar(y) > 4) y <- substr(y, nchar(y) - 3, nchar(y))
  
  # Coerce to integer year
  as.integer(y)
}




year_value_map <- function(x) {
  #' @description
  #' Parse a ranges string into a year -> value map (and detect conflicting overlaps).
  #' Parses `"A (2002), B (2003-2005)"` into a mapping from each year to a value label.
  #' Overlapping years with different labels are returned as conflicts.
  #' Used by [coverage_checks()] (and therefore indirectly by [check_ranges_same_outcome()]).
  #'
  #' @param x Character scalar like `"0 (2002), 1 (2011-2016), 2 (2021-2022)"`.
  #'
  #' @return A list with:
  #' \itemize{
  #'   \item `map`: named character vector (names are years, values are labels).
  #'   \item `conflict_years`: integer vector of years that have >1 distinct label.
  #' }
  
  # Empty input => empty mapping
  if (is.na(x) || !nzchar(trimws(x))) {
    return(list(
      map = setNames(character(0), character(0)),
      conflict_years = integer(0)
    ))
  }
  
  # Extract "<val> (YYYY[-YYYY])" segments
  m <- stringr::str_match_all(
    x,
    "(?:^|,\\s*)([^,()]+?)\\s*\\(\\s*(\\d{4,})(?:\\s*-\\s*(\\d{4,}))?\\s*\\)"
  )[[1]]
  
  # No matches => empty mapping
  if (nrow(m) == 0) {
    return(list(
      map = setNames(character(0), character(0)),
      conflict_years = integer(0)
    ))
  }
  
  # Segment labels
  vals <- stringr::str_trim(m[, 2])
  
  # Start years (normalized)
  starts <- vapply(m[, 3], normalize_year, integer(1))
  
  # End years (or start if missing), normalized
  ends <- vapply(ifelse(is.na(m[, 4]), m[, 3], m[, 4]), normalize_year, integer(1))
  
  # Drop unparseable segments
  keep <- !is.na(starts) & !is.na(ends)
  vals <- vals[keep]
  starts <- starts[keep]
  ends <- ends[keep]
  
  # All segments dropped => empty mapping
  if (length(vals) == 0) {
    return(list(
      map = setNames(character(0), character(0)),
      conflict_years = integer(0)
    ))
  }
  
  # Expand to one row per year
  long <- purrr::map2_dfr(seq_along(vals), vals, function(i, v) {
    # Ignore inverted ranges like "2016-2011"
    if (ends[i] < starts[i]) {
      return(tibble::tibble(year = integer(0), value = character(0)))
    }
    
    # Inclusive expansion
    tibble::tibble(year = seq.int(starts[i], ends[i]), value = v)
  })
  
  # Nothing expanded => empty mapping
  if (nrow(long) == 0) {
    return(list(
      map = setNames(character(0), character(0)),
      conflict_years = integer(0)
    ))
  }
  
  # Years with >1 distinct label (overlaps)
  conflict_years <- long |>
    dplyr::group_by(year) |>
    dplyr::summarise(nv = dplyr::n_distinct(value), .groups = "drop") |>
    dplyr::filter(nv > 1) |>
    dplyr::pull(year)
  
  # First label per year (stable map for downstream checks)
  map_first <- long |>
    dplyr::group_by(year) |>
    dplyr::summarise(value = dplyr::first(value), .groups = "drop")
  
  # Named vector year -> value + conflicts
  list(
    map = setNames(map_first$value, as.character(map_first$year)),
    conflict_years = conflict_years
  )
}




compress_years <- function(years) {
  #' @description
  #' Compress a set of years into "start:end" runs.
  #' `c(2003,2004,2005,2010,2012,2013)` -> `"2003:2005, 2010, 2012:2013"`.
  #' Used by [coverage_checks()] (and therefore indirectly by [check_ranges_same_outcome()]).
  #'
  #' @param years Integer (or coercible) vector of years.
  #'
  #' @return Character scalar. Empty string if `years` is empty.
  
  # Normalize + sort + unique
  years <- sort(unique(as.integer(years)))
  
  # Drop NA
  years <- years[!is.na(years)]
  
  # Nothing to format
  if (length(years) == 0) return("")
  
  # New run when gap != 1
  breaks <- c(TRUE, diff(years) != 1L)
  
  # Run id per year
  grp <- cumsum(breaks)
  
  # Convert each run to "start:end" (or single year)
  parts <- vapply(split(years, grp), function(v) {
    # Single year
    if (length(v) == 1) return(as.character(v[1]))
    
    # Consecutive run
    paste0(v[1], ":", v[length(v)])
  }, character(1))
  
  # Join runs
  paste(parts, collapse = ", ")
}




coverage_checks <- function(avail_str, ranges_str) {
  #' @description
  #' Compute coverage/uniqueness and "decade consistency" checks for one row.
  #' Used by [check_ranges_same_outcome()] to evaluate each `(available_year_ranges, *_ranges)`
  #' pair row-by-row.
  #'
  #' Returns `ok` (single value across all available years) and `decade_ok` (only when `ok` is FALSE:
  #' within each decade, values are consistent; gaps are ignored).
  #'
  #' @param avail_str Character scalar, e.g. `"2002-2022"`.
  #' @param ranges_str Character scalar, e.g. `"0 (2002), 1 (2011-2016), 2 (2021-2022)"`.
  #'
  #' @return Named list: `ok`, `decade_ok`, `missing_years`, `values_seen`, `conflict_years`.
  
  # Expand availability to explicit years
  yrs <- avail_years(avail_str)
  
  # Can't check without available years
  if (length(yrs) == 0) {
    return(list(
      ok = NA,
      decade_ok = NA,
      missing_years = NA_character_,
      values_seen = NA_character_,
      conflict_years = NA_character_
    ))
  }
  
  # Parse mapping + conflicts
  parsed <- year_value_map(ranges_str)
  
  # Named vector year -> value
  m <- parsed$map
  
  # Years as character for matching names()
  avail_chr <- as.character(yrs)
  
  # Missing available years
  missing <- setdiff(avail_chr, names(m))
  
  # Available years that are present in the mapping
  present_years <- intersect(avail_chr, names(m))
  
  # Values for those present available years
  present_vals <- unname(m[present_years])
  
  # Drop empty values defensively
  present_vals <- present_vals[nzchar(present_vals)]
  
  # Distinct values observed across available years
  uniq_vals_all <- unique(present_vals)
  
  # Overall pass/fail: full coverage, no conflicts, and exactly one unique value
  ok <- (length(missing) == 0) &&
    (length(parsed$conflict_years) == 0) &&
    (length(present_vals) == length(avail_chr)) &&
    (length(uniq_vals_all) == 1)
  
  # Decade check only when ok is FALSE
  if (isTRUE(ok)) {
    # Not needed when overall check passes
    decade_ok <- NA
  } else {
    # Fail if nothing is present at all
    if (length(present_years) == 0) {
      decade_ok <- FALSE
    } else {
      # Convert years to decade buckets (e.g., 2011 -> 2010)
      yy <- as.integer(present_years)
      d0 <- (yy %/% 10L) * 10L
      
      # Within each decade, require <= 1 distinct value (ignore gaps)
      decade_ok <- tibble::tibble(decade = d0, value = present_vals) |>
        dplyr::group_by(decade) |>
        dplyr::summarise(nv = dplyr::n_distinct(value), .groups = "drop") |>
        dplyr::summarise(all_one = all(nv <= 1L), .groups = "drop") |>
        dplyr::pull(all_one) |>
        dplyr::first(default = FALSE)
    }
  }
  
  # Return checks + formatted diagnostics
  list(
    ok = ok,
    decade_ok = decade_ok,
    missing_years = compress_years(missing),
    values_seen = if (length(uniq_vals_all) == 0) "" else paste(sort(uniq_vals_all), collapse = ", "),
    conflict_years = compress_years(parsed$conflict_years)
  )
}




check_ranges_same_outcome <- function(df,
                                      available_col = "available_year_ranges",
                                      ranges_suffix = "_ranges") {
  #' @description
  #' Check all `*_ranges` columns against an availability column.
  #' Main entry point. For every column ending in `ranges_suffix`, computes:
  #' `{col}_ok`, `{col}_decade_ok`, `{col}_missing_years`, `{col}_values_seen`, 
  #' `{col}_conflict_years`.
  #'
  #' Internally, it calls [coverage_checks()] for each row/column pair.
  #'
  #' @param df A data.frame/tibble containing an availability column and one or more `*_ranges` columns.
  #' @param available_col Name of the column containing available year ranges.
  #' @param ranges_suffix Suffix used to identify the range columns (default `"_ranges"`).
  #'
  #' @return `df` with additional result columns appended.
  
  stopifnot(available_col %in% names(df))
  
  # Range columns in ORIGINAL order (from df)
  range_cols <- names(df)[stringr::str_ends(names(df), stringr::fixed(ranges_suffix))]
  range_cols <- setdiff(range_cols, available_col)
  
  # Start with original df
  out <- df
  
  # Build outputs (without inserting yet) so we can reorder deterministically
  created_names <- character(0)
  
  for (col in range_cols) {
    base <- stringr::str_remove(col, stringr::fixed(ranges_suffix))
    res  <- purrr::map2(df[[available_col]], df[[col]], coverage_checks)
    
    nm_ok        <- paste0(base, "_ranges_ok")
    nm_decade_ok <- paste0(base, "_ranges_decade_ok")
    nm_values    <- paste0(base, "_values_seen")
    nm_missing   <- paste0(base, "_missing_years")
    nm_conflict  <- paste0(base, "_conflict_years")
    
    out[[nm_ok]]        <- purrr::map_lgl(res, "ok")
    out[[nm_decade_ok]] <- purrr::map(res, "decade_ok") |> unlist(use.names = FALSE)
    out[[nm_values]]    <- purrr::map_chr(res, "values_seen")
    out[[nm_missing]]   <- purrr::map_chr(res, "missing_years")
    out[[nm_conflict]]  <- purrr::map_chr(res, "conflict_years")
    
    created_names <- c(created_names, nm_ok, nm_decade_ok, nm_missing, nm_values, nm_conflict)
  }
  
  # Reorder columns so each block of derived columns sits immediately after its *_ranges column
  # Keep all non-created columns in their original relative order.
  orig <- names(df)
  is_created <- names(out) %in% created_names
  
  ordered <- character(0)
  for (nm in orig) {
    ordered <- c(ordered, nm)
    
    if (nm %in% range_cols) {
      base <- stringr::str_remove(nm, stringr::fixed(ranges_suffix))
      ordered <- c(
        ordered,
        paste0(base, "_ranges_ok"),
        paste0(base, "_ranges_decade_ok"),
        paste0(base, "_values_seen"),
        paste0(base, "_missing_years"),
        paste0(base, "_conflict_years")
      )
    }
  }
  
  # Append any remaining columns (e.g., if df had other columns added elsewhere)
  remaining <- setdiff(names(out), ordered)
  out <- out[, c(ordered, remaining), drop = FALSE]
  
  out
}










