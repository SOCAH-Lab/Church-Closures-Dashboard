## ----------------------------------------------------------------
## Define functions used in the Step 3 script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: August 12th, 2026
## 
## Description: This script defines functions specific to Step 3 of the data
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
##    1. na_if_blank_chr: Convert blank strings to missing values (NA). Trims 
##       leading/trailing whitespace and converts empty strings to 
##       `NA_character_`. Useful for cleaning character columns where blanks 
##       represent missingness.
## 
##    2. fill_gaps_leq_k: Fill bounded zero-gaps (<= k) between ones; vector or 
##       data.table mode. Two modes depending on `x`:
##          - Vector mode: `x` is a 0/1 vector; fill bounded 0-runs of length 
##            <= k.
##          - data.table mode: `x` is a data.table; apply vector mode row-wise 
##            to columns `yr`, optionally add tracking columns and a progress 
##            bar.
## 
##    3. fix_bad_abi_flagfill: Flag-fill short zero-gaps for selected ABIs and 
##       prune conflicts For each ABI in `bad_abi`, this function:
##          1. Takes the corresponding rows from `dt` (not `dt_filled`) and 
##             performs a row-wise gap-fill where short runs of zeros (length 
##             $$\le k$$) that are bounded by ones (pattern $$1 \; 0\ldots0 \; 
##             1$$) are replaced by `flag` (a high sentinel value).
##          2. Prunes (removes) flagged fills in any ABI-year where the column 
##             sum is greater than `flag` (i.e., there is at least one 
##             additional non-zero in that year besides the single flagged fill). 
##             Only entries equal to `flag` are pruned; existing 0/1 values are 
##             left unchanged.
##          3. Converts remaining `flag` values to 1.
## 
##       The result is written into a copy of `dt_filled` for the ABIs in 
##       `bad_abi`.
## 
##    4. sic_desc_split_summary_dt: Summarize SIC6 description distributions 
##       for ABIs tied to a target classification (data.table + progress). 
##       Efficient `data.table` implementation of your SIC6-description summary 
##       workflow with an optional console progress bar.

## ----------------------------------------------------------------
## FUNCTIONS

na_if_blank_chr <- function(x) {
  #' Convert blank strings to missing values (NA). Trims leading/trailing 
  #' whitespace and converts empty strings to `NA_character_`. Useful for 
  #' cleaning character columns where blanks represent missingness.
  #'
  #' @param x A character vector.
  #'
  #' @return A character vector of the same length as `x`, with whitespace trimmed
  #'   and blank entries converted to `NA_character_`.
  #'
  #' @examples
  #' na_if_blank_chr(c("a", "", "   ", NA, " b "))
  #' #> "a" NA NA NA "b"
  
  # Remove leading/trailing whitespace (so "   " becomes "")
  x <- trimws(x)
  
  # Replace empty strings with NA (character NA)
  x[x == ""] <- NA_character_
  
  # Return the cleaned vector
  x
}




fill_gaps_leq_k <- function(x, yr = NULL, k = 3L,
                            add_filled_col = FALSE,
                            filled_col = "gap_filled",
                            add_gap_stats = FALSE,
                            n_gaps_col = "n_gaps_filled",
                            avg_gap_len_col = "avg_gap_len_filled",
                            show_progress = FALSE,
                            progress_every = 100L) {
  #' Fill bounded zero-gaps (<= k) between ones; vector or data.table mode. Two 
  #' modes depending on `x`:
  #' - Vector mode: `x` is a 0/1 vector; fill bounded 0-runs of length <= k.
  #' - data.table mode: `x` is a data.table; apply vector mode row-wise to 
  #'   columns `yr`, optionally add tracking columns and a progress bar.
  #'
  #' @param x Binary vector (0/1) or a `data.table`.
  #' @param yr Character vector of column names to treat as the panel (data.table mode only).
  #' @param k Integer >= 0. Max bounded zero-run length to fill.
  #' @param add_filled_col Logical. Add boolean row flag for any 0->1 fill?
  #' @param filled_col Name of boolean flag column.
  #' @param add_gap_stats Logical. Add per-row gap stats?
  #' @param n_gaps_col Name of integer column counting gaps filled (runs).
  #' @param avg_gap_len_col Name of numeric column with mean length of filled gaps (`NA` if none).
  #' @param show_progress Logical. Emit progress via {progressr} (data.table mode only).
  #' @param progress_every Integer >= 1. Update progress every N rows.
  #'
  #' @return Vector mode: integer vector. data.table mode: modified copy of input `data.table`.
  #'
  #' @examples
  #' fill_gaps_leq_k(c(1,0,1,0,0,1), k = 1L)
  #' \dontrun{
  #' library(data.table)
  #' library(progressr)
  #' handlers("txtprogressbar")
  #' dt2 <- with_progress({
  #'   fill_gaps_leq_k(dt, yr = yr, k = 3L,
  #'                  add_filled_col = TRUE,
  #'                  add_gap_stats  = TRUE,
  #'                  show_progress  = TRUE,
  #'                  progress_every = 200L)
  #' })
  #' }
  
  # -----------------------
  # Mode 1: vector -> vector
  # -----------------------
  # If `x` is not a data.table, treat it as a single binary series.
  if (!data.table::is.data.table(x)) {
    
    # Coerce input to integers for reliable comparisons/assignment.
    v <- as.integer(x)
    
    # Run-length encoding: represent the vector as runs of constant values.
    # r$values  = run values (0/1)
    # r$lengths = run lengths
    r <- rle(v)
    
    # Map each run back to indices in the original vector.
    # e = end index for each run; s = start index.
    e <- cumsum(r$lengths)
    s <- e - r$lengths + 1L
    
    # Identify candidate zero-runs that are "short enough" to fill (length <= k).
    idx0 <- which(r$values == 0L & r$lengths <= k)
    
    # Fill rule:
    # Only fill a zero-run if it is bounded by ones on both sides:
    #   ... 1 | 0 0 ... 0 | 1 ...
    for (j in idx0) {
      
      # Need a left and right neighbor run, and both must be 1.
      if (j > 1L && j < length(r$values) &&
          r$values[j - 1L] == 1L && r$values[j + 1L] == 1L) {
        
        # Convert the whole run from 0 to 1.
        v[s[j]:e[j]] <- 1L
      }
    }
    
    # Return the filled vector.
    return(v)
  }
  
  # ----------------------------
  # Mode 2: data.table -> table
  # ----------------------------
  # Here `x` is a data.table; we fill across the columns listed in `yr`.
  dt <- x
  
  # Validate inputs for data.table mode.
  stopifnot(!is.null(yr))
  stopifnot(all(yr %in% names(dt)))
  stopifnot(is.numeric(progress_every), length(progress_every) == 1L, progress_every >= 1L)
  
  # Work on a copy so the original object is not modified by reference.
  out <- data.table::copy(dt)
  
  # Extract "before" panel (rows = entities; cols = years).
  M0 <- as.matrix(out[, ..yr])
  
  # Pre-allocate "after" panel matrix.
  M1 <- M0
  
  # Create a progressor (only used if show_progress = TRUE).
  # Note: you must wrap the call in progressr::with_progress() and set a handler.
  if (isTRUE(show_progress)) {
    p <- progressr::progressor(steps = nrow(M0))
  }
  
  # Row-wise fill (lets us update progress; apply() is awkward for progress).
  for (i in seq_len(nrow(M0))) {
    
    # Fill one row by calling vector-mode (Mode 1).
    M1[i, ] <- fill_gaps_leq_k(M0[i, ], k = k)
    
    # Update progress every `progress_every` rows (and at the final row).
    if (isTRUE(show_progress) && (i %% progress_every == 0L || i == nrow(M0))) {
      
      # Advance by the chunk size; handle the last partial chunk correctly.
      p(amount = min(progress_every, nrow(M0) - i + progress_every))
    }
  }
  
  # Write filled values back into the year columns.
  out[, (yr) := as.data.table(M1)]
  
  # Compute which cells were filled (exactly those that changed 0 -> 1).
  # We only compute this if we need any tracking output.
  if (isTRUE(add_filled_col) || isTRUE(add_gap_stats)) {
    filled_cells <- (M0 == 0L) & (M1 == 1L)
  }
  
  # Boolean row flag: did any cell change from 0 to 1?
  if (isTRUE(add_filled_col)) {
    out[, (filled_col) := rowSums(filled_cells) > 0L]
  }
  
  # Gap stats per row:
  # - n_gaps_filled: number of distinct filled gaps (TRUE-runs in filled_cells row)
  # - avg_gap_len_filled: mean length of those runs (NA if none)
  if (isTRUE(add_gap_stats)) {
    
    # Initialize outputs.
    n_gaps <- integer(nrow(M0))
    avg_len <- rep(NA_real_, nrow(M0))
    
    for (i in seq_len(nrow(M0))) {
      f <- filled_cells[i, ]
      
      # If nothing was filled in this row, leave defaults (0, NA).
      if (!any(f)) next
      
      # TRUE-runs correspond to filled gaps; their lengths are the gap lengths.
      rr <- rle(f)
      lens <- rr$lengths[rr$values]
      
      n_gaps[i] <- length(lens)
      avg_len[i] <- round(mean(lens), digits = 2)
    }
    
    # Add columns to output table.
    out[, (n_gaps_col) := n_gaps]
    out[, (avg_gap_len_col) := avg_len]
  }
  
  # Return the filled copy.
  out
}




fix_bad_abi_flagfill <- function(dt,
                                 dt_filled,
                                 bad_abi,
                                 yr,
                                 abi_col = "abi",
                                 k = 3L,
                                 flag = 50L) {
  #' Flag-fill short zero-gaps for selected ABIs and prune conflicts For each ABI 
  #' in `bad_abi`, this function:
  #' 1. Takes the corresponding rows from `dt` (not `dt_filled`) and performs a
  #'    row-wise gap-fill where short runs of zeros (length $$\le k$$) that are
  #'    bounded by ones (pattern $$1 \; 0\ldots0 \; 1$$) are replaced by `flag`
  #'    (a high sentinel value).
  #' 2. Prunes (removes) flagged fills in any ABI-year where the column sum is
  #'    greater than `flag` (i.e., there is at least one additional non-zero in
  #'    that year besides the single flagged fill). Only entries equal to `flag`
  #'    are pruned; existing 0/1 values are left unchanged.
  #' 3. Converts remaining `flag` values to 1.
  #'
  #' The result is written into a copy of `dt_filled` for the ABIs in `bad_abi`.
  #'
  #' @param dt A `data.table` containing the original (unfilled) 0/1 panel. Must
  #'   include `abi_col` and the year columns in `yr`.
  #' @param dt_filled A `data.table` to start from (often a generally-filled
  #'   version of `dt`). The function returns a modified copy of this object for
  #'   rows matching `bad_abi`.
  #' @param bad_abi Vector of ABI identifiers to apply the special flagged-fill
  #'   logic to.
  #' @param yr Character vector of column names representing years (e.g.,
  #'   `"2000"`, `"2001"`, ...). These columns must exist in both `dt` and
  #'   `dt_filled`.
  #' @param abi_col Name of the ABI identifier column in both tables.
  #' @param k Integer. Maximum zero-run length to flag-fill when bounded by ones.
  #' @param flag Integer sentinel used to mark newly-filled values before pruning.
  #'   Choose a value much larger than 1 (e.g., 50) so column sums can identify
  #'   conflicts.
  #'
  #' @details
  #' Conflict rule (per ABI-year): let $$S_t = \sum_i M_{i,t}$$ after flagged fill.
  #' If $$S_t > \text{flag}$$, then all entries equal to `flag` in year $$t$$ are
  #' set back to 0. This removes:
  #' - $$1 + \text{flag}$$ conflicts (existing activity plus a flagged fill)
  #' - $$\text{flag} + \text{flag}$$ conflicts (two flagged fills in same year)
  #'
  #' Remaining `flag` entries are then converted to 1.
  #'
  #' @return A `data.table` (copy of `dt_filled`) with updated year columns for
  #'   ABIs in `bad_abi`.
  
  # ---- Input checks ----
  stopifnot(is.data.table(dt), is.data.table(dt_filled))
  stopifnot(all(c(abi_col, yr) %in% names(dt)))
  stopifnot(all(c(abi_col, yr) %in% names(dt_filled)))
  stopifnot(length(bad_abi) > 0L)
  
  # ---- Helper: flag-fill short bounded gaps within a single binary vector ----
  # Replaces qualifying zero-runs (length <= k, bounded by 1s) with `flag`.
  fill_gaps_leq_k_flag <- function(v, k = 3L, flag = 50L) {
    v <- as.integer(v)
    
    # Run-length encode and compute start/end indices of each run
    r <- rle(v); e <- cumsum(r$lengths); s <- e - r$lengths + 1L
    
    # Candidate runs: zeros with length <= k
    idx0 <- which(r$values == 0L & r$lengths <= k)
    
    # Flag-fill only if the zero-run is bounded by 1s (pattern 1-0...0-1)
    for (j in idx0) {
      if (j > 1L && j < length(r$values) &&
          r$values[j - 1L] == 1L && r$values[j + 1L] == 1L) {
        v[s[j]:e[j]] <- flag
      }
    }
    v
  }
  
  # Work on a copy so we don't modify dt_filled by reference
  out <- copy(dt_filled)
  
  # Pre-split dt to avoid repeatedly subsetting inside the loop
  dt_bad_list <- split(dt[get(abi_col) %in% bad_abi], by = abi_col, keep.by = FALSE)
  
  # progressr progressor (visible only if wrapped in with_progress() + handler)
  p <- progressr::progressor(along = bad_abi)
  
  # ---- Process each ABI ----
  for (a in bad_abi) {
    p(sprintf("abi=%s", a))
    
    d0 <- dt_bad_list[[as.character(a)]]
    if (is.null(d0) || nrow(d0) == 0L) next
    
    # Extract the ABI's year panel as a numeric matrix
    M0 <- as.matrix(d0[, ..yr])
    
    # 1) Row-wise flagged fill (creates `flag` entries where gaps are filled)
    Mflag <- t(apply(M0, 1L, fill_gaps_leq_k_flag, k = k, flag = flag))
    
    # 2) Prune flagged fills in "conflict years":
    #    If a column sum exceeds `flag`, there must be additional non-zero mass
    #    beyond a single flagged fill, so we drop ALL flagged entries in that year.
    cs <- colSums(Mflag)
    conf <- cs > flag
    
    if (any(conf)) {
      jj <- which(conf)
      block <- (Mflag[, jj, drop = FALSE] == flag)
      Mflag[, jj] <- ifelse(block, 0L, Mflag[, jj, drop = FALSE])
    }
    
    # 3) Convert surviving flags into real 1s
    Mflag[Mflag == flag] <- 1L
    
    # Write back only this ABI's rows and year columns
    out[get(abi_col) == a, (yr) := as.data.table(Mflag)]
  }
  
  out
}






sic_desc_split_summary_dt <- function(church_2026_form_analysis_dt,
                                      target_classification = "Jewish Synagogue",
                                      sic_classifications,
                                      primary_long,
                                      overflow_long,
                                      progress = TRUE) {
  #' Summarize SIC6 description distributions for ABIs tied to a target 
  #' classification (data.table + progress). Efficient `data.table` 
  #' implementation of your SIC6-description summary workflow with an optional
  #' console progress bar.
  #'
  #' For a given `target_classification` (e.g., "Jewish Synagogue"), the function:
  #' \enumerate{
  #'   \item Finds all SIC codes in `sic_classifications` that map to `target_classification`.
  #'   \item Finds ABIs in `primary_long` and `overflow_long` whose `code` is in those SIC codes.
  #'   \item Within `church_2026_form_analysis_dt`, computes per-ABI SIC6-description counts and
  #'   within-ABI percentages.
  #'   \item Returns overall description distributions for:
  #'   \itemize{
  #'     \item `pct100`: ABIs with exactly one SIC6 description (one code per ABI).
  #'     \item `pctlt100`: ABIs with multiple SIC6 descriptions (multiple codes per ABI).
  #'   }
  #' }
  #'
  #' @param church_2026_form_analysis_dt A data frame/data.table containing at least columns
  #'   `abi` and `sic6_descriptions`.
  #' @param target_classification Character scalar (or vector) giving the classification label(s)
  #'   to match in `sic_classifications$classification`. Default is `"Jewish Synagogue"`.
  #' @param sic_classifications A data frame/data.table containing at least `sic_code` and `classification`.
  #' @param primary_long A data frame/data.table containing at least `abi` and `code`.
  #' @param overflow_long A data frame/data.table containing at least `abi` and `code`.
  #' @param progress Logical; if `TRUE` (default) show a base R console progress bar via
  #'   [utils::txtProgressBar()].
  #'
  #' @details
  #' The per-ABI table `by_abi` has one row per $$\text{ABI} \times \text{sic6\_descriptions}$$ with:
  #' \describe{
  #'   \item{n}{Count of records for that ABI-description pair.}
  #'   \item{pct}{Within-ABI percent share: $$pct = 100 \times n / \sum n$$ (rounded to 2 decimals).}
  #' }
  #'
  #' The labels `pct100` and `pctlt100` reflect **ABI-level** structure:
  #' \itemize{
  #'   \item `pct100`: ABIs with exactly one distinct `sic6_descriptions` value.
  #'   \item `pctlt100`: ABIs with more than one distinct `sic6_descriptions` value.
  #' }
  #'
  #' @return A named list with elements:
  #' \describe{
  #'   \item{target_classification}{The input `target_classification`.}
  #'   \item{search_code}{Character/integer vector of unique SIC codes matched to the classification.}
  #'   \item{primary}{List with `by_abi`, `pct100`, `pctlt100` for ABIs derived from `primary_long`.}
  #'   \item{overflow}{List with `by_abi`, `pct100`, `pctlt100` for ABIs derived from `overflow_long`.}
  #' }
  #'
  #' Each of `pct100`/`pctlt100` is a `data.table` with columns:
  #' \describe{
  #'   \item{x}{A `sic6_descriptions` value.}
  #'   \item{Freq}{Proportion across the selected set (rounded to 3 decimals).}
  #' }
  
  requireNamespace("data.table")
  requireNamespace("utils")
  
  # -- Progress bar helpers ------------------------------------------------------
  pb_init <- function(n) if (isTRUE(progress)) utils::txtProgressBar(min = 0, max = n, style = 3) else NULL
  pb_step <- function(pb, i) if (!is.null(pb)) utils::setTxtProgressBar(pb, i)
  pb_close <- function(pb) if (!is.null(pb)) close(pb)
  
  # Steps: coerce 4 inputs + search_code + compute summaries
  pb <- pb_init(6)
  on.exit(pb_close(pb), add = TRUE)
  i <- 0L
  
  # -- Coerce to data.table (no copy if already data.table) ----------------------
  dt_church <- data.table::as.data.table(church_2026_form_analysis_dt); i <- i + 1L; pb_step(pb, i)
  dt_sic    <- data.table::as.data.table(sic_classifications);          i <- i + 1L; pb_step(pb, i)
  dt_pri    <- data.table::as.data.table(primary_long);                 i <- i + 1L; pb_step(pb, i)
  dt_ovr    <- data.table::as.data.table(overflow_long);                i <- i + 1L; pb_step(pb, i)
  
  # -- 1) SIC codes for the target classification --------------------------------
  search_code <- unique(dt_sic[classification %chin% target_classification, sic_code])
  i <- i + 1L; pb_step(pb, i)
  
  # Helper: frequency/proportion table for a character vector
  dist_table <- function(x) {
    if (length(x) == 0L) return(data.table::data.table(x = character(), Freq = numeric()))
    out <- data.table::as.data.table(prop.table(table(x)))
    data.table::setnames(out, c("x", "Freq"))
    out[, Freq := round(Freq, 3)]
    data.table::setorder(out, -Freq)
    out
  }
  
  # Helper: compute summaries for a given set of ABIs
  summarize_for_abis <- function(abis) {
    if (length(abis) == 0L) {
      return(list(
        by_abi = data.table::data.table(
          abi = character(), sic6_descriptions = character(), n = integer(), pct = numeric()
        ),
        pct100 = data.table::data.table(x = character(), Freq = numeric()),
        pctlt100 = data.table::data.table(x = character(), Freq = numeric())
      ))
    }
    
    # Filter to relevant ABIs and keep only needed columns
    dat <- dt_church[abi %chin% abis & !is.na(sic6_descriptions),
                     .(abi, sic6_descriptions)]
    
    # Per-ABI counts and within-ABI percentages
    by_abi <- dat[, .(n = .N), by = .(abi, sic6_descriptions)]
    by_abi[, pct := round(n / sum(n) * 100, 2), by = abi]
    data.table::setorder(by_abi, abi, -pct, -n)
    
    # ABI-level classification: one vs multiple descriptions
    abi_n_desc <- by_abi[, .(n_desc = .N), by = abi]
    abis_one   <- abi_n_desc[n_desc == 1L, abi]
    abis_multi <- abi_n_desc[n_desc >  1L, abi]
    
    # Overall distributions of sic6_descriptions for each ABI class
    pct100   <- dist_table(by_abi[abi %chin% abis_one,   sic6_descriptions])
    pctlt100 <- dist_table(by_abi[abi %chin% abis_multi, sic6_descriptions])
    
    list(by_abi = by_abi, pct100 = pct100, pctlt100 = pctlt100)
  }
  
  # -- 2) ABIs from primary/overflow ---------------------------------------------
  primary_abi  <- unique(dt_pri[code %chin% search_code, abi])
  overflow_abi <- unique(dt_ovr[code %chin% search_code, abi])
  
  # -- 3) Summaries ---------------------------------------------------------------
  primary_res  <- summarize_for_abis(primary_abi)
  overflow_res <- summarize_for_abis(overflow_abi)
  i <- i + 1L; pb_step(pb, i)
  
  list(
    target_classification = target_classification,
    search_code = search_code,
    primary  = primary_res,
    overflow = overflow_res
  )
}


