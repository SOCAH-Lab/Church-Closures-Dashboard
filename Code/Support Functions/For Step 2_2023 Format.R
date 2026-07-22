## ----------------------------------------------------------------
## Define functions used in the Step 2 script for the 2023 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: April 9th, 2026
## 
## Description: In addition to the general-purpose functions defined in another
##              script, the following functions are used to complete Step 2 of
##              the data cleaning and validation process, as identified through
##              exploratory data analysis.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions:
##    1. generate_usps_token: Requests an OAuth access token from USPS using the 
##       client credentials grant. Intended for use by `validate_usps_address()`.
##       
##    2. validate_usps_address: Calls the USPS Addresses API (v3) to 
##       validate/standardize an address and returns a one-row tibble of the 
##       preferred USPS-formatted address. On failure or if no address is found, 
##       returns an empty tibble (0 rows).
##       
##       Source: https://developers.usps.com/addressesv3
##       Example: https://github.com/USPS/api-examples
## 
##    3. build_zip_city_lookup: Takes the Simplemaps `uscities` dataset (e.g., 
##       `simplemaps_uscities_basicv1.90`) and creates a lookup table with 
##       **one row per 5-digit ZIP code**, mapping each ZIP to a single city/state.
## 
##    4. get_city_info: Looks up city name(s) for one or more ZIP codes in 
##       `zip_city_lookup`, converts them to uppercase, de-duplicates, and 
##       returns a single comma-separated string. If no matches are found, 
##       returns "No Matches Found: " followed by the ZIPs provided to `zip` 
##       (normalized to 5 digits where possible).
## 
##    5. make_zip5_candidates: USPS/lookup data sometimes disagrees when a ZIP 
##       has leading/trailing zeros. This helper:
##          1) normalizes input to a 5-digit ZIP (keeps leading zeros),
##          2) counts edge zeros (leading + trailing),
##          3) strips ONLY those edge zeros to get the core,
##          4) rebuilds a sequence of candidate ZIPs by moving zeros one-by-one
##             from the front to the back.
## 
##    6. qc_validation_results: Validate row indices, coverage, and address 
##       verification across a list of data frames. `qc_validation_results()` 
##       runs a set of consistency checks on a *named* list of data frames 
##       (typically split into numeric ranges encoded in the list names).
## 
##       Checks performed:
##          1) Dimensions: returns a tibble of (file_range, n_row, n_col) for 
##             each list item.
##          2) Row coverage (count): total number of rows across all list items.
##          3) Row coverage (divisibility test): whether the total rows are an 
##             exact multiple of `denom` (useful when each chunk should be 
##             exactly `denom` rows).
##          4) Per-row range validity: for each row, verifies `rowname` lies 
##             within the expected start/end parsed from the list element name.
##          5) Per-file range validity: for each file, checks whether the 
##             min/max `rowname` exactly match the expected start/end.
##          6) Address verification distribution: tabulates `address_verified` 
##             counts (TRUE/FALSE/NA) per file_range for quick diagnostics.
## 
## 
##    7. get_row_id_raw: Returns a vector of row identifiers for a data 
##       frame-like object. If the input contains a `rowname` column, that 
##       column is used. Otherwise, the function falls back to the object's row 
##       names (`rownames(x)`).
## 
##    8. canonicalize_row_id: Converts an input vector of identifiers to a 
##       consistent character form so that equivalent values compare equal even 
##       when represented differently as text.
## 
##    9. combine_mixed_dfs: Combines a named list of data frames where some 
##       elements are already in a "full" format (default `n_full` columns) and 
##       others are in a reduced format (default `n_needs_meta` columns) that 
##       must be enriched using a metadata table. The function orders inputs 
##       using `dims_tbl$file_range`, enriches only the reduced items by joining 
##       to `step_1`, then row-binds everything into a single data frame.

# Helper: return left-hand side unless it's NULL, otherwise return fallback
`%||%` <- function(x, y) if (is.null(x)) y else x


## ----------------------------------------------------------------
## FUNCTIONS

generate_usps_token <- function(consumer_key, consumer_secret) {
  #' @description
  #' Requests an OAuth access token from USPS using the client credentials grant.
  #' Intended for use by `validate_usps_address()`.
  #'
  #' @param consumer_key Character. USPS API Consumer Key (client_id).
  #' @param consumer_secret Character. USPS API Consumer Secret (client_secret).
  #'
  #' @return Character scalar. The OAuth access token.
  #'
  #' @examples
  #' \dontrun{
  #' token <- generate_usps_token("<key>", "<secret>")
  #' }
  
  # USPS OAuth token endpoint
  oauth_url <- "https://apis.usps.com/oauth2/v3/token"
  
  # Request payload (client credentials grant)
  body <- list(
    client_id     = consumer_key,
    client_secret = consumer_secret,
    grant_type    = "client_credentials"
  )
  
  # Request token
  resp <- POST(
    url = oauth_url,
    add_headers(`Content-Type` = "application/json", accept = "application/json"),
    body = toJSON(body, auto_unbox = TRUE),
    encode = "raw"
  )
  
  # Fail fast on non-success
  if (status_code(resp) != 200) {
    stop(
      "Failed to obtain OAuth token. Status: ", status_code(resp),
      " Body: ", content(resp, "text", encoding = "UTF-8")
    )
  }
  
  # Parse and extract token
  parsed <- fromJSON(content(resp, "text", encoding = "UTF-8"), simplifyVector = TRUE)
  token <- parsed$access_token
  
  if (is.null(token) || !nzchar(token)) stop("OAuth response did not contain a non-empty access_token.")
  
  token
}




validate_usps_address <- function(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "") {
  #' @description
  #' Calls the USPS Addresses API (v3) to validate/standardize an address and returns
  #' a one-row tibble of the preferred USPS-formatted address. On failure or if no
  #' address is found, returns an empty tibble (0 rows).
  #'
  #' @details
  #' This function obtains an OAuth token via `generate_usps_token()` (client credentials),
  #' then performs a GET request to the USPS Addresses v3 endpoint.
  #'
  #' USPS docs: https://developers.usps.com/addressesv3
  #'
  #' @param consumer_key Character. USPS API Consumer Key (client_id).
  #' @param consumer_secret Character. USPS API Consumer Secret (client_secret).
  #' @param address1 Character. Street address line 1.
  #' @param address2 Character. Secondary address (apt/suite/unit), default "".
  #' @param city Character. City.
  #' @param state Character. State abbreviation (e.g., "CT").
  #' @param zip5 Character. 5-digit ZIP code.
  #' @param zip4 Character. Optional 4-digit ZIP+4 extension, default "".
  #'
  #' @return
  #' A tibble with columns:
  #' `address_line_1`, `address_line_2`, `city`, `state`, `zipcode`, `zipcode_ext`.
  #' Returns an empty tibble on request failure or if USPS returns no `address` object.
  
  # 1) Get OAuth token (uses your existing generate_usps_token())
  token <- generate_usps_token(consumer_key, consumer_secret)
  
  # 2) USPS Addresses v3 endpoint
  base_url <- "https://apis.usps.com/addresses/v3/address"
  
  # 3) Build query parameters (ZIP5 required by the API call)
  params <- list(
    streetAddress    = address1,
    secondaryAddress = address2,
    city             = city,
    state            = state,
    ZIPCode          = zip5
  )
  
  # 4) Add ZIP+4 if provided; enforce exactly 4 digits
  if (nzchar(zip4)) {
    if (!grepl("^[0-9]{4}$", zip4)) stop("Invalid ZIPPlus4 format. Must be exactly 4 digits.")
    params$ZIPPlus4 <- zip4
  }
  
  # 5) Build the full request URL (query string included)
  request_url <- modify_url(base_url, query = params)
  
  # 6) Call the API with Bearer token auth
  resp <- GET(
    url = request_url,
    add_headers(
      accept = "application/json",
      Authorization = paste("Bearer", token)
    )
  )
  
  # 7) If request failed, warn and return an empty result (instead of NULL)
  if (status_code(resp) != 200) {
    warning(
      "USPS API request failed. Status: ", status_code(resp),
      " Body: ", content(resp, "text", encoding = "UTF-8")
    )
    return(tibble())
  }
  
  # 8) Parse JSON response
  parsed <- fromJSON(content(resp, "text", encoding = "UTF-8"),
                               simplifyVector = TRUE)
  
  # 9) Extract the address payload; if none, warn and return empty result
  addr <- parsed$address
  if (is.null(addr)) {
    warning("No valid addresses were found by the USPS API.")
    return(tibble::tibble())
  }
  
  # 10) Return a standardized one-row tibble with consistent column names
  tibble(
    address_line_1 = addr$streetAddress %||% "",
    address_line_2 = addr$secondaryAddress %||% "",
    city           = addr$city %||% "",
    state          = addr$state %||% "",
    zipcode        = addr$ZIPCode %||% "",
    zipcode_ext    = addr$ZIPPlus4 %||% ""
  )
}




build_zip_city_lookup <- function(uscities_df) {
  #' @description
  #' Takes the Simplemaps `uscities` dataset (e.g., `simplemaps_uscities_basicv1.90`)
  #' and creates a lookup table with **one row per 5-digit ZIP code**, mapping each
  #' ZIP to a single city/state.
  #'
  #' @param uscities_df A data frame containing (at minimum) the columns:
  #'   `city`, `state_id`, and `zips`. The `zips` column is expected to be a
  #'   whitespace-separated list of 5-digit ZIP codes (as in the Simplemaps file).
  #'
  #' @return A tibble/data.frame with columns:
  #'   \describe{
  #'     \item{zip}{5-digit ZIP code as a character string (zero-padded).}
  #'     \item{city}{City name associated with the ZIP in the Simplemaps file.}
  #'     \item{state_id}{Two-letter state abbreviation.}
  #'   }
  #'   If a ZIP appears for multiple cities in the source, the function keeps the
  #'   **first** encountered mapping due to `distinct(zip, .keep_all = TRUE)`.
  #'
  #' @source 
  #' https://simplemaps.com/data/us-cities
  #'
  #' @details
  #' The Simplemaps `zips` field can contain many ZIPs per city. This function
  #' "unnests" that field into one ZIP per row via `tidyr::separate_rows()`, then
  #' standardizes ZIP formatting and removes duplicates.
  
  uscities_df %>%
    # Keep only what we need for the lookup
    dplyr::select(city, state_id, zips) %>%
    
    # Expand the whitespace-separated `zips` list:
    # one output row per ZIP code per city.
    tidyr::separate_rows(zips, sep = "\\s+") %>%
    
    # Standardize/validate ZIP formatting:
    # - extract 5 digits (defensive)
    # - pad with leading zeros
    dplyr::mutate(
      zip = stringr::str_pad(
        stringr::str_extract(zips, "\\d{5}"),
        width = 5,
        pad = "0"
      )
    ) %>%
    
    # Drop rows where we couldn't parse a 5-digit ZIP
    dplyr::filter(!is.na(zip)) %>%
    
    # Ensure one row per ZIP in the final lookup.
    # If a ZIP appears multiple times, keep the first occurrence.
    dplyr::distinct(zip, .keep_all = TRUE) %>%
    
    # Output only the fields typically needed for matching
    dplyr::select(zip, city, state_id)
}




get_city_info <- function(zip, zip_city_lookup) {
  #' @description
  #' Looks up city name(s) for one or more ZIP codes in `zip_city_lookup`, converts
  #' them to uppercase, de-duplicates, and returns a single comma-separated string.
  #' If no matches are found, returns "No Matches Found: " followed by the ZIPs
  #' provided to `zip` (normalized to 5 digits where possible).
  #'
  #' @param zip A ZIP code or vector of ZIP codes (character or numeric). The first
  #'   5 digits are used; non-digits are ignored.
  #' @param zip_city_lookup A data frame with at least columns `zip` and `city`.
  #'
  #' @return A length-1 character string like `"NEW HAVEN"` or `"NEW HAVEN, BOULDER"`;
  #'   if none match, returns `"No Matches Found: 06519, 80324"`.
  
  # Coerce input ZIP(s) to character so we can safely run regex on them
  z <- as.character(zip)
  
  # Normalize each input to a 5-digit ZIP:
  # - extract the first 5 consecutive digits anywhere in the string
  # - left-pad with zeros to ensure width 5
  z5 <- stringr::str_pad(stringr::str_extract(z, "\\d{5}"), width = 5, pad = "0")
  
  # Lookup: for each normalized ZIP, find the matching city in the lookup table
  # (match() returns NA when the ZIP isn't found)
  cities <- zip_city_lookup$city[match(z5, zip_city_lookup$zip)]
  
  # Treat any missing ZIP normalization or missing lookup result as NA
  cities[is.na(z5) | is.na(cities)] <- NA_character_
  
  # Standardize output formatting:
  # - uppercase
  # - drop NAs
  # - de-duplicate while preserving first-seen order
  cities_out <- unique(stats::na.omit(stringr::str_to_upper(cities)))
  
  # If there were no matched cities at all, return a message listing the ZIPs tried
  if (length(cities_out) == 0) {
    # Keep only valid normalized ZIPs (drop NAs), and de-duplicate
    z_list <- unique(stats::na.omit(z5))
    
    # If we couldn't even extract any 5-digit ZIPs, return a simpler message
    if (length(z_list) == 0) return("No Matches Found")
    
    # Otherwise, list the ZIPs we attempted
    return(paste0("No Matches Found: ", paste(z_list, collapse = ", ")))
  }
  
  # Otherwise, return the matched city/cities as a single comma-separated string
  paste(cities_out, collapse = ", ")
}




make_zip5_candidates <- function(zip5_raw) {
  #' @description
  #' USPS/lookup data sometimes disagrees when a ZIP has leading/trailing zeros.
  #' This helper:
  #' 1) normalizes input to a 5-digit ZIP (keeps leading zeros),
  #' 2) counts edge zeros (leading + trailing),
  #' 3) strips ONLY those edge zeros to get the core,
  #' 4) rebuilds a sequence of candidate ZIPs by moving zeros one-by-one
  #'    from the front to the back.
  #'
  #' Example: "01200" (core = "12", edge zeros = 3) =>
  #'   00012 -> 00120 -> 01200 -> 12000
  #' Returned with the original ZIP first.
  #'
  #' @param zip5_raw Character. A ZIP-like value (may include non-digits).
  #'
  #' @return Character vector of unique ZIP5 candidates (each 5 digits).
  #'
  #' @examples
  #' make_zip5_candidates("01234")  # "01234" "12340"
  #' make_zip5_candidates("01230")  # "01230" "00123" "12300"
  #' make_zip5_candidates("01200")  # "01200" "00012" "00120" "12000"
  
  # Digits only; force exactly 5 chars (preserves leading zeros)
  zip5_raw <- ifelse(is.na(zip5_raw) || zip5_raw == "", "", zip5_raw)
  zip5_raw <- stringr::str_replace_all(zip5_raw, "\\D", "")
  if (!nzchar(zip5_raw)) return(character(0))
  zip5_raw <- stringr::str_pad(zip5_raw, 5, side = "left", pad = "0")
  
  # Count edge zeros
  lead0  <- nchar(sub("^((0)*).*", "\\1", zip5_raw))
  trail0 <- nchar(sub(".*?((0)*)$", "\\1", zip5_raw))
  
  # Core digits after stripping only edge zeros (keep internal zeros, if any)
  core <- substring(zip5_raw, lead0 + 1, 5 - trail0)
  if (!nzchar(core)) return(zip5_raw)
  
  # Total number of movable edge-zeros
  n0 <- lead0 + trail0
  
  # Iterate from "all zeros leading" -> ... -> "no zeros leading (all trailing)"
  # Example: 01200 (n0=3, core=12) =>
  # lead = 3,2,1,0  => 00012, 00120, 01200, 12000
  candidates <- vapply(seq.int(n0, 0, by = -1), function(k_lead) {
    paste0(strrep("0", k_lead), core, strrep("0", n0 - k_lead))
  }, character(1))
  
  # Return with the original ZIP first (then the rest in iteration order)
  unique(c(zip5_raw, candidates))
}




qc_validation_results <- function(dfs, denom = 42000) {
  #' @description
  #' Validate row indices, coverage, and address verification across a list of 
  #' data frames. `qc_validation_results()` runs a set of consistency checks on a 
  #' *named* list of data frames (typically split into numeric ranges encoded in 
  #' the list names).
  #'
  #' Checks performed:
  #' 1) **Dimensions**: returns a tibble of (file_range, n_row, n_col) for each list item.
  #' 2) **Row coverage (count)**: total number of rows across all list items.
  #' 3) **Row coverage (divisibility test)**: whether the total rows are an exact
  #'    multiple of `denom` (useful when each chunk should be exactly `denom` rows).
  #' 4) **Per-row range validity**: for each row, verifies `rowname` lies within the
  #'    expected start/end parsed from the list element name.
  #' 5) **Per-file range validity**: for each file, checks whether the min/max `rowname`
  #'    exactly match the expected start/end.
  #' 6) **Address verification distribution**: tabulates `address_verified` counts
  #'    (TRUE/FALSE/NA) per file_range for quick diagnostics.
  #'
  #' @param dfs Named list of data frames/tibbles. Each element name must contain
  #'   a starting and ending integer (the first and last number found in the string),
  #'   e.g., "84001 to 126000". Each data frame must include a `rowname` column.
  #' @param denom Integer used for the row-coverage divisibility check (default 42000).
  
  # ---- Input checks ---------------------------------------------------------
  stopifnot(is.list(dfs), length(dfs) > 0)
  
  # ---- 1) Dimensions per list element --------------------------------------
  # "Confirm the range recorded is within the index" (sanity check):
  # report nrow/ncol for each file_range without printing a nested list.
  dims <- purrr::imap_dfr(dfs, ~{
    d <- dim(.x)
    tibble::tibble(
      file_range = .y,
      n_row      = d[1],
      n_col      = d[2]
    )
  })
  
  # ---- 2) Parse expected start/end from names(dfs) --------------------------
  # Each list name encodes the expected rowname range.
  # We extract:
  # - expected_start: first integer in the name
  # - expected_end:   last integer in the name
  expected <- stringr::str_squish(names(dfs)) %>%
    tibble::enframe(name = NULL, value = "expected_range") %>%
    dplyr::mutate(
      expected_start = as.integer(stringr::str_extract(expected_range, "^\\d+")),
      expected_end   = as.integer(stringr::str_extract(expected_range, "\\d+$"))
    ) %>%
    dplyr::select(expected_range, expected_start, expected_end)
  
  # ---- 3) Row coverage across all list elements ----------------------------
  # Total number of rows represented across all chunks.
  row_representation <- sum(vapply(dfs, nrow, integer(1)))
  
  # Divisibility check: TRUE implies total rows form whole chunks of size `denom`.
  # (This is not a "ratio" numerically; it is a pass/fail check.)
  row_representation_ratio <- (row_representation %% denom) == 0
  
  # ---- 4) Per-row check: rowname within expected range ---------------------
  # For each row, confirm its `rowname` lies between the expected start/end for that file.
  per_row <- purrr::imap_dfr(dfs, function(dat, nm) {
    exp <- dplyr::filter(expected, expected_range == nm)
    
    dat %>%
      dplyr::transmute(
        file_range = nm,
        rowname,
        within_expected = dplyr::between(
          as.integer(rowname),
          exp$expected_start[1],
          exp$expected_end[1]
        )
      )
  })
  
  # Aggregate view of the per-row check.
  within_expected_table <- table(per_row$within_expected, useNA = "ifany")
  
  # ---- 5) Per-file check: min/max rowname match expected start/end ----------
  # Stronger check: verifies the observed min/max rowname equals the expected boundaries.
  per_file <- purrr::imap_dfr(dfs, function(dat, nm) {
    exp <- dplyr::filter(expected, expected_range == nm)
    
    dat_num <- dat %>% dplyr::transmute(rowname_num = as.integer(rowname))
    
    actual_min <- min(dat_num$rowname_num, na.rm = TRUE)
    actual_max <- max(dat_num$rowname_num, na.rm = TRUE)
    
    tibble::tibble(
      file_range  = nm,
      min_matches = actual_min == exp$expected_start[1],
      max_matches = actual_max == exp$expected_end[1]
    )
  })
  
  # ---- 6) Address verification summary (TRUE/FALSE/NA) ----------------------
  # Diagnostic: if a chunk has ~0 TRUE (or unexpected NA patterns), it may indicate
  # an upstream failure and could warrant re-running that chunk.
  address_ver_tab <- lapply(dfs, \(x) table(x$address_verified, useNA = "ifany"))
  
  address_ver_compiled <- do.call(
    rbind,
    lapply(names(address_ver_tab), function(nm) {
      tt <- address_ver_tab[[nm]]
      nms <- names(tt)
      iNA <- which(is.na(nms))
      
      data.frame(
        Range   = nm,
        `FALSE` = as.integer(if ("FALSE" %in% nms) tt[["FALSE"]] else 0),
        `TRUE`  = as.integer(if ("TRUE"  %in% nms) tt[["TRUE"]]  else 0),
        `NA`    = as.integer(if (length(iNA)) tt[iNA] else 0),
        row.names = NULL,
        check.names = FALSE
      )
    })
  )
  
  # ---- Return ---------------------------------------------------------------
  list(
    dims = dims,
    row_representation = row_representation,
    row_representation_ratio = row_representation_ratio,
    per_row = per_row,
    within_expected_table = within_expected_table,
    per_file = per_file,
    address_verifications = address_ver_compiled
  )
}




get_row_id_raw <- function(x) {
  #' @description
  #' Returns a vector of row identifiers for a data frame-like object. If the input
  #' contains a `rowname` column, that column is used. Otherwise, the function falls
  #' back to the object's row names (`rownames(x)`).
  #'
  #' @details
  #' This helper is useful when working with inputs that are inconsistent about
  #' where the record identifier is stored (as an explicit column vs. as row names).
  #' If neither a `rowname` column nor row names are available, the function returns
  #' a character vector of `NA` values with length `nrow(x)`.
  #'
  #' @param x A data frame (or similar) from which to extract row identifiers.
  #'
  #' @return A character vector of length `nrow(x)` containing row identifiers.
  
  # Prefer an explicit identifier column if present
  if ("rowname" %in% names(x)) {
    # Coerce to character to standardize type for downstream use
    as.character(x[["rowname"]])
  } else {
    # Otherwise fall back to the rownames attribute
    rn <- rownames(x)
    
    # If there are no rownames, return NAs of the correct length
    if (is.null(rn)) rep(NA_character_, nrow(x)) else as.character(rn)
  }
}




canonicalize_row_id <- function(x) {
  #' @description
  #' Converts an input vector of identifiers to a consistent character form so that
  #' equivalent values compare equal even when represented differently as text.
  #'
  #' @details
  #' The function:
  #' \itemize{
  #'   \item trims leading/trailing whitespace;
  #'   \item converts scientific-notation strings (e.g., `"7e+05"`) to a non-scientific
  #'         representation (e.g., `"700000"`);
  #'   \item converts integer-like decimal strings (e.g., `"700000.0"`) to plain integers
  #'         (e.g., `"700000"`);
  #'   \item leaves non-numeric identifiers unchanged (aside from trimming).
  #' }
  #'
  #' @param x A vector of identifiers (character, factor, numeric, etc.).
  #'
  #' @return A character vector of the same length as `x`, with identifiers in a
  #' canonicalized form.
  
  # Coerce to character and remove leading/trailing whitespace
  x <- trimws(as.character(x))
  
  # Identify values that look like scientific notation, e.g. "7e+05", "-1.2E3"
  sci <- grepl("^[+-]?\\d+(?:\\.\\d+)?[eE][+-]?\\d+$", x)
  
  # Convert scientific-notation strings to plain (non-scientific) text
  if (any(sci, na.rm = TRUE)) {
    # Parse only the scientific-notation subset to numeric
    y <- suppressWarnings(as.numeric(x[sci]))
    
    # Convert back to character without scientific notation; keep plenty of digits
    x[sci] <- format(y, scientific = FALSE, trim = TRUE, digits = 22)
  }
  
  # Identify values that are integers encoded with trailing ".0", e.g. "700000.0"
  dec_int <- grepl("^[+-]?\\d+\\.0+$", x)
  
  # Strip the redundant fractional part to get a plain integer string
  if (any(dec_int, na.rm = TRUE)) {
    x[dec_int] <- sub("\\.0+$", "", x[dec_int])
  }
  
  # Final trim for safety (e.g., if format() introduced any padding)
  trimws(x)
}




combine_mixed_dfs <- function(dfs, dims_tbl, n_full = 40, n_needs_meta = 9) {
  #' @description
  #' Combines a named list of data frames where some elements are already in a
  #' "full" format (default `n_full` columns) and others are in a reduced format
  #' (default `n_needs_meta` columns) that must be enriched using a metadata table.
  #' The function orders inputs using `dims_tbl$file_range`, enriches only the
  #' reduced items by joining to `step_1`, then row-binds everything into a single
  #' data frame.
  #'
  #' @details
  #' - Ordering is determined by parsing the numeric start of `dims_tbl$file_range`
  #'   (e.g., `"001-100"`) and sorting numerically.
  #' - Items with `n_full` columns are passed through unchanged.
  #' - Items with `n_needs_meta` columns are enriched by joining to `step_1` on a
  #'   stable row identifier.
  #' - The join key is normalized to a canonical character representation so that
  #'   equivalent identifiers match even if stored in different textual forms.
  #'
  #' @param dfs Named list of data frames to combine. Names must correspond to
  #'   `dims_tbl$file_range`.
  #' @param dims_tbl A tibble/data.frame with (at minimum) columns `file_range`
  #'   and `n_col`, describing each element's expected column count.
  #' @param n_full Integer. Column count that indicates a data frame is already in
  #'   full/complete format. Default is 40.
  #' @param n_needs_meta Integer. Column count that indicates a data frame needs
  #'   metadata enrichment. Default is 9.
  #'
  #' @return A single data frame/tibble produced by row-binding all processed
  #'   elements (after optional enrichment).
  #'
  #' @note
  #' This function assumes an object named `step_1` exists in the calling
  #' environment and contains the join identifier plus any metadata columns needed.
  
  stopifnot(is.list(dfs), length(dfs) > 0)
  
  # ---- Order inputs deterministically using dims_tbl ----
  # Parse the numeric start of file_range so we can sort (e.g., "001-100" -> 1),
  # then remove the helper column after ordering.
  dims_tbl2 <- dims_tbl %>%
    dplyr::mutate(range_start = as.integer(stringr::str_extract(file_range, "^\\d+"))) %>%
    dplyr::arrange(range_start) %>%
    dplyr::select(-range_start)
  
  # Reorder dfs to match dims_tbl2$file_range (expects dfs to be named)
  dfs_sorted <- dfs[dims_tbl2$file_range]
  
  # ---- Process each data frame according to its expected width ----
  processed <- purrr::imap(dfs_sorted, function(dat, nm) {
    # Look up expected column count for this file_range key
    n_col <- dims_tbl2$n_col[dims_tbl2$file_range == nm][1]
    if (is.na(n_col)) stop("No dims info for: ", nm)
    
    if (n_col == n_full) {
      # Already in full format: return unchanged
      return(dat)
    }
    
    if (n_col != n_needs_meta) {
      # Any other width indicates an unexpected input state
      stop(
        "Unexpected n_col=", n_col, " for: ", nm,
        " (expected ", n_full, " or ", n_needs_meta, ")"
      )
    }
    
    # ---- Reduced format: enrich using metadata ----
    # Build canonical join keys on BOTH sides to ensure consistent matching.
    dat_keyed <- dat %>%
      dplyr::mutate(
        .row_id_raw = get_row_id_raw(dat),
        .row_id     = canonicalize_row_id(.row_id_raw)
      )
    
    step1_keyed <- step_1 %>%
      dplyr::mutate(
        .row_id_raw = get_row_id_raw(step_1),
        .row_id     = canonicalize_row_id(.row_id_raw)
      ) %>%
      dplyr::select(-c(address_line_1:address_verified))
    
    # Join on canonical key; keep the original columns, then clean up helpers.
    out <- dplyr::inner_join(step1_keyed, dat_keyed, by = ".row_id") %>%
      dplyr::relocate(abi, .before = address_line_1) %>%
      dplyr::select(-.row_id_raw.x, -.row_id_raw.y)
    
    # Ensure a `rowname` column exists for downstream consistency.
    if (!("rowname" %in% names(out))) {
      out <- out %>% dplyr::mutate(rowname = .row_id, .before = 1)
    }
    
    out %>% dplyr::select(-.row_id)
  })
  
  # ---- Combine all processed chunks into one table ----
  dplyr::bind_rows(processed)
}






