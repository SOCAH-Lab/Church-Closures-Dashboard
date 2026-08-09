## ----------------------------------------------------------------
## Define functions used in the Step 2 script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 30th, 2026
## 
## Description: This script defines functions specific to Step 2 of the data
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
##    1. build_zip_city_lookup: Takes the Simplemaps `uscities` dataset (e.g., 
##       `simplemaps_uscities_basicv1.90`) and creates a lookup table with 
##       **one row per 5-digit ZIP code**, mapping each ZIP to a single city/state.
## 
##    2. get_city_info: Looks up city name(s) for one or more ZIP codes in 
##       `zip_city_lookup`, converts them to uppercase, de-duplicates, and 
##       returns a single comma-separated string. If no matches are found, 
##       returns "No Matches Found: " followed by the ZIPs provided to `zip` 
##       (normalized to 5 digits where possible).
## 
##    3. preprocess_address: This function standardizes the format of an 
##       address string to facilitate checking for address similarity. It 
##       performs the following steps:
##          1. Converts all characters to lowercase.
##          2. Normalizes spaces around commas and retains commas.
##          3. Removes all non-alphanumeric characters except for commas and spaces.
##          4. Normalizes multiple spaces to a single space.
##          5. Trims leading and trailing whitespace.
## 
##    4. find_components: This function performs Depth-First Search (DFS) to 
##       find all nodes in the connected component. It's used to identify similar 
##       addresses within a specified tolerance range, creating unique groups. 
##       Utilized in the `find_similar_addresses()` function.
## 
##    5. find_similar_addresses: This function groups addresses based on their 
##       similarity using a specified threshold. It preprocesses the addresses, 
##       builds a similarity graph, and identifies groups of similar addresses.
## 
##    6. find_first_one: Finds the date column name where the first 1 
##       occurs. Used for arranging the rows associated with one ABI 
##       in descending order: i.e. older address to recent address.
## 
##    7. usps_quota_mark_api: Select groups for API processing under a quota 
##       (all-or-nothing per group). This helper marks records for an external 
##       API call while respecting a hard quota measured in *unique* $$ (group, 
##       item) $$ pairs (e.g., unique addresses per group).
## 
##       Key behavior:
##          - **All-or-nothing per group**: if a group is selected, all eligible 
##            records in that group are marked.
##          - **Quota unit**: quota is consumed by the number of *unique items* 
##            within each group among eligible records (not raw row count).
##          - **Selection**:
##          - If $$quota = "all"$$ (or $$Inf$$ or $$NULL$$), all eligible groups/items are selected.
##          - If $$quota$$ is numeric, groups are considered in random order and 
##            added greedily until quota is exhausted (not guaranteed optimal).
##          - **Reproducibility**: set $$seed$$ to make randomized selection 
##            deterministic.
## 
##    8. make_zip5_candidates: USPS/lookup data sometimes disagrees when a ZIP 
##       has leading/trailing zeros. This helper:
##          1) normalizes input to a 5-digit ZIP (keeps leading zeros),
##          2) counts edge zeros (leading + trailing),
##          3) strips ONLY those edge zeros to get the core,
##          4) rebuilds a sequence of candidate ZIPs by moving zeros one-by-one
##             from the front to the back.
## 
##    9. generate_usps_token: Requests an OAuth access token from USPS using the 
##       client credentials grant. Intended for use by `validate_usps_address()`.
##       
##   10. validate_usps_address: Validate and standardize a US address via the 
##       USPS Addresses API (v3). Obtains an OAuth token via 
##       \code{generate_usps_token()} (client credentials flow), then calls the 
##       USPS Addresses v3 endpoint to validate and standardize the supplied 
##       address. Returns a one-row tibble of the preferred USPS-formatted 
##       address on success.
##       
##       Source: https://developers.usps.com/addressesv3
##       Example: https://github.com/USPS/api-examples
##       
##   11. zip_to_pred_city: Given a ZIP5, look up the predicted city by trying 
##       the original ZIP and any generated alternatives (e.g., shifting leading 
##       zeros) until a match is found.
## 
##   12. pick_best: Select the Best Anchor Row from a Pool of Candidate Address 
##       Records. Given a pool of candidate rows (typically sharing the same 
##       \code{address_line_1} or other clustering key), \code{pick_best()} 
##       scores each candidate by its geo and temporal compatibility with all 
##       rows in \code{reference_pool} (or \code{dt} itself if no reference 
##       pool is supplied) and returns the single most representative row to 
##       serve as the anchor for downstream matching.
## 
##       This version is written to be \strong{schema-stable}: it always 
##       attaches the scoring columns \code{has_zip4}, \code{has_geo}, 
##       \code{geo_agree}, \code{year_overlap}, and \code{year_gap_total} 
##       \emph{even when} \code{dt} has only one row. This prevents grouped 
##       \code{data.table} operations from failing due to inconsistent columns 
##       across groups.
## 
##   13. pick_best_by_city: Pick the Best Anchor Row \emph{Within Each} 
##       \code{pred_city}. \code{pick_best_by_city()} applies \code{\link{pick_best}} 
##       separately to each \code{pred_city} group in \code{candidates_dt}, 
##       returning exactly one selected anchor row per city. Each city’s 
##       candidates are scored against the same \code{match_pool} (the 
##       “reference context”), which is useful when anchors must be optimized 
##       for compatibility with a broader set of rows (e.g., \code{dtT ∪ dtF})
##       rather than only within-city candidates.
## 
##   14. census_geo_show_options: Show available Census Geocoder benchmarks and 
##       vintages. Downloads the Census Geocoder benchmark list, optionally 
##       filters it, then downloads the vintages available for each benchmark 
##       displayed. Results are printed and also returned invisibly as a list 
##       of tibbles.
## 
##   15. census_geo_make_tries: Build a `tries` list from benchmarkName + 
##       vintageName pairs, Converts a human-readable specification 
##       (benchmarkName + vintageName strings) into the `tries` structure 
##       required by `validate_geolocation()`.
##       
##   16. build_addr_geo_url: Build a U.S. Census Geocoder request URL for 
##       structured address geographies Constructs the URL for the Census 
##       Geocoder endpoint \code{/geocoder/geographies/address} using a 
##       structured address (street/city/state/zip) and an explicit benchmark 
##       + vintage.
##       
##   17. call_census_geocoder: Call the Census Geocoder and parse the JSON 
##       response. Issues a GET request to the provided Census Geocoder URL and 
##       parses the returned JSON payload. The function marks the call as 
##       successful (\code{ok=TRUE}) only if:
##          - HTTP status code is 200, and
##          - \code{result$addressMatches} exists and contains at least 
##                    one match.
## 
##   18. select_best_match: Select the best candidate address match from a Census 
##       Geocoder response. Given a parsed Census Geocoder API response 
##       containing one or more candidate address matches, applies a prioritized 
##       decision procedure to select a single best match. Designed to be called 
##       immediately after a successful \code{call_census_geocoder()} result 
##       inside \code{validate_geolocation()}.
## 
##   19. resolve_vintage_id: Resolve a Census Geocoder vintage value to a 
##       numeric vintage id. Converts a vintage value — either a numeric id or 
##       a vintage name string — into the numeric id expected by the Census 
##       Geocoder API. When a name string is supplied, the function hits the 
##       \code{/geocoder/vintages} endpoint for the given benchmark and matches 
##       by \code{vintageName}. Results are cached in a caller-supplied 
##       environment so the endpoint is only hit once per unique benchmark per 
##       \code{validate_geolocation()} call.
##       
##   20. validate_geolocation: GGeocode an address (Census Geocoder) and return 
##       the best match, trying multiple benchmark/vintage pairs. Queries the 
##       U.S. Census Geocoder "geographies/address" endpoint using a structured 
##       address (street/city/state/zip). Tries a prioritized sequence of 
##       benchmark/vintage combinations until it gets at least one candidate 
##       match, then applies a "best-candidate" selection procedure:
##          - If exactly one candidate, take it.
##          - If multiple candidates, prefer those whose ZIP matches the input ZIP.
##          - If still ambiguous, use \code{find_similar_addresses()} to pick the
##            most similar candidate to the input address string.
##          - If similarity logic does not resolve, fall back to the first candidate.
## 
##       This is designed to validate and lock in a lon/lat for an address before 
##       you later assign decennial geographies via TIGER/Line shapefiles 
##       (point-in-polygon).
## 
##   21. read_state_gpkgs_for_data: Read per-state TIGER block GeoPackages for 
##       the unique states present in a dataset. This helper finds the unique 
##       states represented in `data`, locates each state's output GeoPackage 
##       in `out_root`, and reads *all layers* from each GeoPackage.
## 
##   22. add_decennial_geoid_block: Add decennial Census block GEOIDs 
##       (2000/2010/2020) to candidate addresses. Given point locations 
##       (lon/lat already converted to an $$sf$$ POINT object), this function 
##       spatially assigns each point to its containing Census block for 
##       multiple decennial vintages.
##       
##       The join is performed state-by-state (outer loop) to:
##          - keep joins small and fast,
##          - avoid mixing CRSs across states/years,
##          - align with your per-state GeoPackage storage pattern.
## 
##   23. decode_zcta: Decode (Assign) ZCTA Codes to Point Locations. Given a 
##       set of candidate point locations and a ZCTA polygon layer (e.g. 2000, 
##       2010, or 2020 vintage), this function performs a point-in-polygon 
##       spatial join and returns the ZCTA code for each point.
## 
##       **Performance:** The state-based pre-filter (`state_col` + `area_states`) 
##       is applied to the raw `zcta_sf` object *before* any CRS transformation, 
##       column subsetting, or spatial join. This means all subsequent 
##       operations work on a much smaller polygon set, which is the primary 
##       speed lever for large national ZCTA layers.
## 
##       **CRS:** Points are re-projected into the CRS of `zcta_sf` before the
##       join. The original `cand_sf` object is not modified.
## 
##       **Duplicate matches:** `largest = TRUE` in `sf::st_join` ensures at most 
##       one polygon match is retained per point. Duplicate matches are rare but 
##       can occur at polygon boundaries or with invalid geometries.
## 
##       **Geometry warning suppression:** The common sf warning about attributes 
##       being "spatially constant throughout all geometries" is suppressed; it 
##       is benign for ZCTA layers because `area_code` is constant per feature.
## 
##   24. decode_cbsa_csa: Decode (Assign) CBSA and CSA Codes to Point Locations.
##       Given a set of candidate point locations and a combined CBSA/CSA polygon
##       layer, this function performs two point-in-polygon spatial joins—one for
##       Core Based Statistical Areas (CBSAs) and one for Combined Statistical 
##       Areas (CSAs)—and returns the corresponding codes and CBSA 
##       metropolitan/micropolitan level for each point.
## 
##       **Performance:** The state-based pre-filter (`state_col` + `area_states`) 
##       is applied to the raw `cbsa_csa_sf` object *before* any CRS 
##       transformation, polygon splitting, or spatial join. All subsequent 
##       operations therefore work on a much smaller polygon set, which is the 
##       primary speed lever for large national CBSA/CSA layers.
## 
##       **Two-pass joining:** After the pre-filter, the polygon layer is split 
##       into CBSA and CSA subsets and each is joined independently. This allows 
##       CBSA-only attributes (`area_level`) to be handled cleanly without 
##       polluting the CSA result.
## 
##       **Post-join state masking (row-preserving):** After each join, a 
##       secondary, row-level state-consistency check is applied via an internal 
##       `mask_by_state()` helper. If the matched polygon’s `area_states` does 
##       not include the point’s state, the polygon-derived attributes (e.g. 
##       `area_code`, `area_level`) are set to `NA` for that row. Importantly, 
##       rows are *not removed*, preserving the invariant of one output row per 
##       input point and avoiding dimension mismatches when combining CBSA and 
##       CSA results.
## 
##       **CRS:** Points are re-projected into the native CRS of the 
##       (now-filtered) `cbsa_csa_sf` layer. The original `cand_sf` object is 
##       not modified.
## 
##       **Duplicate matches:** `largest = TRUE` in `sf::st_join` is used to 
##       prefer a single polygon when a point matches more than one feature 
##       (rare boundary ambiguities). Where an sf backend does not enforce a 
##       single match, downstream code should ensure one record per `row_id`.
## 
##       **Geometry warning suppression:** The common sf warning about 
##       attributes being "spatially constant throughout all geometries" is 
##       suppressed; it is benign here because `area_code` and `area_level` are 
##       constant per feature.
## 
##   25. format_year_ranges: Format a set of years into compact consecutive 
##       ranges (e.g., "2001:2003, 2006"). Takes a vector of years (possibly 
##       unsorted and with duplicates) and returns a human-readable string where 
##       consecutive years are collapsed into "start:end" ranges and separated 
##       by ", ".
## 
##   26. parse_years: Parse a formatted year-range string into an integer vector 
##       of individual years.
## 
##   27. expected_vintages: Map a vector of years to their corresponding 
##       decennial census vintage labels. Uses the standard decennial period 
##       boundaries: 2000 -> 2000-2009, 2010 -> 2010-2019, 2020 -> 2020-2029.
##
##   28. parse_vintages: Parse a comma-separated vintage string into an integer 
##       vector.
## 
##   29. check_alignment: Check whether a boundary's recorded vintages cover all 
##       decennial periods implied by the archive year range.
## 
##       Intended for use with `mapply()` over rows of a summarised data.table 
##       where `archive_versions_present` has already been formatted by 
##       `format_year_ranges()`.
## 
##   30. check_alignment_cbsa: Check whether a CBSA/CSA boundary's recorded 
##       vintages cover all periods implied by the archive year range.
## 
##       Mirrors `check_alignment()` but resolves archive years against
##       `cbsa_vintage_map` so the 2000-2009 period maps to the `2007` vintage 
##       label rather than `2000`.
## 
##   31. write_list_to_xlsx: Write a named list of tables to a multi-sheet Excel 
##       workbook (.xlsx). Takes a list where each element is a 
##       data.frame/tibble/data.table and writes each element to its own 
##       worksheet in an Excel file. List names are used as sheet names; 
##       unnamed/blank elements are assigned default names.
## 
##   32. write_list_to_duckdb: Write a list of tables to a single DuckDB 
##       database file. A lightweight replacement for writing a multi-sheet 
##       Excel workbook. Each element of `lst` is written as its own DuckDB 
##       table (analogous to an XLSX sheet) inside one `.duckdb` file.
## 
##       This workflow does not require DuckDB extensions (it uses built-in 
##       DuckDB functionality). Optionally, the function can verify that the 
##       user's home directory is writable and, if so, set DuckDB's storage home 
##       there to provide a stable location for extension caching *if extensions 
##       are ever used*.
## 
##   33. read_list_from_duckdb: Read tables from a DuckDB database file into a 
##       named list. Companion to write_list_to_duckdb(): each DuckDB table is 
##       returned as one list element (analogous to reading sheets from an XLSX 
##       workbook).
## 
##   34. make_ranges: Chunk by unique ABI (NOT row ranges). Returns a data.table 
##       with one row per chunk:
##          - start_abi/end_abi: positions in the unique-ABI vector
##          - label: human-readable label
##          - abi_list: list-column containing the ABI values in that chunk
## 
##   35. normalize_address: Normalize an address string for comparison.
##       Oxygen labels (what this normalizer tries to do):
##          - Standardize whitespace (trim + collapse multiple spaces)
##          - Standardize the punctuation between $$\text{STATE}$$ and $$\text{ZIP}$$:
##            "AK 99803-2360"  and  "AK, 99803-2360"  both become "AK 99803-2360"
##          - Leave other punctuation/case largely unchanged so we don't 
##            over-normalize
## 
##   36. compare_tabs: Compare "new" vs "old" tabs on (address, year) keys.
##          1) Validate that required columns exist in each table.
##          2) Build a key table for each input with two standardized columns:
##                - .addr = normalized address (from the specified address column)
##                - .year = archived year (from the specified year column)
##          3) Deduplicate keys (distinct) so comparisons are set-based.
##          4) Compute two set differences:
##                - Keys present in new_tab but not in old_tab
##                - Keys present in old_tab but not in new_tab
##          5) Return counts + a few example rows from each difference set.
## 
##   37. compile_parquet_folder: Compile a folder of Parquet result files and 
##       generate QC summaries. Opens a directory of Parquet files as a single 
##       Arrow Dataset (lazy “compiled” handle) and computes several QC tables 
##       by iterating file-by-file with a progress bar. QC is computed on an 
##       in-memory tibble per file to preserve base-R semantics for $$nrow()$$, 
##       $$is.na()$$, and $$table()$$.
## 
##   38. extract_range: Extract a numeric range (from/to) from a string like 
##       "234001 to 235000". Parses a character string containing a range 
##       expressed as $$\text{<from> to <to>}$$ (allowing arbitrary whitespace 
##       around "to"), and returns the endpoints as integers named `from` and 
##       `to`.
## 
##   39. compile_duckdb_folder: Compile DuckDB QC outputs from a folder 
##       (with progress, header cleanup, and ABI QC). Reads all DuckDB 
##       \code{.db} files in a directory (optionally recursively), loads each
##       file via \code{read_list_from_duckdb()}, normalizes column names 
##       (including dotted headers), optionally performs an ABI integrity QC 
##       check, and then binds like-named tables across files.
## 
##       Column-name normalization:
##          - Renames \code{"Allow.USPS.API."} to \code{"Allow USPS API"}.
##          - Replaces one-or-more periods with spaces (e.g., 
##            \code{"Any.Addresses.Line.1.NA"} becomes 
##            \code{"Any Addresses Line 1 NA"}).
##          - Collapses repeated whitespace and trims.
## 
##       ABI QC (optional): When \code{abi_ref} is provided, the function checks 
##       every list element (table/data.frame) that contains an ABI column 
##       (case-insensitive match to \code{"abi"}). For each file and each 
##       ABI-bearing table, it compares the unique ABIs present to the expected 
##       ABIs from \code{abi_ref[from:to]}, where \code{from/to} are parsed from 
##       the filename pattern \code{"QC_<from> to <to>"}. Unexpected ABIs cause 
##       an error; missing ABIs are summarized in \code{out$qc_import}.
## 
##   40. write_qc_groups: Writes multiple *groups* of QC tables into an open 
##       DuckDB connection using a consistent naming convention: 
##       $$\texttt{<prefix>__<qc\_name>}$$
## 
##       This is useful when each batch (or cohort) has a list of QC tables, 
##       but some QC tables may be missing (`NULL`) in some groups. To avoid 
##       schema drift and keep downstream reads predictable, the function can 
##       create 0-row placeholder tables using a schema "template" learned from 
##       the first non-`NULL` instance of each QC table name across all groups.
## 
##   41. import_church_db: Import church-closures DuckDB tables (data + QC) with 
##       minimal dependencies. Reads tables from a DuckDB database file located 
##       at `db_path`. Designed to avoid `dplyr`/`dbplyr` and return a simple R 
##       list:
##          - optionally the main compiled `data` table
##          - QC tables grouped by prefix (e.g., `import_qc_18`, `import_qc_20`)
## 
##       QC tables are expected to follow the naming pattern: <prefix>__<qc_name>
##       For example: `import_qc_20__abi_check` becomes accessible as
##       `res$import_qc_20$abi_check`.
## 
##   42. rbind_qc: Row-bind two QC tables with column alignment (NULL-safe).
##       Takes two tabular objects (typically data.frames) and returns a single
##       data.frame created by row-binding them. If the two inputs have 
##       different columns, missing columns are added and filled with `NA` so 
##       that `rbind()` succeeds. `NULL` inputs are treated as “missing tables”.
## 
##   43. flag_boxplot: Boxplot + jitter for QC flag distributions across 
##       arrays/files. Creates a compact visualization for QC flag summaries 
##       (e.g., `address_verified`, `geoid_match`) where each row represents a 
##       batch/array and `pct` is the percent of unique addresses in a given 
##       flag category. The plot shows:
##          - A boxplot summarizing the distribution of `pct` by flag value, and
##          - Jittered points showing each batch/array observation.
## 
##   44. join_places_with_zip_fix: Join city/state records to a places table, 
##       with ZIP-based fallback for missing coordinates. Performs a two-step 
##       enrichment of a city-level dataset with longitude/latitude:
##          1) primary join on (state, city) to `places_dt`;
##          2) for rows still missing lon/lat, uses `zipcode` to look up 
##             standardized city/state from `zip_city_lookup`, then re-joins to 
##             `places_dt` to recover coordinates.

## ----------------------------------------------------------------
## FUNCTIONS

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
  #' @param zip A ZIP code or vector of ZIP codes (character or numeric). The 
  #'            first 5 digits are used; non-digits are ignored.
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



preprocess_address <- function(address) {
  #' @description 
  #' This function standardizes the format of an address string to facilitate
  #' checking for address similarity. It performs the following steps:
  #' 1. Converts all characters to lowercase.
  #' 2. Normalizes spaces around commas and retains commas.
  #' 3. Removes all non-alphanumeric characters except for commas and spaces.
  #' 4. Normalizes multiple spaces to a single space.
  #' 5. Trims leading and trailing whitespace.
  #'
  #' @param address A string containing the address to be standardized.
  #'
  #' @return A cleaned and standardized address string.
  
  # Convert to lowercase
  address <- tolower(address)
  
  # Normalize spaces around commas and retain commas in addresses
  address <- gsub("\\s*,\\s*", ", ", address)
  
  # Remove all characters except alphanumeric characters, commas, and spaces
  address <- gsub("[^a-z0-9, ]", "", address)
  
  # Normalize multiple spaces to a single space
  address <- gsub("\\s+", " ", address)
  
  # Trim leading and trailing whitespace
  address <- gsub("^\\s+|\\s+$", "", address)
  
  return(address)
}




find_components <- function(node, visited, address_graph) {
  #' @description
  #' This function performs Depth-First Search (DFS) to find all nodes in 
  #' the connected component. It's used to identify similar addresses within
  #' a specified tolerance range, creating unique groups. Utilized in
  #' the `find_similar_addresses()` function.
  #' 
  #'
  #' @param node An integer representing the starting node in the undirected 
  #'             graph. Each node represents similar addresses defined by the 
  #'             `stringdist(method = "jw")` function.
  #'             
  #' @param visited A logical vector indicating whether a node has been visited.
  #' 
  #' @param address_graph A list where each element contains the indices of
  #'                      its neighboring nodes.
  #'
  #' @return A vector containing all nodes in the connected component of the 
  #'         graph.
  
  
  # Initialize stack with the starting node and create an empty vector to store 
  # the connected component nodes.
  stack <- c(node)
  component <- c()
  
  # Perform DFS until the stack is empty.
  while (length(stack) > 0) {
    # After getting the top node in the stack, remove it.
    top <- stack[length(stack)]
    stack <- stack[-length(stack)]
    
    if (!visited[top]) {
      # Mark the node as visited.
      visited[top] <- TRUE
      # Add the node to the connected component.
      component <- c(component, top)
      # Add the neighbors of the node to the stack.
      stack <- c(stack, address_graph[[top]])
    }
  }
  return(component)
}




find_similar_addresses <- function(addresses, threshold = 0.15) {
  #' @description
  #' This function groups addresses based on their similarity using a specified 
  #' threshold. It preprocesses the addresses, builds a similarity graph, and 
  #' identifies groups of similar addresses.
  #' 
  #' @param addresses A character vector containing the addresses to be grouped.
  #' @param threshold A numeric value specifying the similarity threshold 
  #'                  (default is 0.15). Addresses with a similarity score 
  #'                  below this threshold are considered similar.
  #' 
  #' @return A list where each element is a group of similar addresses. Only
  #'         gives the uniquely defined address, and does not list redundancies.
  
  
  # Preprocess addresses to standardize the format.
  processed_addresses <- sapply(addresses, preprocess_address)
  n <- length(processed_addresses)
  
  # Initialize graph.
  address_graph <- vector("list", n)
  
  # Build the similarity graph.
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      # Compute the similarity between address nodes i and address j by adding
      # an edge from i to j and j to i, respectively.
      if (stringdist(processed_addresses[i], processed_addresses[j], method = "jw") < threshold) {
        address_graph[[i]] <- c(address_graph[[i]], j)
        address_graph[[j]] <- c(address_graph[[j]], i)
      }
    }
  }
  
  # Initialize the visited vector and a list to store unique groups.
  visited <- rep(FALSE, n)
  unique_groups <- list()
  
  # Find connected components for each unvisited node.
  for (i in 1:n) {
    if (!visited[i]) {
      component <- find_components(i, visited, address_graph)
      unique_groups <- c(unique_groups, list(sort(unique(addresses[component]))))
    }
  }
  
  # Convert address groups to strings and filter out duplicates.
  string_groups <- sapply(unique_groups, function(group) paste(sort(group), collapse = " ||| "))
  unique_string_groups <- unique(string_groups)
  unique_address_groups <- lapply(unique_string_groups, function(sgroup) unlist(strsplit(sgroup, " \\|\\|\\| ")))
  
  return(unique_address_groups)
}




find_first_one <- function(...) {
  #' @description
  #' This function finds the first column where a 1 occurs in a given row of a 
  #' data frame. It is used for arranging rows in descending order, from older 
  #' dates to more recent dates.
  #' 
  #' @param ... Variable arguments representing the elements of a row in a given 
  #'            data frame.
  #' 
  #' @return A character string representing the name of the first column where 
  #'         a 1 occurs. If no 1 is found, returns NA.
  
  
  # Convert the row elements into a single vector.
  row <- c(...)
  
  # Find the index of the first occurrence of 1.
  first_one_index <- which(row == 1)
  
  if (length(first_one_index) == 0) {
    # If there is no 1 in the row, return NA.
    return(NA)
    
  } else {
    # Return the name of the first column where a 1 occurs, removing any "X" 
    # prefix added to numeric column names.
    return(str_replace(names(row)[first_one_index[1]], "X", ""))
    
  }
}




usps_quota_mark_api <- function(df,
                                quota = NULL,        # NULL/"all"/Inf => select all eligible
                                group_col = "abi",
                                item_col = "combined_address",
                                mark_col = "do_api",
                                already_flag_col = NULL,
                                exclude_already = TRUE,
                                seed = NULL,
                                verbose = TRUE) {
  #' Select groups for API processing under a quota (all-or-nothing per group)
  #' This helper marks records for an external API call while respecting a hard 
  #' quota measured in *unique* $$ (group, item) $$ pairs (e.g., unique addresses 
  #' per group).
  #'
  #' Key behavior:
  #' - **All-or-nothing per group**: if a group is selected, all eligible records in that
  #'   group are marked.
  #' - **Quota unit**: quota is consumed by the number of *unique items* within each group
  #'   among eligible records (not raw row count).
  #' - **Selection**:
  #'   - If $$quota = "all"$$ (or $$Inf$$ or $$NULL$$), all eligible groups/items are selected.
  #'   - If $$quota$$ is numeric, groups are considered in random order and added greedily
  #'     until quota is exhausted (not guaranteed optimal).
  #' - **Reproducibility**: set $$seed$$ to make randomized selection deterministic.
  #'
  #' @param df A data.frame/tibble containing at least a group column and an item column.
  #' @param quota Either a single non-negative number (max unique $$ (group,item) $$ pairs),
  #'   or one of $$NULL$$ / $$Inf$$ / `"all"` to select everything eligible.
  #' @param group_col Character scalar. Name of the grouping column (e.g., ABI, customer_id).
  #' @param item_col Character scalar. Name of the item column used to count unique units
  #'   against quota (e.g., address, identifier).
  #' @param mark_col Character scalar. Name of the logical output column to create (TRUE = selected).
  #' @param already_flag_col Optional character scalar. Name of a logical flag column indicating
  #'   items already processed previously.
  #' @param exclude_already Logical. If TRUE, exclude rows with $$already\_flag\_col = TRUE$$ from
  #'   costing/selection.
  #' @param seed Optional integer. If provided, controls random ordering used in selection.
  #' @param verbose Logical. If TRUE, emit a summary message about what was selected.
  
  stopifnot(is.data.frame(df))
  
  # Build a lightweight working view for costing/selection (do not alter df yet)
  dat <- df %>%
    dplyr::mutate(.group = .data[[group_col]],
                  .item  = .data[[item_col]])
  
  # Optionally exclude records already processed (for costing and selection only)
  if (!is.null(already_flag_col) && exclude_already) {
    dat <- dat %>% dplyr::filter(!isTRUE(.data[[already_flag_col]]))
  }
  
  # Only non-missing items can be sent to an API; these define eligibility
  dat_elig <- dat %>% dplyr::filter(!is.na(.item))
  
  # Quota is consumed by unique (group,item) pairs
  combos <- dat_elig %>% dplyr::distinct(.group, .item)
  
  # Cost per group = number of unique items in that group
  costs <- combos %>%
    dplyr::group_by(.group) %>%
    dplyr::summarise(n_item = dplyr::n_distinct(.item), .groups = "drop") %>%
    dplyr::mutate(n_item = as.integer(n_item))
  
  total_units  <- as.integer(sum(costs$n_item))
  total_groups <- as.integer(nrow(costs))
  
  # Interpret quota
  select_all <- is.null(quota) ||
    identical(quota, Inf) ||
    (is.character(quota) && length(quota) == 1L && tolower(quota) %in% c("all", "everything"))
  
  if (!select_all) {
    stopifnot(is.numeric(quota), length(quota) == 1, !is.na(quota), quota >= 0)
    quota <- as.integer(quota)
  }
  
  # ---- Select groups ----
  if (select_all) {
    
    picked <- as.character(costs$.group)
    picked_unit_n <- total_units
    remaining <- 0L
    
    # if (isTRUE(verbose)) {
    #   message(sprintf(
    #     "Selected ALL eligible units: %s unique (group,item) pairs across %s groups.",
    #     format(picked_unit_n, big.mark = ","),
    #     format(total_groups, big.mark = ",")
    #   ))
    # }
    
  } else {
    
    if (!is.null(seed)) set.seed(seed)
    
    # Randomize group order, then greedily add while quota allows
    ordered <- costs %>%
      dplyr::filter(n_item <= quota) %>%
      dplyr::mutate(rand = stats::runif(dplyr::n())) %>%
      dplyr::arrange(rand) %>%
      dplyr::select(.group, n_item)
    
    picked <- character(0)
    remaining <- as.integer(quota)
    
    for (i in seq_len(nrow(ordered))) {
      need <- ordered$n_item[i]
      if (need <= remaining) {
        picked <- c(picked, ordered$.group[i])
        remaining <- remaining - need
        if (remaining == 0L) break
      }
    }
    
    picked_unit_n <- as.integer(sum(ordered$n_item[ordered$.group %in% picked]))
    
    # if (isTRUE(verbose)) {
    #   message(sprintf(
    #     "Quota = %s; selected %s unique units across %s groups (remaining quota = %s).",
    #     format(quota, big.mark = ","),
    #     format(picked_unit_n, big.mark = ","),
    #     format(length(picked), big.mark = ","),
    #     format(remaining, big.mark = ",")
    #   ))
    # }
  }
  
  # Mark records in the original df: selected group AND non-missing item
  out <- df %>%
    dplyr::mutate(
      !!mark_col := (.data[[group_col]] %in% picked) & !is.na(.data[[item_col]])
    )
  
  list(
    data = out,
    picked_groups = picked,
    picked_unit_n = picked_unit_n,
    remaining_quota = remaining,
    group_costs = costs %>% dplyr::rename(!!group_col := .group)
  )
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




validate_usps_address <- function(consumer_key, consumer_secret,
                                  address1, address2 = "",
                                  city, state, zip5, zip4 = "") {
  #' Validate and standardize a US address via the USPS Addresses API (v3).
  #' Obtains an OAuth token via \code{generate_usps_token()} (client credentials
  #' flow), then calls the USPS Addresses v3 endpoint to validate and standardize
  #' the supplied address. Returns a one-row tibble of the preferred 
  #' USPS-formatted address on success.
  #' 
  #' Source: https://developers.usps.com/addressesv3
  #' Example: https://github.com/USPS/api-examples
  #'
  #' @param consumer_key Character. USPS API Consumer Key (client_id).
  #' @param consumer_secret Character. USPS API Consumer Secret (client_secret).
  #' @param address1 Character. Street address line 1 (e.g., \code{"55 Whitney Ave"}).
  #' @param address2 Character. Secondary address line (apt/suite/unit). Default \code{""}.
  #' @param city Character. City name (e.g., \code{"New Haven"}).
  #' @param state Character. State postal abbreviation (e.g., \code{"CT"}).
  #' @param zip5 Character. 5-digit ZIP code (required by the USPS API).
  #' @param zip4 Character. Optional ZIP+4 extension; must be exactly 4 digits if
  #'   supplied. Default \code{""}.
  #'
  #' @return A one-row \code{\link[tibble]{tibble}} with columns:
  #'   \describe{
  #'     \item{address_line_1}{Standardized street address.}
  #'     \item{address_line_2}{Standardized secondary address (apt/suite/unit).}
  #'     \item{city}{Standardized city name.}
  #'     \item{state}{State postal abbreviation.}
  #'     \item{zipcode}{5-digit ZIP code.}
  #'     \item{zipcode_ext}{ZIP+4 extension (empty string if not assigned).}
  #'     \item{ok}{Logical. \code{TRUE} on success, \code{FALSE} on any failure.}
  #'     \item{status}{Character. \code{"ok"} on success, or a short reason
  #'       string on failure — one of \code{"invalid_zip4_format"},
  #'       \code{"token_error"}, \code{"http_<code>"} (e.g. \code{"http_401"}),
  #'       \code{"parse_error"}, or \code{"no_address_in_response"}.}
  #'     \item{status_detail}{Character. Extended human-readable detail about the
  #'       failure (e.g. raw HTTP body, error message). Empty string on success.}
  #'   }
  #'   On any failure the address columns are empty strings, \code{ok} is
  #'   \code{FALSE}, and \code{status}/\code{status_detail} describe the reason.
  #'   A zero-row tibble is never returned — callers can always check \code{ok}.
  #'
  #' @section Error handling:
  #' \itemize{
  #'   \item \strong{Invalid ZIP+4 format:} Raises an immediate \code{stop()}
  #'     because this is a programmer/config error that must be fixed before
  #'     calling the API.
  #'   \item \strong{Token failure:} If \code{generate_usps_token()} throws,
  #'     caught via \code{tryCatch()} and returned as
  #'     \code{status = "token_error"}.
  #'   \item \strong{HTTP non-200:} Returned as \code{status = "http_<code>"}
  #'     with the raw response body in \code{status_detail}.
  #'   \item \strong{JSON parse error:} Returned as \code{status = "parse_error"}
  #'     with the error message in \code{status_detail}.
  #'   \item \strong{Missing address in response:} Returned as
  #'     \code{status = "no_address_in_response"}.
  #' }
  #'
  #' @references USPS Addresses API v3: \url{https://developers.usps.com/addressesv3}
  
  # ---------------------------------------------------------------------------
  # Helper: construct a consistent failure row so every return path has the
  # same tibble shape and callers can always check $ok without NULL guards.
  # ---------------------------------------------------------------------------
  fail <- function(status, detail = "") {
    tibble::tibble(
      address_line_1 = "",
      address_line_2 = "",
      city           = "",
      state          = "",
      zipcode        = "",
      zipcode_ext    = "",
      ok             = FALSE,
      status         = status,
      status_detail  = detail
    )
  }
  
  # ---------------------------------------------------------------------------
  # Hard stop: invalid ZIP+4 format is a config/programmer error that must be
  # fixed before the API is called. All other failures are soft returns.
  # ---------------------------------------------------------------------------
  if (nzchar(zip4) && !grepl("^[0-9]{4}$", zip4)) {
    stop("Invalid ZIPPlus4 format. Must be exactly 4 digits.")
  }
  
  # ---------------------------------------------------------------------------
  # Step 1: obtain OAuth token via client credentials flow.
  # Catch failures so a bad token does not crash the caller.
  # ---------------------------------------------------------------------------
  token <- tryCatch(
    generate_usps_token(consumer_key, consumer_secret),
    error = function(e) {
      message("USPS token request failed — ", conditionMessage(e))
      NULL
    }
  )
  
  if (is.null(token)) {
    return(fail("token_error", "generate_usps_token() did not return a token."))
  }
  
  # ---------------------------------------------------------------------------
  # Step 2: build query parameters.
  # ZIP5 is always included; ZIP+4 is appended only when supplied.
  # ---------------------------------------------------------------------------
  params <- list(
    streetAddress    = address1,
    secondaryAddress = address2,
    city             = city,
    state            = state,
    ZIPCode          = zip5
  )
  
  if (nzchar(zip4)) params$ZIPPlus4 <- zip4
  
  # ---------------------------------------------------------------------------
  # Step 3: build the full request URL and call the USPS API.
  # ---------------------------------------------------------------------------
  request_url <- httr::modify_url(
    "https://apis.usps.com/addresses/v3/address",
    query = params
  )
  
  resp <- httr::GET(
    url = request_url,
    httr::add_headers(
      accept        = "application/json",
      Authorization = paste("Bearer", token)
    )
  )
  
  # ---------------------------------------------------------------------------
  # Step 4: check HTTP status — parse the error message out of the JSON body
  # rather than storing the raw blob, falling back to the raw text only if
  # JSON parsing fails (e.g. plain-text error responses).
  # ---------------------------------------------------------------------------
  if (httr::status_code(resp) != 200) {
    body   <- httr::content(resp, "text", encoding = "UTF-8")
    detail <- tryCatch({
      parsed_err <- jsonlite::fromJSON(body, simplifyVector = TRUE)
      parsed_err$error$message %||% body
    }, error = function(e) body)
    return(fail(
      status = paste0("http_", httr::status_code(resp)),
      detail = detail
    ))
  }
  
  # ---------------------------------------------------------------------------
  # Step 5: parse JSON response. Catch malformed payloads without crashing.
  # ---------------------------------------------------------------------------
  parsed <- tryCatch(
    jsonlite::fromJSON(
      httr::content(resp, "text", encoding = "UTF-8"),
      simplifyVector = TRUE
    ),
    error = function(e) {
      message("USPS API response could not be parsed — ", conditionMessage(e))
      NULL
    }
  )
  
  if (is.null(parsed)) {
    return(fail("parse_error", "JSON parsing failed; check status_detail for raw body."))
  }
  
  # ---------------------------------------------------------------------------
  # Step 6: extract the address payload. USPS returns an `address` object when
  # validation succeeds; its absence means no match was found.
  # ---------------------------------------------------------------------------
  addr <- parsed$address
  
  if (is.null(addr)) {
    message("USPS API returned a response but contained no address object.")
    return(fail("no_address_in_response"))
  }
  
  # ---------------------------------------------------------------------------
  # Step 7: return a standardized one-row tibble with consistent column names.
  # ---------------------------------------------------------------------------
  tibble::tibble(
    address_line_1 = addr$streetAddress    %||% "",
    address_line_2 = addr$secondaryAddress %||% "",
    city           = addr$city             %||% "",
    state          = addr$state            %||% "",
    zipcode        = addr$ZIPCode          %||% "",
    zipcode_ext    = addr$ZIPPlus4         %||% "",
    ok             = TRUE,
    status         = "ok",
    status_detail  = ""
  )
}





zip_to_pred_city <- function(zip5, zip_city_lookup) {
  #' Given a ZIP5, look up the predicted city by trying the original ZIP and any
  #' generated alternatives (e.g., shifting leading zeros) until a match is found.
  #'
  #' @param zip5 A single ZIP value (character or numeric). May be NA/blank.
  #' @param zip_city_lookup Lookup object/table used by get_city_info().
  #'
  #' @return
  #' - NA_character_ if the input ZIP is missing/blank (no lookup attempted)
  #' - Otherwise, the first successful city returned by get_city_info()
  #' - "No Matches" if all candidates fail
  #'
  #' @details
  #' Candidate ZIPs are generated by make_zip5_candidates(zip5_raw). Candidates are
  #' tried in the order returned; the first "successful" result wins.
  
  # ---- OXYGEN: Step 1 — Normalize ZIP input ----------------------------------
  # Normalize input to a 5-character ZIP5:
  # - coerce to character
  # - trim whitespace
  # - left-pad with zeros to width 5 (e.g., "4123" -> "04123")
  zip5_raw <- ifelse(is.na(zip5) || zip5 == "", "", as.character(zip5))
  zip5_raw <- trimws(zip5_raw)
  zip5_raw <- ifelse(
    nzchar(zip5_raw),
    stringr::str_pad(zip5_raw, width = 5, side = "left", pad = "0"),
    ""
  )
  
  # If ZIP is still blank after normalization, stop early (no lookup attempted).
  if (!nzchar(zip5_raw)) return(NA_character_)
  
  # ---- OXYGEN: Step 2 — Generate candidate ZIPs ------------------------------
  # Generate ZIP5 alternatives to try (e.g., move stripped zero, etc.).
  # NOTE: If you need zip5_raw tried first, ensure make_zip5_candidates() returns
  # it first, or wrap with unique(c(zip5_raw, ...)).
  zip5_candidates <- make_zip5_candidates(zip5_raw)
  
  # ---- OXYGEN: Step 3 — Try lookup(s) in order -------------------------------
  # First successful lookup wins. "Successful" means:
  # - not NA
  # - not empty string
  # - not the sentinel "No Matches"
  for (z in zip5_candidates) {
    res <- get_city_info(z, zip_city_lookup = zip_city_lookup)
    
    ok <- !is.na(res) &&
      nzchar(res) &&
      !stringr::str_detect(res, stringr::regex("^\\s*No\\s*Matches\\b", ignore_case = TRUE))
    
    if (ok) return(res)
  }
  
  # ---- OXYGEN: Step 4 — Nothing worked ---------------------------------------
  "No Matches"
}




pick_best <- function(dt, geo_tol = 0.02, reference_pool = NULL) {
  #' Select the Best Anchor Row from a Pool of Candidate Address Records.
  #' Given a pool of candidate rows (typically sharing the same \code{address_line_1}
  #' or other clustering key), \code{pick_best()} scores each candidate by its geo
  #' and temporal compatibility with all rows in \code{reference_pool} (or \code{dt}
  #' itself if no reference pool is supplied) and returns the single most
  #' representative row to serve as the anchor for downstream matching.
  #'
  #' This version is written to be \strong{schema-stable}: it always attaches the
  #' scoring columns \code{has_zip4}, \code{has_geo}, \code{geo_agree},
  #' \code{year_overlap}, and \code{year_gap_total} \emph{even when} \code{dt} has
  #' only one row. This prevents grouped \code{data.table} operations from failing
  #' due to inconsistent columns across groups.
  #'
  #' @param dt A \code{data.table} of candidate rows to score and rank. Expected columns:
  #'   \itemize{
  #'     \item \code{latitude_avg}, \code{longitude_avg} – average coordinates (may be \code{NA})
  #'     \item \code{anchor_year_min}, \code{anchor_year_max} – year span of the record
  #'     \item \code{zip4} – 4-digit ZIP extension (may be \code{NA})
  #'     \item \code{n_geo} – count of geocoded observations backing the coordinates
  #'     \item \code{combined_address} – full address string used as a stable tie-break key
  #'   }
  #' @param geo_tol Numeric scalar. Maximum allowable difference in decimal degrees
  #'   for both longitude and latitude when assessing geo compatibility between two
  #'   rows. Default \code{0.02} (~2 km).
  #' @param reference_pool Optional \code{data.table}. When supplied, each candidate
  #'   in \code{dt} is scored against the rows in \code{reference_pool} rather than
  #'   against the other rows in \code{dt}. Use this when the anchor must be optimised
  #'   for compatibility with a broader pool (e.g. all \code{dtT} + \code{dtF} rows)
  #'   rather than just its own subset.
  #'
  #' @return A single-row \code{data.table} representing the best anchor candidate,
  #'   with additional scoring columns attached:
  #'   \itemize{
  #'     \item \code{has_geo} – \code{TRUE} if the row has valid coordinates
  #'     \item \code{geo_agree} – number of reference pool rows within \code{geo_tol}
  #'     \item \code{year_overlap} – number of reference pool rows with overlapping year spans
  #'     \item \code{year_gap_total} – sum of year gaps to non-overlapping reference rows
  #'     \item \code{has_zip4} – \code{TRUE} if a 4-digit ZIP extension is present
  #'   }
  #'
  #' @details
  #' Scoring priority (descending):
  #' \enumerate{
  #'   \item \strong{geo_agree} – maximise geo-compatible neighbours within \code{geo_tol}
  #'   \item \strong{year_overlap} – maximise temporally overlapping neighbours
  #'   \item \strong{year_gap_total} – minimise total year distance to non-overlapping rows
  #'   \item \strong{has_zip4} – prefer rows with a ZIP+4 extension
  #'   \item \strong{n_geo} – prefer rows backed by more geocoded observations
  #'   \item \strong{combined_address} – alphabetic stable tie-break
  #' }
  #' When \code{reference_pool} is supplied, self-comparisons are excluded by
  #' dropping any reference row whose \code{combined_address} matches the candidate.
  #'
  #' @note
  #' \code{data.table} modifies by reference. This function will add columns and
  #' reorder \code{dt} in-place. If you want to preserve the original, call
  #' \code{pick_best(copy(dt), ...)}.
  
  n <- nrow(dt)
  if (n == 0L) return(dt)  # safeguard: nothing to rank
  
  # Use reference_pool for scoring if provided, otherwise score within dt
  ref <- if (!is.null(reference_pool)) reference_pool else dt
  
  # Precompute which rows have valid coordinates
  has_geo_vec <- !is.na(dt$latitude_avg) & !is.na(dt$longitude_avg)
  ref_has_geo <- !is.na(ref$latitude_avg) & !is.na(ref$longitude_avg)
  
  # Initialize score vectors (ensures scoring columns exist even when n == 1)
  geo_agree      <- integer(n)
  year_overlap   <- integer(n)
  year_gap_total <- integer(n)
  
  if (n > 1L) {
    
    # ── 1. Geo compatibility score ───────────────────────────────────────────
    # For each candidate in dt, count how many rows in ref (excluding self)
    # are within geo_tol degrees (both lat and lon).
    geo_agree <- vapply(seq_len(n), function(i) {
      if (!has_geo_vec[i]) return(0L)
      ca_i <- dt$combined_address[i]
      sum(vapply(seq_len(nrow(ref)), function(k) {
        if (ref$combined_address[k] == ca_i) return(0L)   # exclude self
        if (!ref_has_geo[k]) return(0L)
        lon_ok <- abs(dt$longitude_avg[i] - ref$longitude_avg[k]) <= geo_tol
        lat_ok <- abs(dt$latitude_avg[i]  - ref$latitude_avg[k])  <= geo_tol
        as.integer(lon_ok & lat_ok)
      }, integer(1L)))
    }, integer(1L))
    
    # ── 2. Temporal compatibility score ─────────────────────────────────────
    # year_overlap:   number of ref rows (excluding self) whose year span overlaps.
    # year_gap_total: sum of nearest-edge year distances to non-overlapping ref rows.
    year_overlap <- vapply(seq_len(n), function(i) {
      ca_i <- dt$combined_address[i]
      sum(vapply(seq_len(nrow(ref)), function(k) {
        if (ref$combined_address[k] == ca_i) return(0L)   # exclude self
        overlaps <- dt$anchor_year_min[i] <= ref$anchor_year_max[k] &
          ref$anchor_year_min[k] <= dt$anchor_year_max[i]
        as.integer(overlaps)
      }, integer(1L)))
    }, integer(1L))
    
    year_gap_total <- vapply(seq_len(n), function(i) {
      ca_i <- dt$combined_address[i]
      sum(vapply(seq_len(nrow(ref)), function(k) {
        if (ref$combined_address[k] == ca_i) return(0L)   # exclude self
        overlaps <- dt$anchor_year_min[i] <= ref$anchor_year_max[k] &
          ref$anchor_year_min[k] <= dt$anchor_year_max[i]
        if (overlaps) return(0L)
        min(abs(dt$anchor_year_min[i] - ref$anchor_year_max[k]),
            abs(ref$anchor_year_min[k] - dt$anchor_year_max[i]))
      }, integer(1L)))
    }, integer(1L))
  }
  
  # ── 3. Attach scores and sort ────────────────────────────────────────────
  # Always attach score columns (even when n == 1) so downstream grouped
  # data.table operations see consistent columns across groups.
  dt[, `:=`(
    has_zip4       = !is.na(zip4),
    has_geo        = has_geo_vec,
    geo_agree      = geo_agree,
    year_overlap   = year_overlap,
    year_gap_total = year_gap_total
  )]
  
  setorder(dt,
           -geo_agree,
           -year_overlap,
           year_gap_total,
           -has_zip4,
           -n_geo,
           combined_address)
  
  dt[1L]
}




pick_best_by_city <- function(candidates_dt, match_pool, geo_tol = 0.02) {
  #' Pick the Best Anchor Row \emph{Within Each} \code{pred_city}.
  #' \code{pick_best_by_city()} applies \code{\link{pick_best}} separately to each
  #' \code{pred_city} group in \code{candidates_dt}, returning exactly one selected
  #' anchor row per city. Each city’s candidates are scored against the same
  #' \code{match_pool} (the “reference context”), which is useful when anchors must
  #' be optimized for compatibility with a broader set of rows (e.g., \code{dtT ∪ dtF})
  #' rather than only within-city candidates.
  #'
  #' @param candidates_dt A \code{data.table} of candidate rows. Must contain
  #'   \code{pred_city} and all columns required by \code{\link{pick_best}}.
  #' @param match_pool A \code{data.table} used as the scoring reference context
  #'   passed to \code{\link{pick_best}} via \code{reference_pool}.
  #' @param geo_tol Numeric scalar. Geo tolerance (decimal degrees) passed through
  #'   to \code{\link{pick_best}}. Default \code{0.02}.
  #'
  #' @return A \code{data.table} with exactly one row per unique \code{pred_city}
  #'   present in \code{candidates_dt}. The returned rows include all original
  #'   columns plus the scoring columns added by \code{\link{pick_best}}:
  #'   \code{has_zip4}, \code{has_geo}, \code{geo_agree}, \code{year_overlap},
  #'   \code{year_gap_total}.
  #'
  #' @details
  #' Internally this function does:
  #' \preformatted{
  #' candidates_dt[, pick_best(.SD, reference_pool = match_pool), by = pred_city]
  #' }
  #' Because \code{data.table} modifies by reference, \code{\link{pick_best}} will
  #' modify each group’s \code{.SD}. To avoid mutating the caller’s data, this
  #' function copies \code{candidates_dt} before grouping and also copies each
  #' \code{.SD} before scoring.
  #'
  #' @note
  #' The output schema stability depends on \code{\link{pick_best}} always attaching
  #' its scoring columns even when a city group has only one candidate row.
  
  # Basic validations (fail fast with clear messages)
  stopifnot(data.table::is.data.table(candidates_dt))
  stopifnot(data.table::is.data.table(match_pool))
  if (!"pred_city" %chin% names(candidates_dt)) {
    stop("pick_best_by_city(): candidates_dt must contain column 'pred_city'.")
  }
  
  # data.table modifies by reference; copy to avoid side effects in caller
  candidates_dt <- data.table::copy(candidates_dt)
  
  # Pick exactly one best row per pred_city, scoring against match_pool
  out <- candidates_dt[, {
    pick_best(data.table::copy(.SD), geo_tol = geo_tol, reference_pool = match_pool)
  }, by = pred_city]
  
  # Optional: make output deterministic/easy to scan
  data.table::setorder(out, pred_city)
  
  out
}



census_geo_show_options <- function(filter_benchmark_name = NULL, max_benchmarks = Inf) {
  #' Show available Census Geocoder benchmarks and vintages. Downloads the Census 
  #' Geocoder benchmark list, optionally filters it, then downloads the vintages 
  #' available for each benchmark displayed. Results are printed and also returned 
  #' invisibly as a list of tibbles.
  #'
  #' Notes:
  #' - Benchmarks and vintages each have an `isDefault` flag in the API. To avoid 
  #'   name collisions after joining, we rename them to `benchmark_default` and 
  #'   `vintage_default`.
  #'
  #' @param filter_benchmark_name Optional character scalar. If provided, only 
  #'                              benchmarks whose `benchmark_name` matches this 
  #'                              regex (case-insensitive) are included. 
  #'                              Example: `"Public_AR_"`.
  #'                              
  #' @param max_benchmarks Numeric. Maximum number of benchmarks to show/fetch 
  #'                       vintages for. Use this to limit API calls (because 
  #'                       vintages require one call per benchmark).
  #'
  #' @return (Invisibly) a list with:
  #'   \describe{
  #'     \item{benchmarks}{A tibble with `benchmark_id`, `benchmark_name`, `benchmark_default`.}
  #'     \item{vintages}{A tibble with `benchmark_id`, `benchmark_name`, `vintage_id`, `vintage_name`,
  #'                    `vintage_default`, `benchmark_default`.}
  #'   }
  #'
  #' @examples
  #' \dontrun{
  #' # Show everything (may be long)
  #' opt <- census_geo_show_options()
  #'
  #' # Show only Public_AR_* benchmarks (recommended)
  #' opt <- census_geo_show_options(filter_benchmark_name = "Public_AR_", max_benchmarks = 50)
  #' }
  #' @export
  
  # ---- 1) Fetch benchmarks ---------------------------------------------------
  # Benchmarks define which underlying address range data the geocoder uses.
  bench_url <- "https://geocoding.geo.census.gov/geocoder/benchmarks?format=json"
  
  # Parse JSON -> data frame-like object; `benchmarks` is the payload field from the API.
  bench_raw <- jsonlite::fromJSON(
    httr::content(httr::GET(bench_url), "text", encoding = "UTF-8"),
    simplifyVector = TRUE
  )$benchmarks
  
  # Keep only the fields we care about, and standardize types/names.
  bench <- tibble::as_tibble(bench_raw) %>%
    dplyr::transmute(
      benchmark_id      = as.numeric(id),
      benchmark_name    = benchmarkName,
      benchmark_default = dplyr::coalesce(isDefault, FALSE)
    ) %>%
    # Defaults first, then alphabetical.
    dplyr::arrange(dplyr::desc(.data$benchmark_default), .data$benchmark_name)
  
  # Optional benchmark name filter (regex, case-insensitive).
  if (!is.null(filter_benchmark_name)) {
    bench <- bench %>%
      dplyr::filter(grepl(filter_benchmark_name, .data$benchmark_name, ignore.case = TRUE))
  }
  
  # Optional cap: reduces output and reduces number of vintage API calls.
  if (is.finite(max_benchmarks)) {
    bench <- bench %>% dplyr::slice_head(n = max_benchmarks)
  }
  
  cat("\nAvailable benchmarks (benchmarkName -> id):\n")
  print(bench, n = nrow(bench))
  
  # ---- 2) Fetch vintages for each benchmark ----------------------------------
  # Vintages define the geography/vintage context for the request. The API requires
  # specifying a vintage along with a benchmark for the geographies/address endpoint.
  vintages <- lapply(bench$benchmark_id, function(bid) {
    
    v_url <- paste0(
      "https://geocoding.geo.census.gov/geocoder/vintages?format=json&benchmark=",
      bid
    )
    
    v_raw <- jsonlite::fromJSON(
      httr::content(httr::GET(v_url), "text", encoding = "UTF-8"),
      simplifyVector = TRUE
    )$vintages
    
    # Standardize fields; rename the default flag to avoid clashes after join.
    tibble::as_tibble(v_raw) %>%
      dplyr::transmute(
        benchmark_id    = as.numeric(bid),
        vintage_id      = as.numeric(id),
        vintage_name    = vintageName,
        vintage_default = dplyr::coalesce(isDefault, FALSE)
      )
  }) %>%
    dplyr::bind_rows() %>%
    # Add benchmark names/default flags to each vintage row.
    dplyr::left_join(bench, by = "benchmark_id") %>%
    # Sort within each benchmark: default vintages first, then alphabetical.
    dplyr::arrange(
      .data$benchmark_name,
      dplyr::desc(.data$vintage_default),
      .data$vintage_name
    )
  
  cat("\nAvailable vintages by benchmark (benchmarkName + vintageName):\n")
  print(vintages, n = nrow(vintages))
  
  # Return data invisibly so callers can programmatically use it without printing again.
  invisible(list(benchmarks = bench, vintages = vintages))
}




census_geo_make_tries <- function(spec) {
  #' Build a `tries` list from benchmarkName + vintageName pairs, Converts a 
  #' human-readable specification (benchmarkName + vintageName strings)
  #' into the `tries` structure required by `validate_geolocation()`.
  #'
  #' What it does:
  #' 1) Downloads the benchmark table, converts `benchmark_name` -> numeric `benchmark_id`.
  #' 2) Validates that each requested vintage_name exists for the associated benchmark_id.
  #' 3) Returns a `tries` list where each element is `list(benchmark=<id>, vintage=<vintage_name>)`.
  #'
  #' @param spec A data.frame/tibble with columns:
  #'   \describe{
  #'     \item{benchmark_name}{Character. A Census Geocoder `benchmarkName` value.}
  #'     \item{vintage_name}{Character. A Census Geocoder `vintageName` value.}
  #'   }
  #'
  #' @return A list of lists: `list(list(benchmark=<benchmark_id>, vintage=<vintage_name>), ...)`.
  #'
  #' @examples
  #' \dontrun{
  #' # First discover options:
  #' census_geo_show_options(filter_benchmark_name = "Public_AR_")
  #'
  #' # Then build tries:
  #' spec <- tibble::tibble(
  #'   benchmark_name = c("Public_AR_Census2020", "Public_AR_Census2020",
  #'                      "Public_AR_Current",  "Public_AR_ACS2025"),
  #'   vintage_name   = c("Census2020_Census2020", "Census2010_Census2020",
  #'                      "Current_Current",       "Current_ACS2025")
  #' )
  #' tries <- census_geo_make_tries(spec)
  #' }
  #' @export
  
  # Basic input validation: must have required columns.
  if (!all(c("benchmark_name", "vintage_name") %in% names(spec))) {
    stop("spec must contain columns: benchmark_name, vintage_name")
  }
  
  # ---- 1) Fetch benchmarks and build name -> id mapping ----------------------
  bench_url <- "https://geocoding.geo.census.gov/geocoder/benchmarks?format=json"
  bench_raw <- jsonlite::fromJSON(
    httr::content(httr::GET(bench_url), "text", encoding = "UTF-8"),
    simplifyVector = TRUE
  )$benchmarks
  
  bench <- tibble::as_tibble(bench_raw) %>%
    dplyr::transmute(
      benchmark_name = benchmarkName,
      benchmark_id   = as.numeric(id)
    )
  
  # Join user's spec onto the benchmark table to get numeric benchmark_id.
  spec2 <- tibble::as_tibble(spec) %>%
    dplyr::left_join(bench, by = "benchmark_name")
  
  # Fail early if any benchmark_name is unknown.
  if (any(is.na(spec2$benchmark_id))) {
    missing <- unique(spec2$benchmark_name[is.na(spec2$benchmark_id)])
    stop("Unknown benchmark_name(s): ", paste(missing, collapse = ", "))
  }
  
  # ---- 2) Validate vintages exist for each benchmark -------------------------
  # This is optional but helpful: it catches typos in vintage_name immediately.
  validate_one <- function(bid, vname) {
    
    v_url <- paste0(
      "https://geocoding.geo.census.gov/geocoder/vintages?format=json&benchmark=",
      bid
    )
    
    v <- jsonlite::fromJSON(
      httr::content(httr::GET(v_url), "text", encoding = "UTF-8"),
      simplifyVector = TRUE
    )$vintages
    
    if (!(vname %in% v$vintageName)) {
      stop("Vintage '", vname, "' not available for benchmark_id=", bid, ".")
    }
    
    TRUE
  }
  
  # Run validation for each row; errors stop immediately with a clear message.
  mapply(validate_one, spec2$benchmark_id, spec2$vintage_name)
  
  # ---- 3) Construct `tries` --------------------------------------------------
  # Keep the vintage as a *name* string because your `resolve_vintage_id()` can
  # translate name -> numeric vintage id at runtime (and cache results).
  Map(
    function(bid, vname) list(benchmark = bid, vintage = vname),
    spec2$benchmark_id,
    spec2$vintage_name
  )
}




build_addr_geo_url <- function(street, city, state, zip,
                               benchmark, vintage) {
  #' Build a U.S. Census Geocoder request URL for structured address geographies 
  #' Constructs the URL for the Census Geocoder endpoint 
  #' \code{/geocoder/geographies/address} using a structured address 
  #' (street/city/state/zip) and an explicit benchmark + vintage.
  #'
  #' @param street Character scalar. Street address line (e.g., "55 Whitney Ave").
  #' @param city Character scalar. City name (e.g. "New Haven").
  #' @param state Character scalar. State postal abbreviation (e.g., "CT").
  #' @param zip Character scalar. ZIP code (5-digit or ZIP+4).
  #' @param benchmark Numeric or character scalar. Census Geocoder benchmark identifier.
  #' @param vintage Numeric or character scalar. Census Geocoder vintage identifier (typically a numeric id).
  #'
  #' @return A character scalar URL suitable for passing to \code{httr::GET()}.
  #'
  #' @examples
  #' \dontrun{
  #' url <- build_addr_geo_url(
  #'   street = "55 Whitney Ave",
  #'   city   = "New Haven",
  #'   state  = "CT",
  #'   zip    = "06510",
  #'   benchmark = 2020,
  #'   vintage   = 430   # example id
  #' )
  #' }
  #' @export
  
  httr::modify_url(
    "https://geocoding.geo.census.gov/geocoder/geographies/address",
    query = list(
      format    = "json",
      benchmark = benchmark,
      vintage   = vintage,
      street    = street,
      city      = city,
      state     = state,
      zip       = zip
    )
  )
}




call_census_geocoder <- function(url) {
  #' Call the Census Geocoder and parse the JSON response. Issues a GET request 
  #' to the provided Census Geocoder URL and parses the returned JSON payload. 
  #' The function marks the call as successful (\code{ok=TRUE}) only if:
  #' \itemize{
  #'   \item HTTP status code is 200, and
  #'   \item \code{result$addressMatches} exists and contains at least one match.
  #' }
  #'
  #' @param url Character scalar. A fully formed request URL (typically from \code{build_addr_geo_url()}).
  #'
  #' @return A list with elements:
  #'   \describe{
  #'     \item{ok}{Logical. \code{TRUE} if at least one address match was returned.}
  #'     \item{parsed}{Parsed JSON (a nested list) when HTTP 200; otherwise \code{NULL}.}
  #'     \item{url}{The URL that was requested (useful for debugging/logging).}
  #'     \item{status}{Integer HTTP status code.}
  #'   }
  #'
  #' @examples
  #' \dontrun{
  #' out <- call_census_geocoder(url)
  #' if (out$ok) {
  #'   length(out$parsed$result$addressMatches)
  #' }
  #' }
  #' @export
  
  # Make request
  resp <- httr::GET(url)
  
  # Fail fast on non-200 responses
  if (httr::status_code(resp) != 200) {
    return(list(
      ok     = FALSE,
      parsed = NULL,
      url    = url,
      status = httr::status_code(resp)
    ))
  }
  
  # Parse JSON payload
  txt <- httr::content(resp, "text", encoding = "UTF-8")
  parsed <- jsonlite::fromJSON(txt, simplifyVector = FALSE)
  
  # Determine whether we got any candidate matches
  matches <- parsed$result$addressMatches
  ok <- !is.null(matches) && length(matches) > 0
  
  list(
    ok     = ok,
    parsed = parsed,
    url    = url,
    status = 200
  )
}




select_best_match <- function(parsed_response, street, city, state, zip) {
  #' Select the best candidate address match from a Census Geocoder response.
  #' Given a parsed Census Geocoder API response containing one or more candidate
  #' address matches, applies a prioritized decision procedure to select a single
  #' best match. Designed to be called immediately after a successful
  #' \code{call_census_geocoder()} result inside \code{validate_geolocation()}.
  #'
  #' @param parsed_response List. The full parsed JSON response from the Census
  #'   Geocoder API, as returned by \code{call_census_geocoder()}. The candidate
  #'   matches are expected at \code{parsed_response$result$addressMatches}.
  #' @param street Character scalar. Street address line of the original input
  #'   (e.g., \code{"55 Whitney Ave"}). Used to construct the target address
  #'   string for similarity matching.
  #' @param city Character scalar. City name of the original input
  #'   (e.g., \code{"New Haven"}).
  #' @param state Character scalar. State postal abbreviation of the original
  #'   input (e.g., \code{"CT"}).
  #' @param zip Character scalar. ZIP code of the original input; may be 5-digit
  #'   or ZIP+4 (e.g., \code{"06510"} or \code{"06510-1234"}). ZIP+4 values are
  #'   normalized to 5-digit for matching.
  #'
  #' @return The single best candidate as a list (one element of
  #'   \code{parsed_response$result$addressMatches}), or \code{NULL} if the
  #'   response contains no candidates. The returned element includes fields such
  #'   as \code{matchedAddress}, \code{coordinates}, \code{addressComponents},
  #'   and \code{geographies} as returned by the API.
  #'
  #' @section Selection procedure:
  #' Candidates are evaluated in this priority order:
  #' \enumerate{
  #'   \item \strong{Single candidate:} If only one candidate is returned,
  #'     accept it immediately.
  #'   \item \strong{ZIP match:} If exactly one candidate's ZIP matches the
  #'     (normalized) input ZIP, select it.
  #'   \item \strong{ZIP filtering:} If multiple candidates match the input ZIP,
  #'     restrict the pool to those before proceeding to similarity matching.
  #'   \item \strong{Similarity matching:} Use \code{find_similar_addresses()}
  #'     with an adaptive threshold (starting at \code{0.2}, tightened by
  #'     \code{0.01} each iteration) to find the candidate most similar to the
  #'     input address string. Stops when the pool is sufficiently narrowed,
  #'     the threshold reaches \code{0}, or all candidates are singletons.
  #'   \item \strong{Fallback:} If similarity matching does not resolve to a
  #'     single candidate, return the first candidate in the (possibly
  #'     ZIP-filtered) pool.
  #' }
  #'
  #' @details
  #' This function depends on the following helpers being defined in your
  #' environment:
  #' \itemize{
  #'   \item \code{find_similar_addresses()} — your existing similarity matcher.
  #'     Accepts a character vector where the first element is the target address
  #'     and the rest are candidates, plus a numeric \code{threshold} argument.
  #'     Returns a list of clusters.
  #'   \item The infix operators \code{\%||\%} (null coalescing) and
  #'     \code{\%!in\%} (negated \code{\%in\%}).
  #'   \item \code{stringr} — used for address string normalization
  #'     (\code{str_to_upper}, \code{str_flatten}).
  #'   \item The pipe \code{\%>\%} (either \code{magrittr} or base R \code{|>}).
  #' }
  
  # ---------------------------------------------------------------------------
  # Extract candidate list — return NULL immediately if the response is empty.
  # ---------------------------------------------------------------------------
  api_result <- parsed_response$result$addressMatches
  if (is.null(api_result) || length(api_result) == 0) return(NULL)
  
  # ---------------------------------------------------------------------------
  # Normalize ZIP: strip the +4 suffix if present so that ZIP comparisons
  # against candidate addressComponents$zip work correctly.
  # ---------------------------------------------------------------------------
  if (!is.null(zip) && grepl("^\\d{5}-\\d{4}$", zip)) {
    zip <- sub("-.*$", "", zip)
  }
  
  # ---------------------------------------------------------------------------
  # Build a standardized upper-case target address string used for similarity
  # comparisons later in the procedure.
  # ---------------------------------------------------------------------------
  target_address <- stringr::str_flatten(
    stringr::str_to_upper(c(street, city, state, zip)),
    ", "
  )
  
  # ---------------------------------------------------------------------------
  # Rule 1: single candidate — accept immediately, no further logic needed.
  # ---------------------------------------------------------------------------
  if (length(api_result) == 1) return(api_result[[1]])
  
  # ---------------------------------------------------------------------------
  # Extract per-candidate fields used in the selection rules below.
  # Safe extraction via %||% guards against missing fields in any candidate.
  # ---------------------------------------------------------------------------
  cand_addr <- vapply(api_result, function(x) x$matchedAddress %||% NA_character_, character(1))
  cand_zip  <- vapply(api_result, function(x) x$addressComponents$zip %||% NA_character_, character(1))
  
  # ---------------------------------------------------------------------------
  # Rule 2: ZIP match.
  # If exactly one candidate's ZIP matches the input ZIP, select it.
  # If multiple candidates match, narrow the pool to those before proceeding
  # to similarity matching (Rule 3).
  # ---------------------------------------------------------------------------
  zip_hits <- which(cand_zip %in% zip)
  
  if (length(zip_hits) == 1) {
    return(api_result[[zip_hits]])
  } else {
    
    # Multiple ZIP matches — restrict candidate pool before similarity logic.
    if (length(zip_hits) > 1) {
      api_result <- api_result[zip_hits]
      cand_addr  <- cand_addr[zip_hits]
      cand_zip   <- cand_zip[zip_hits]
    }
    
    # -------------------------------------------------------------------------
    # Rule 3: similarity tie-breaker (TARGET-CENTERED).
    # Pass the target address + all candidate addresses to find_similar_addresses().
    # Adaptively tighten the threshold (starting at 0.2, step -0.01) until the
    # *target's* cluster is narrowed to a resolvable size, the threshold bottoms
    # out at 0, or all candidates are already singletons.
    # -------------------------------------------------------------------------
    comparisons <- c(target_address, cand_addr)
    
    threshold <- 0.2
    repeat {
      match <- find_similar_addresses(comparisons, threshold = threshold)
      
      # Identify the cluster that contains the target address
      target_in_cluster <- vapply(match, function(x) any(x %in% target_address), logical(1))
      
      # If for some reason the target isn't present in any cluster, bail out
      if (!any(target_in_cluster)) break
      
      target_cluster <- unlist(match[target_in_cluster], use.names = FALSE)
      
      # Is the target cluster still too large (> 2 items)?
      too_many_target <- length(target_cluster) > 2
      
      # Exit when target cluster is sufficiently narrow, threshold exhausted,
      # or (after tightening) everything is singleton clusters.
      if (!too_many_target || threshold <= 0 ||
          (threshold < 0.2 && all(vapply(match, length, integer(1)) == 1))) break
      
      # Tighten threshold and retry
      threshold <- max(0, threshold - 0.01)
    }
    
    # -------------------------------------------------------------------------
    # If similarity produced a target-centered cluster of exactly two addresses,
    # the *other* address in that pair is the best candidate.
    # -------------------------------------------------------------------------
    target_in_cluster <- vapply(match, function(x) any(x %in% target_address), logical(1))
    
    if (any(target_in_cluster)) {
      target_cluster <- unlist(match[target_in_cluster], use.names = FALSE)
      
      if (length(target_cluster) == 2) {
        matched <- setdiff(target_cluster, target_address)
        
        hit <- which(cand_addr %in% matched)[1]
        if (!is.na(hit)) return(api_result[[hit]])
      }
    }
    
    # -------------------------------------------------------------------------
    # Rule 4: fallback — return the first candidate in the (possibly
    # ZIP-filtered) pool if all other rules failed to resolve.
    # -------------------------------------------------------------------------
    return(api_result[[1]])
  }
}




resolve_vintage_id <- function(benchmark, vintage, vintage_cache) {
  #' Resolve a Census Geocoder vintage value to a numeric vintage id. Converts a 
  #' vintage value — either a numeric id or a vintage name string — into the 
  #' numeric id expected by the Census Geocoder API. When a name string is 
  #' supplied, the function hits the \code{/geocoder/vintages} endpoint for the 
  #' given benchmark and matches by \code{vintageName}. Results are cached in a
  #' caller-supplied environment so the endpoint is only hit once per unique
  #' benchmark per \code{validate_geolocation()} call.
  #'
  #' @param benchmark Numeric. Census Geocoder benchmark code (e.g., \code{2020},
  #'   \code{4}, \code{8}). Must be provided; \code{NULL} or blank raises a
  #'   \code{stop()}.
  #' @param vintage Character or numeric. Either a vintage name string
  #'   (e.g., \code{"Census2020_Census2020"}) or a numeric vintage id
  #'   (e.g., \code{4}). Must be provided; \code{NULL} or blank raises a
  #'   \code{stop()}.
  #' @param vintage_cache An \code{environment} used to cache the vintage lookup
  #'   table per benchmark. Should be created once by the calling function and
  #'   passed in on every call. Avoids redundant hits to \code{/geocoder/vintages}
  #'   when multiple tries share the same benchmark.
  #'
  #' @return A length-1 numeric vintage id on success. On a soft/runtime failure
  #'   (network error, HTTP non-200, JSON parse error, vintage name not found),
  #'   returns \code{NA_real_} with a \code{reason} attribute describing the
  #'   failure. The reason will be one of:
  #'   \itemize{
  #'     \item \code{"vintage_lookup_network_error"}
  #'     \item \code{"vintage_lookup_http_<code>"} (e.g. \code{"vintage_lookup_http_400"})
  #'     \item \code{"vintage_lookup_parse_error"}
  #'     \item \code{"vintage_name_not_found"}
  #'   }
  #'
  #' @section Error handling:
  #' \itemize{
  #'   \item \strong{Hard stops (programmer/config errors):} \code{NULL} or blank
  #'     \code{benchmark} or \code{vintage} raise an immediate \code{stop()}
  #'     because these indicate a misconfigured \code{tries} list.
  #'   \item \strong{Soft skips (runtime/network errors):} All other failures are
  #'     caught, emitted via \code{message()}, and returned as a named
  #'     \code{NA_real_} so the caller can log the reason and continue to the next
  #'     attempt without crashing the run.
  #' }
  #'
  #' @details
  #' This function is an internal helper for \code{validate_geolocation()} and is
  #' not intended to be called directly. It depends on the following packages
  #' being available: \code{httr} and \code{jsonlite}.
  
  # Local helper: return NA_real_ with a reason attribute (soft failure).
  make_skip <- function(reason) {
    out <- NA_real_
    attr(out, "reason") <- reason
    out
  }
  
  # ---------------------------------------------------------------------------
  # Hard stops — NULL or blank benchmark/vintage indicate a misconfigured tries
  # list and must be fixed by the caller before running. These are programmer
  # errors, not runtime failures, so stop() is appropriate.
  # ---------------------------------------------------------------------------
  if (is.null(benchmark) || (!is.numeric(benchmark) && !nzchar(as.character(benchmark)))) {
    stop("benchmark must be provided as a numeric value (e.g. 2020, 4, 8).")
  }
  if (is.null(vintage) || !nzchar(as.character(vintage))) {
    stop("vintage must be provided as an ID (numeric) or a vintage name string.")
  }
  
  # ---------------------------------------------------------------------------
  # Numeric vintage id supplied directly — no API lookup needed.
  # ---------------------------------------------------------------------------
  if (is.numeric(vintage) && length(vintage) == 1) return(vintage)
  
  # ---------------------------------------------------------------------------
  # Vintage name supplied — resolve to numeric id via /geocoder/vintages.
  # Results are cached by benchmark key so the endpoint is only called once
  # per unique benchmark per validate_geolocation() call.
  # ---------------------------------------------------------------------------
  key <- paste0("bmk_", benchmark)
  
  if (!exists(key, envir = vintage_cache, inherits = FALSE)) {
    
    url <- httr::modify_url(
      "https://geocoding.geo.census.gov/geocoder/vintages",
      query = list(format = "json", benchmark = benchmark)
    )
    
    # Catch network-level errors (e.g. no connectivity, DNS failure).
    resp <- tryCatch(
      httr::GET(url, httr::timeout(30)),
      error = function(e) e
    )
    
    if (inherits(resp, "error")) {
      message(sprintf(
        "Skipping attempt: vintage lookup request failed for benchmark=%s — %s",
        benchmark, conditionMessage(resp)
      ))
      return(make_skip("vintage_lookup_network_error"))
    }
    
    # Catch unexpected HTTP status codes (e.g. invalid benchmark value).
    if (httr::status_code(resp) != 200) {
      message(sprintf(
        "Skipping attempt: vintage lookup returned HTTP %s for benchmark=%s.",
        httr::status_code(resp), benchmark
      ))
      return(make_skip(paste0("vintage_lookup_http_", httr::status_code(resp))))
    }
    
    # Catch JSON parse failures.
    txt <- httr::content(resp, "text", encoding = "UTF-8")
    
    if (grepl("^\\s*<(!DOCTYPE|html)\\b", txt, ignore.case = TRUE)) {
      message("Skipping attempt: vintages endpoint returned HTML (not JSON).")
      return(make_skip("vintage_lookup_html_response"))
    }
    
    v <- tryCatch(
      jsonlite::fromJSON(txt, simplifyVector = TRUE)$vintages,
      error = function(e) {
        message("Skipping attempt: failed to parse vintages JSON — ", conditionMessage(e))
        NULL
      }
    )
    
    if (is.null(v)) return(make_skip("vintage_lookup_parse_error"))
    
    # Store result in cache for reuse by subsequent tries with the same benchmark.
    assign(key, v, envir = vintage_cache)
  }
  
  # Match the supplied vintage name against the cached vintage table.
  v_df <- get(key, envir = vintage_cache, inherits = FALSE)
  hit  <- v_df[v_df$vintageName == as.character(vintage), , drop = FALSE]
  
  if (nrow(hit) == 0) {
    message(sprintf(
      "Skipping attempt: vintage name '%s' not found for benchmark=%s.",
      vintage, benchmark
    ))
    return(make_skip("vintage_name_not_found"))
  }
  
  as.numeric(hit$id[1])
}




validate_geolocation <- function(street, city, state, zip,
                                 tries = list(
                                   list(benchmark = 2020, vintage = "Census2020_Census2020"),
                                   list(benchmark = 2020, vintage = "Census2010_Census2020"),
                                   list(benchmark = 4,    vintage = "Current_Current"),
                                   list(benchmark = 8,    vintage = "Current_ACS2025")
                                 ),
                                 quiet = FALSE) {
  #' Geocode an address (Census Geocoder) and return the best match, trying
  #' multiple benchmark/vintage pairs. Queries the U.S. Census Geocoder 
  #' "geographies/address" endpoint using a structured address 
  #' (street/city/state/zip). Tries a prioritized sequence of benchmark/vintage 
  #' combinations until it gets at least one candidate match, then applies a 
  #' "best-candidate" selection procedure:
  #' \enumerate{
  #'   \item If exactly one candidate, take it.
  #'   \item If multiple candidates, prefer those whose ZIP matches the input ZIP.
  #'   \item If still ambiguous, use \code{find_similar_addresses()} to pick the
  #'         most similar candidate to the input address string.
  #'   \item If similarity logic does not resolve, fall back to the first candidate.
  #' }
  #'
  #' This is designed to validate and lock in a lon/lat for an address before you
  #' later assign decennial geographies via TIGER/Line shapefiles (point-in-polygon).
  #'
  #' @param street Character scalar. Street address line (e.g., \code{"55 Whitney Ave"}).
  #' @param city Character scalar. City name (e.g., \code{"New Haven"}).
  #' @param state Character scalar. State postal abbreviation (e.g., \code{"CT"}).
  #' @param zip Character scalar. ZIP code; may be 5-digit or ZIP+4
  #'   (e.g., \code{"06510"} or \code{"06510-1234"}).
  #' @param tries List. Each element is a named list with fields:
  #'   \describe{
  #'     \item{benchmark}{Numeric. Census Geocoder benchmark code. Must be
  #'       provided; \code{NULL} or blank will raise an error.}
  #'     \item{vintage}{Character or numeric. Either a vintage name string
  #'       (e.g., \code{"Census2020_Census2020"}) or a numeric vintage id.
  #'       Must be provided; \code{NULL} or blank will raise an error.}
  #'   }
  #'   Attempts are tried in order and the loop stops at the first attempt
  #'   yielding at least one address match.
  #' @param quiet Logical. If \code{FALSE} (default), prints a one-line summary
  #'   for each attempt showing benchmark, vintage, and whether it matched.
  #'
  #' @return A named list with components:
  #'   \describe{
  #'     \item{ok}{Logical. \code{TRUE} if any attempt produced a match.}
  #'     \item{best}{If \code{ok = TRUE}, a named list for the selected best
  #'       match containing: \code{benchmark}, \code{vintage_input},
  #'       \code{vintage_id}, \code{matched_address}, \code{lon}, \code{lat},
  #'       \code{geographies}, and \code{n_candidates}. \code{NULL} if no match.}
  #'     \item{parsed_response}{If \code{ok = TRUE}, the full parsed JSON
  #'       response for the successful attempt; \code{NULL} otherwise.}
  #'     \item{attempts}{A list of per-attempt metadata (index, benchmark,
  #'       vintage, ok, status, url) covering every attempt including skipped
  #'       and failed ones. Useful for debugging and auditing.}
  #'   }
  #'
  #' @section Error handling:
  #' \itemize{
  #'   \item \strong{Hard stops (programmer/config errors):} A \code{NULL} or
  #'     blank \code{benchmark} or \code{vintage} in any element of \code{tries}
  #'     raises an immediate \code{stop()} because these indicate a misconfigured
  #'     \code{tries} list that must be fixed before running.
  #'   \item \strong{Soft skips (runtime/network errors):} HTTP non-200 responses,
  #'     network failures, JSON parse errors, and vintage names not found for a
  #'     given benchmark are logged via \code{message()} and the attempt is
  #'     skipped. The reason is captured in \code{attempts[[i]]$status}.
  #'   \item \strong{API call errors:} If \code{call_census_geocoder()} itself
  #'     throws an error, it is caught via \code{tryCatch()} and logged as
  #'     \code{"error: <message>"} in \code{attempts[[i]]$status}.
  #' }
  #'
  #' @details
  #' This function depends on the following helpers being defined in your environment:
  #' \itemize{
  #'   \item \code{build_addr_geo_url()} — constructs the Census Geocoder request URL.
  #'   \item \code{call_census_geocoder()} — executes the HTTP request and parses JSON,
  #'     returning a list with \code{ok}, \code{status}, \code{url}, and \code{parsed}.
  #'   \item \code{select_best_match()} — selects the best candidate from parsed matches.
  #'   \item \code{find_similar_addresses()} — similarity matcher used inside
  #'     \code{select_best_match()} for ambiguous multi-candidate results.
  #'   \item \code{resolve_vintage_id()} — resolves vintage name strings to numeric ids.
  #'   \item The infix operators \code{\%||\%} (null coalescing) and \code{\%!in\%}.
  #' }
  
  # ---------------------------------------------------------------------------
  # Vintage id cache — populated lazily by resolve_vintage_id(), one entry per
  # unique benchmark. Avoids redundant calls to /geocoder/vintages when
  # multiple tries share the same benchmark.
  # ---------------------------------------------------------------------------
  vintage_cache <- new.env(parent = emptyenv())
  
  # ---------------------------------------------------------------------------
  # Helper: construct a skippable NA with a named reason attribute so the loop
  # can log a precise status rather than a generic failure label.
  # Defined here and used by resolve_vintage_id() via lexical scoping.
  # ---------------------------------------------------------------------------
  skip <- function(reason) {
    out <- NA_real_
    attr(out, "reason") <- reason
    out
  }
  
  # ---------------------------------------------------------------------------
  # Main loop — iterate through prioritized benchmark/vintage pairs.
  # Every attempt (including skipped/failed ones) is logged to all_attempts
  # for downstream QC and audit use.
  # ---------------------------------------------------------------------------
  all_attempts <- vector("list", length(tries))
  
  for (i in seq_along(tries)) {
    t      <- tries[[i]]
    bmk    <- t$benchmark
    vin_id <- resolve_vintage_id(bmk, t$vintage, vintage_cache)
    
    # -- Vintage resolution failed — log reason and move to next attempt -------
    if (is.na(vin_id)) {
      reason <- attr(vin_id, "reason") %||% "vintage_resolution_failed"
      
      all_attempts[[i]] <- list(
        i             = i,
        benchmark     = bmk,
        vintage_input = t$vintage,
        vintage_id    = NA_real_,
        ok            = FALSE,
        status        = reason,
        url           = NA_character_
      )
      if (!quiet) cat(sprintf(
        "Try %d: benchmark=%s, vintage=%s -> SKIPPED (%s)\n",
        i, bmk, as.character(t$vintage), reason
      ))
      next
    }
    
    # -- Build the geocoder URL and call the API -------------------------------
    url <- build_addr_geo_url(
      street = street, city = city, state = state, zip = zip,
      benchmark = bmk, vintage = vin_id
    )
    
    # Wrap in tryCatch so a runtime error in call_census_geocoder() is logged
    # as a failed attempt rather than propagating up and crashing the run.
    out <- tryCatch(
      call_census_geocoder(url),
      error = function(e) {
        list(ok = FALSE, status = paste0("error: ", conditionMessage(e)), url = url, parsed = NULL)
      }
    )
    
    # -- Log this attempt ------------------------------------------------------
    all_attempts[[i]] <- list(
      i             = i,
      benchmark     = bmk,
      vintage_input = t$vintage,
      vintage_id    = vin_id,
      ok            = out$ok,
      status        = out$status,
      url           = out$url
    )
    
    if (!quiet) cat(sprintf(
      "Try %d: benchmark=%s, vintage=%s (id=%s) -> %s\n",
      i, bmk, as.character(t$vintage), as.character(vin_id),
      ifelse(out$ok, "MATCH", "no match")
    ))
    
    # -- Match found — select best candidate and return ------------------------
    if (out$ok) {
      best_match <- select_best_match(out$parsed, street, city, state, zip)
      
      # select_best_match() returns NULL if it cannot resolve a candidate;
      # fall through to try the next benchmark/vintage pair.
      if (is.null(best_match)) next
      
      return(list(
        ok = TRUE,
        
        # Single best candidate chosen by select_best_match().
        best = list(
          benchmark       = bmk,
          vintage_input   = t$vintage,
          vintage_id      = vin_id,
          matched_address = best_match$matchedAddress %||% NA_character_,
          lon             = best_match$coordinates$x   %||% NA_real_,
          lat             = best_match$coordinates$y   %||% NA_real_,
          geographies     = best_match$geographies,
          n_candidates    = length(out$parsed$result$addressMatches)
        ),
        
        # Full raw API response for the successful attempt (diagnostics/audit).
        parsed_response = out$parsed,
        
        # Complete attempt log including all prior failed/skipped attempts.
        attempts = all_attempts
      ))
    }
  }
  
  # ---------------------------------------------------------------------------
  # All attempts exhausted with no match.
  # Return ok = FALSE with the full attempt log so callers can diagnose why.
  # ---------------------------------------------------------------------------
  list(ok = FALSE, best = NULL, parsed_response = NULL, attempts = all_attempts)
}




read_state_gpkgs_for_data <- function(data, out_root, geography = c("blocks", "block groups"), quiet = TRUE) {
  #' Read per-state TIGER GeoPackages (blocks or block groups) for the unique
  #' states present in a dataset.
  #'
  #' This helper finds the unique states represented in `data`, locates each
  #' state's output GeoPackage in `out_root`, and reads *all layers* from each
  #' GeoPackage.
  #'
  #' Input flexibility:
  #' - If `data` is a data.frame/tibble, it must contain either `state` (USPS, e.g. "AL")
  #'   or `statefp` (FIPS, e.g. "01" or 1).
  #' - If `data` is an atomic vector, it is treated as USPS state abbreviations.
  #'
  #' Output structure:
  #' - Returns a named list keyed by USPS state abbreviation.
  #' - Each state contains a named list of layers (each an `sf` object).
  #'
  #' @param data A data.frame/tibble with `state` or `statefp`, OR an atomic
  #'             vector of USPS abbreviations (e.g., `c("AL","GA")`).
  #' @param out_root Character path to the directory containing per-state GeoPackages.
  #' @param geography One of "blocks" or "block groups".
  #' @param quiet Logical; passed to $$sf::st_read()$$ to suppress per-layer messages.
  #'
  #' @return A nested list: `list(USPS = list(layer_name = sf_object, ...), ...)`.
  #' @examples
  #' \dontrun{
  #' blocks_by_state <- read_state_gpkgs_for_data(c("AL", "GA"), out_root, geography = "blocks")
  #' bg_by_state     <- read_state_gpkgs_for_data(c("AL", "GA"), out_root, geography = "block groups")
  #' }
  
  geography <- match.arg(geography)
  
  # --- Crosswalks -------------------------------------------------------------
  usps_to_fips <- c(
    "AL"="01","AK"="02","AZ"="04","AR"="05","CA"="06","CO"="08","CT"="09","DE"="10","DC"="11","FL"="12",
    "GA"="13","HI"="15","ID"="16","IL"="17","IN"="18","IA"="19","KS"="20","KY"="21","LA"="22","ME"="23",
    "MD"="24","MA"="25","MI"="26","MN"="27","MS"="28","MO"="29","MT"="30","NE"="31","NV"="32","NH"="33",
    "NJ"="34","NM"="35","NY"="36","NC"="37","ND"="38","OH"="39","OK"="40","OR"="41","PA"="42","RI"="44",
    "SC"="45","SD"="46","TN"="47","TX"="48","UT"="49","VT"="50","VA"="51","WA"="53","WV"="54","WI"="55",
    "WY"="56"
  )
  fips_to_usps <- setNames(names(usps_to_fips), unname(usps_to_fips))
  
  out_root_abs <- normalizePath(out_root, winslash = "/", mustWork = FALSE)
  
  # --- Determine which states to load ----------------------------------------
  if (is.atomic(data) && !is.data.frame(data)) {
    usps <- toupper(trimws(as.character(data)))
    usps <- unique(usps[!is.na(usps) & nzchar(usps)])
    
    fips <- unname(usps_to_fips[usps])
    bad <- is.na(fips)
    if (any(bad)) stop("Could not map USPS state(s) to FIPS: ", paste(usps[bad], collapse = ", "))
    
    state_map <- stats::setNames(fips, usps)
    
  } else {
    if ("state" %in% names(data)) {
      usps <- toupper(trimws(as.character(data$state)))
      usps <- unique(usps[!is.na(usps) & nzchar(usps)])
      
      fips <- unname(usps_to_fips[usps])
      bad <- is.na(fips)
      if (any(bad)) stop("Could not map USPS state(s) to FIPS: ", paste(usps[bad], collapse = ", "))
      
      state_map <- stats::setNames(fips, usps)
      
    } else if ("statefp" %in% names(data)) {
      fips <- sprintf("%02d", as.integer(as.character(data$statefp)))
      fips <- unique(fips[!is.na(fips)])
      
      usps <- unname(fips_to_usps[fips])
      bad <- is.na(usps)
      if (any(bad)) stop("Could not map statefp(s) to USPS: ", paste(fips[bad], collapse = ", "))
      
      state_map <- stats::setNames(fips, usps)
      
    } else {
      stop("`data` must contain either `statefp` or `state` (USPS).")
    }
  }
  
  if (length(state_map) == 0) stop("No states found in `data`.")
  
  # --- Choose filename prefix based on geography ------------------------------
  prefix <- if (geography == "blocks") "blocks" else "bg"
  
  # --- Read each state's GeoPackage (all layers) ------------------------------
  res <- purrr::imap(state_map, function(fips, usps) {
    
    gpkg_name <- paste0(prefix, "_statefp_", fips, "_2000_2010_2020.gpkg")
    gpkg_path <- file.path(out_root_abs, gpkg_name)
    
    if (!file.exists(gpkg_path)) {
      stop("Missing state GPKG: ", gpkg_name, " in ", out_root)
    }
    
    layer_names <- sf::st_layers(gpkg_path)$name
    
    purrr::map(
      stats::setNames(layer_names, layer_names),
      ~ sf::st_read(gpkg_path, layer = .x, quiet = quiet)
    )
  })
  
  res
}




add_decennial_geoid_block <- function(cand_sf,
                                      geos_by_state,
                                      geography = c("blocks", "block groups"),
                                      years = c(2000, 2010, 2020)) {
  #' Add decennial Census GEOIDs (2000/2010/2020) to candidate addresses for 
  #' either. Census blocks or block groups.
  #' - geography = "blocks": expects layers `blocks_2000`, `blocks_2010`, `blocks_2020`
  #'   with a `geoid_block` column.
  #' - geography = "block groups": expects layers `bg_2000`, `bg_2010`, `bg_2020`
  #'   with a `geoid` column.
  #'
  #' The join is performed state-by-state to keep joins small and CRS-consistent.
  #'
  #' @param cand_sf An sf POINT object with columns `row_id` and `state` (USPS).
  #' @param geos_by_state Nested list of layers keyed by USPS, as returned by
  #'        `read_state_gpkgs_for_data(..., geography = ...)`.
  #' @param geography One of "blocks" or "block groups".
  #' @param years Integer vector of decennial years to attach.
  #'
  #' @return A tibble/data.frame of attributes (geometry dropped) with `geoid_YYYY`
  #'         columns for the requested years (as available).
  
  geography <- match.arg(geography)
  
  # --- Preconditions / light validation --------------------------------------
  if (!inherits(cand_sf, "sf")) stop("`cand_sf` must be an sf object.")
  if (!all(c("row_id", "state") %in% names(cand_sf))) {
    stop("`cand_sf` must contain columns: row_id, state.")
  }
  
  # Layer prefix + GEOID column differ by geography
  layer_prefix <- if (geography == "blocks") "blocks" else "bg"
  geoid_col <- if (geography == "blocks") "geoid_block" else "geoid"
  
  # --- Organize work: outer loop by state ------------------------------------
  cand_split <- split(cand_sf, cand_sf$state)
  
  out_list <- vector("list", length(cand_split))
  names(out_list) <- names(cand_split)
  
  for (st in names(cand_split)) {
    
    pts_st <- cand_split[[st]]
    if (nrow(pts_st) == 0) next
    
    if (!st %in% names(geos_by_state)) {
      warning("No ", geography, " loaded for state: ", st)
      out_list[[st]] <- sf::st_drop_geometry(pts_st)
      next
    }
    
    base <- sf::st_drop_geometry(pts_st)
    
    for (yr in years) {
      
      layer_name <- paste0(layer_prefix, "_", yr)
      
      if (!layer_name %in% names(geos_by_state[[st]])) {
        warning("Missing layer ", layer_name, " for state ", st)
        next
      }
      
      polys_yr <- geos_by_state[[st]][[layer_name]]
      
      if (!geoid_col %in% names(polys_yr)) {
        warning("Layer ", layer_name, " for state ", st, " lacks column `", geoid_col, "`; skipping.")
        next
      }
      
      # Match CRS for spatial join
      pts_st_x <- sf::st_transform(pts_st, sf::st_crs(polys_yr))
      
      # Spatial join: point-in-polygon, keep all points (left=TRUE)
      j <- sf::st_join(
        pts_st_x,
        dplyr::select(polys_yr, dplyr::all_of(geoid_col)),
        join = sf::st_within,
        left = TRUE
      ) |>
        sf::st_drop_geometry() |>
        dplyr::transmute(
          row_id = .data$row_id,
          !!paste0("geoid_", yr) := .data[[geoid_col]]
        )
      
      base <- dplyr::left_join(base, j, by = "row_id")
    }
    
    out_list[[st]] <- base
  }
  
  dplyr::bind_rows(out_list)
}




decode_zcta <- function(cand_sf,
                        zcta_sf,
                        zcta_colname = "zcta",
                        state_col    = "state") {
  #' Decode (Assign) ZCTA Codes to Point Locations. Given a set of candidate 
  #' point locations and a ZCTA polygon layer (e.g. 2000, 2010, or 2020 vintage), 
  #' this function performs a point-in-polygon spatial join and returns the ZCTA 
  #' code for each point.
  #'
  #' @param cand_sf      An `sf` object with POINT geometry. Must contain a
  #'                     unique integer or character column named `row_id`.
  #' @param zcta_sf      An `sf` object with (MULTI)POLYGON geometry representing
  #'                     ZCTA boundaries. Must contain:
  #'                     \describe{
  #'                       \item{area_code}{ZCTA identifier, typically a 5-digit
  #'                         string that may include leading zeros.}
  #'                       \item{area_states}{(Optional) Hyphen-delimited string of
  #'                         state identifiers that the ZCTA touches (e.g.
  #'                         `"-CA-NV-"`). Used for state-based pre-filtering when
  #'                         `state_col` is supplied.}
  #'                     }
  #' @param zcta_colname Character scalar. Name given to the ZCTA output column in
  #'                     the returned data frame. Defaults to `"zcta"`. Typically
  #'                     set to a vintage-specific name such as `"zcta_2010"`.
  #' @param state_col    Character scalar or `NULL`. Name of a column in `cand_sf`
  #'                     holding state identifiers (e.g. FIPS codes or postal
  #'                     abbreviations). When supplied *and* `zcta_sf` contains an
  #'                     `area_states` column, ZCTA polygons are pre-filtered to
  #'                     only those whose `area_states` overlaps the set of states
  #'                     present in `cand_sf` — significantly reducing the polygon
  #'                     set before the expensive spatial join. Defaults to
  #'                     `"state"`. Set to `NULL` to skip filtering entirely.
  #'
  #' @return A data frame with one row per point in `cand_sf` and two columns:
  #' \describe{
  #'   \item{row_id}{Candidate identifier copied from `cand_sf$row_id`.}
  #'   \item{<zcta_colname>}{ZCTA code (character) from `zcta_sf$area_code`, or
  #'     `NA` if the point did not fall within any ZCTA polygon.}
  #' }
  #'
  #' @details
  #' **Performance:** The state-based pre-filter (`state_col` + `area_states`) is
  #' applied to the raw `zcta_sf` object *before* any CRS transformation, column
  #' subsetting, or spatial join. This means all subsequent operations work on a
  #' much smaller polygon set, which is the primary speed lever for large national
  #' ZCTA layers.
  #'
  #' **CRS:** Points are re-projected into the CRS of `zcta_sf` before the join.
  #' The original `cand_sf` object is not modified.
  #'
  #' **Duplicate matches:** `largest = TRUE` in `sf::st_join` ensures at most one
  #' polygon match is retained per point. Duplicate matches are rare but can occur
  #' at polygon boundaries or with invalid geometries.
  #'
  #' **Geometry warning suppression:** The common sf warning about attributes being
  #' "spatially constant throughout all geometries" is suppressed; it is benign for
  #' ZCTA layers because `area_code` is constant per feature.
  #'
  #' @seealso [decode_cbsa_csa()] for CBSA/CSA assignment using the same pattern.
  #'
  #' @examples
  #' \dontrun{
  #'   # Assign 2010 ZCTAs; cand_sf must have columns row_id and state
  #'   z2010 <- decode_zcta(cand_sf, core_areas$zcta_2010,
  #'                        zcta_colname = "zcta_2010",
  #'                        state_col    = "state")
  #'
  #'   # Merge result back onto the original point table
  #'   out <- dplyr::left_join(sf::st_drop_geometry(cand_sf), z2010, by = "row_id")
  #'
  #'   # Skip state filtering (e.g. polygon layer has no area_states column)
  #'   z2000 <- decode_zcta(cand_sf, core_areas$zcta_2000,
  #'                        zcta_colname = "zcta_2000",
  #'                        state_col    = NULL)
  #' }
  
  # ---- Input checks ----
  # Verify spatial object types
  if (!inherits(cand_sf, "sf"))  stop("`cand_sf` must be an sf object.")
  if (!inherits(zcta_sf, "sf"))  stop("`zcta_sf` must be an sf object.")
  # Verify required columns exist
  if (!("row_id" %in% names(cand_sf)))    stop("`cand_sf` must contain column: row_id.")
  if (!("area_code" %in% names(zcta_sf))) stop("`zcta_sf` must contain column: area_code.")
  # Verify output column name is a non-empty string
  if (!is.character(zcta_colname) || length(zcta_colname) != 1L || nchar(zcta_colname) == 0L) {
    stop("`zcta_colname` must be a non-empty character scalar.")
  }
  # Verify state_col exists in cand_sf if supplied
  if (!is.null(state_col) && !(state_col %in% names(cand_sf))) {
    stop("`state_col` was provided but is not a column in `cand_sf`: ", state_col)
  }
  
  # ---- State-based pre-filter on zcta_sf (before any CRS work) ----
  # Reducing the polygon set here is the primary performance optimisation: all
  # subsequent steps (CRS transform, column subset, spatial join) then operate
  # on a much smaller layer. Pre-filtering is only attempted when:
  #   (a) state_col was supplied,
  #   (b) zcta_sf carries an area_states column, and
  #   (c) at least one area_states value is non-NA.
  if (!is.null(state_col) &&
      "area_states" %in% names(zcta_sf) &&
      any(!is.na(zcta_sf$area_states))) {
    
    # Derive the unique set of states present in the candidate points
    candidate_states <- unique(as.character(cand_sf[[state_col]]))
    candidate_states <- candidate_states[!is.na(candidate_states)]
    
    if (length(candidate_states) > 0L) {
      ps <- as.character(zcta_sf$area_states)
      
      # Use hyphen sentinels (-STATE-) to avoid partial substring matches
      # (e.g. "AL" matching inside "CAL"). Build a single alternation pattern
      # covering all candidate states for efficiency.
      pattern <- paste0("-(", paste(candidate_states, collapse = "|"), ")-")
      keep    <- is.na(ps) | stringr::str_detect(paste0("-", ps, "-"), pattern)
      
      # Overwrite zcta_sf in place so all downstream code uses the reduced layer
      zcta_sf <- zcta_sf[keep, , drop = FALSE]
    }
  }
  
  # ---- CRS alignment ----
  # Re-project candidate points into the native CRS of the (now-filtered)
  # ZCTA layer. The original cand_sf object is not modified.
  pts <- sf::st_transform(cand_sf, sf::st_crs(zcta_sf))
  
  # ---- Build polygon subset for the join ----
  # Retain only the columns needed: area_code (required) and area_states
  # (carried forward if present, though only area_code enters the join below).
  keep_cols <- intersect(c("area_code", "area_states"), names(zcta_sf))
  poly <- zcta_sf[, keep_cols, drop = FALSE]
  
  # ---- Point-in-polygon join ----
  # st_within assigns a point to a polygon only when it lies strictly inside.
  # largest = TRUE resolves the rare case where a point matches multiple
  # polygons (boundary ambiguity) by keeping the largest-area match.
  suppressWarnings({
    joined <- sf::st_join(
      pts,
      poly[, "area_code", drop = FALSE], # pass only area_code into the join
      join    = sf::st_within,
      left    = TRUE,                    # keep unmatched points as NA
      largest = TRUE
    )
  })
  
  # ---- Return geometry-free result ----
  # Drop spatial geometry and retain only the two output columns.
  out <- sf::st_drop_geometry(joined)[, c("row_id", "area_code")]
  
  # Rename area_code to the caller-specified column name and coerce to character
  # to preserve leading zeros in 5-digit ZCTA codes.
  names(out)[2]       <- zcta_colname
  out[[zcta_colname]] <- as.character(out[[zcta_colname]])
  
  out
}




decode_cbsa_csa <- function(cand_sf,
                            cbsa_csa_sf,
                            year,
                            state_col = "state") {
  #' Decode (Assign) CBSA and CSA Codes to Point Locations.
  #' Given a set of candidate point locations and a combined CBSA/CSA polygon
  #' layer, this function performs two point-in-polygon spatial joins—one for
  #' Core Based Statistical Areas (CBSAs) and one for Combined Statistical Areas
  #' (CSAs)—and returns the corresponding codes and CBSA
  #' metropolitan/micropolitan level for each point.
  #'
  #' @param cand_sf     An `sf` object with POINT geometry. Must contain a unique
  #'                    integer or character column named `row_id`.
  #' @param cbsa_csa_sf An `sf` object with (MULTI)POLYGON geometry representing
  #'                    CBSA and CSA boundaries. Must contain:
  #'                    \describe{
  #'                      \item{area_type}{Character; either `"cbsa"` or `"csa"`
  #'                        (case-insensitive) identifying the polygon type.}
  #'                      \item{area_code}{Character or numeric area identifier
  #'                        (e.g. 5-digit CBSA or CSA FIPS code).}
  #'                      \item{area_level}{Character; CBSA classification, typically
  #'                        `"Metropolitan"` or `"Micropolitan"`. Not used for CSA
  #'                        rows but must be present in the layer.}
  #'                      \item{area_states}{(Optional) Hyphen-delimited string of
  #'                        state identifiers that the polygon touches (e.g.
  #'                        `"-OH-KY-IN-"`). Used for state-based pre-filtering
  #'                        and post-join validation when `state_col` is supplied.}
  #'                    }
  #' @param year        Single numeric value (e.g. `2015`). Appended as a suffix to
  #'                    the output column names: `cbsa_code_<year>`,
  #'                    `cbsa_level_<year>`, and `csa_code_<year>`.
  #' @param state_col   Character scalar or `NULL`. Name of a column in `cand_sf`
  #'                    holding state identifiers (e.g. FIPS codes or postal
  #'                    abbreviations).
  #'
  #'                    When supplied *and* `cbsa_csa_sf` contains an `area_states`
  #'                    column, polygons are pre-filtered to only those whose
  #'                    `area_states` overlaps the states present in `cand_sf`
  #'                    before any CRS or spatial work is performed—this is the
  #'                    primary speed optimisation for large national layers.
  #'
  #'                    The same `state_col` is also used for a post-join
  #'                    state-consistency check (see Details).
  #'
  #'                    Defaults to `NULL` (no state-based pre-filtering or
  #'                    post-join validation).
  #'
  #' @return A data frame with one row per point in `cand_sf` and four columns:
  #' \describe{
  #'   \item{row_id}{Candidate identifier copied from `cand_sf$row_id`.}
  #'   \item{cbsa_code_<year>}{CBSA code (character) or `NA` if no match.}
  #'   \item{cbsa_level_<year>}{CBSA level (character, e.g. `"Metropolitan"`) or
  #'     `NA` if no match.}
  #'   \item{csa_code_<year>}{CSA code (character) or `NA` if no match.}
  #' }
  #'
  #' @details
  #' **Performance:** The state-based pre-filter (`state_col` + `area_states`) is
  #' applied to the raw `cbsa_csa_sf` object *before* any CRS transformation,
  #' polygon splitting, or spatial join. All subsequent operations therefore work
  #' on a much smaller polygon set, which is the primary speed lever for large
  #' national CBSA/CSA layers.
  #'
  #' **Two-pass joining:** After the pre-filter, the polygon layer is split into
  #' CBSA and CSA subsets and each is joined independently. This allows CBSA-only
  #' attributes (`area_level`) to be handled cleanly without polluting the CSA
  #' result.
  #'
  #' **Post-join state masking (row-preserving):** After each join, a secondary,
  #' row-level state-consistency check is applied via an internal `mask_by_state()`
  #' helper. If the matched polygon’s `area_states` does not include the point’s
  #' state, the polygon-derived attributes (e.g. `area_code`, `area_level`) are
  #' set to `NA` for that row. Importantly, rows are *not removed*, preserving the
  #' invariant of one output row per input point and avoiding dimension mismatches
  #' when combining CBSA and CSA results.
  #'
  #' **CRS:** Points are re-projected into the native CRS of the (now-filtered)
  #' `cbsa_csa_sf` layer. The original `cand_sf` object is not modified.
  #'
  #' **Duplicate matches:** `largest = TRUE` in `sf::st_join` is used to prefer a
  #' single polygon when a point matches more than one feature (rare boundary
  #' ambiguities). Where an sf backend does not enforce a single match, downstream
  #' code should ensure one record per `row_id`.
  #'
  #' **Geometry warning suppression:** The common sf warning about attributes being
  #' "spatially constant throughout all geometries" is suppressed; it is benign
  #' here because `area_code` and `area_level` are constant per feature.
  #'
  #' @seealso [decode_zcta()] for ZCTA assignment using the same pattern.
  #'
  #' @examples
  #' \dontrun{
  #'   # Assign 2015 CBSA and CSA codes; cand_sf must have columns row_id and state
  #'   cbsa_csa_2015 <- decode_cbsa_csa(cand_sf, core_areas$cbsa_csa_2015,
  #'                                    year      = 2015,
  #'                                    state_col = "state")
  #'
  #'   # Merge result back onto the original point table
  #'   out <- dplyr::left_join(sf::st_drop_geometry(cand_sf), cbsa_csa_2015,
  #'                           by = "row_id")
  #'
  #'   # Skip state filtering/validation entirely
  #'   cbsa_csa_2010 <- decode_cbsa_csa(cand_sf, core_areas$cbsa_csa_2010,
  #'                                    year      = 2010,
  #'                                    state_col = NULL)
  #' }
  
  # ---- Input checks ----
  # Verify spatial object types
  if (!inherits(cand_sf, "sf"))     stop("`cand_sf` must be an sf object.")
  if (!inherits(cbsa_csa_sf, "sf")) stop("`cbsa_csa_sf` must be an sf object.")
  # Verify required point column
  if (!("row_id" %in% names(cand_sf))) stop("`cand_sf` must contain column: row_id.")
  # Verify year is a single number (used as an output column name suffix)
  if (!is.numeric(year) || length(year) != 1L) stop("`year` must be a single number (e.g., 2007).")
  # Verify all required polygon columns are present
  need <- c("area_type", "area_code", "area_level")
  miss <- setdiff(need, names(cbsa_csa_sf))
  if (length(miss) > 0) stop("`cbsa_csa_sf` is missing columns: ", paste(miss, collapse = ", "))
  # Verify state_col exists in cand_sf if supplied
  if (!is.null(state_col) && !(state_col %in% names(cand_sf))) {
    stop("`state_col` was provided but is not a column in `cand_sf`: ", state_col)
  }
  
  # ---- State-based pre-filter on cbsa_csa_sf (before any CRS work) ----
  # Reducing the polygon set here is the primary performance optimisation: all
  # subsequent steps (CRS transform, polygon split, spatial joins) then operate
  # on a much smaller layer. Pre-filtering is only attempted when:
  #   (a) state_col was supplied,
  #   (b) cbsa_csa_sf carries an area_states column, and
  #   (c) at least one area_states value is non-NA.
  if (!is.null(state_col) &&
      "area_states" %in% names(cbsa_csa_sf) &&
      any(!is.na(cbsa_csa_sf$area_states))) {
    
    # Derive the unique set of states present in the candidate points
    candidate_states <- unique(as.character(cand_sf[[state_col]]))
    candidate_states <- candidate_states[!is.na(candidate_states)]
    
    if (length(candidate_states) > 0L) {
      ps <- as.character(cbsa_csa_sf$area_states)
      
      # Use hyphen sentinels (-STATE-) to avoid partial substring matches
      # (e.g. "AL" matching inside "CAL"). Build a single alternation pattern
      # covering all candidate states for efficiency.
      pattern <- paste0("-(", paste(candidate_states, collapse = "|"), ")-")
      keep    <- is.na(ps) | stringr::str_detect(paste0("-", ps, "-"), pattern)
      
      # Overwrite cbsa_csa_sf in place so all downstream code uses the reduced layer
      cbsa_csa_sf <- cbsa_csa_sf[keep, , drop = FALSE]
    }
  }
  
  # ---- CRS alignment ----
  # Re-project candidate points into the native CRS of the (now-filtered)
  # polygon layer. The original cand_sf object is not modified.
  pts <- sf::st_transform(cand_sf, sf::st_crs(cbsa_csa_sf))
  
  # ---- Split polygons into CBSA vs CSA ----
  # Normalise area_type to lowercase for a case-insensitive comparison, then
  # subset into two separate layers. area_states is carried forward when present
  # so the post-join state filter (below) can operate on both subsets.
  type_chr <- tolower(as.character(cbsa_csa_sf$area_type))
  cbsa_sf  <- cbsa_csa_sf[type_chr == "cbsa", c("area_code", "area_level",
                                                intersect("area_states", names(cbsa_csa_sf))),
                          drop = FALSE]
  csa_sf   <- cbsa_csa_sf[type_chr == "csa",  c("area_code",
                                                intersect("area_states", names(cbsa_csa_sf))),
                          drop = FALSE]
  
  # ---- Post-join state masking helper ----
  # Applied after each spatial join as a secondary guard against rare cross-state
  # matches near borders.
  #
  # What it does (and why):
  # - It DOES NOT drop rows. Dropping rows can break the “one output row per input
  #   point” invariant and cause downstream dimension mismatches (e.g., CBSA join
  #   keeps 1 row but CSA join keeps 2).
  # - Instead, for any row where the matched polygon’s `area_states` does not
  #   include the point’s state, it sets selected polygon-derived attributes to NA.
  #   This preserves row counts and alignment while still preventing incorrect
  #   assignments.
  #
  # How state matching works:
  # - `area_states` is expected to be a hyphen-delimited sentinel string like
  #   "-TX-AR-" (or similar). We wrap both sides in "-" and search for the exact
  #   token "-ST-" to avoid partial substring matches (e.g., "AL" inside "CAL").
  # - Polygons with NA `area_states` are treated as “no metadata to validate” and
  #   are left unchanged (i.e., not masked).
  mask_by_state <- function(joined_df, poly_states_col, point_states, cols_to_na) {
    ps <- as.character(joined_df[[poly_states_col]])
    pt <- as.character(point_states)
    
    # Rowwise pattern "-ST-" for each point (vector length = nrow(joined_df))
    pat <- paste0("-", pt, "-")
    
    # Keep matches when polygon has no state metadata (NA), otherwise require token match
    ok <- is.na(ps) | stringr::str_detect(paste0("-", ps, "-"), pat)
    
    # For rows failing the check, blank out polygon-derived fields
    for (cc in cols_to_na) {
      if (cc %in% names(joined_df)) joined_df[[cc]][!ok] <- NA
    }
    
    joined_df
  }
  
  # ---- Join to CBSA polygons ----
  # st_within assigns a point to a polygon only when it lies strictly inside.
  # largest = TRUE resolves the rare case where a point matches multiple
  # polygons (boundary ambiguity) by keeping the largest-area match.
  suppressWarnings({
    cbsa_joined <- sf::st_join(
      pts,
      cbsa_sf,
      join    = sf::st_within,
      left    = TRUE,   # keep unmatched points as NA
      largest = TRUE
    )
  })
  cbsa_df <- sf::st_drop_geometry(cbsa_joined)
  
  # Apply post-join state filter to CBSA results if conditions are met
  if (!is.null(state_col) && ("area_states" %in% names(cbsa_df)) && any(!is.na(cbsa_df$area_states))) {
    cbsa_df <- mask_by_state(
      cbsa_df,
      poly_states_col = "area_states",
      point_states    = cbsa_df[[state_col]],
      cols_to_na      = c("area_code", "area_level")
    )
  }
  
  # ---- Join to CSA polygons ----
  # Identical join strategy as CBSA above; CSA polygons do not carry area_level
  # so that column is intentionally absent from csa_sf and the output.
  suppressWarnings({
    csa_joined <- sf::st_join(
      pts,
      csa_sf,
      join    = sf::st_within,
      left    = TRUE,
      largest = TRUE
    )
  })
  csa_df <- sf::st_drop_geometry(csa_joined)
  
  # Apply post-join state filter to CSA results if conditions are met
  if (!is.null(state_col) && ("area_states" %in% names(csa_df)) && any(!is.na(csa_df$area_states))) {
    csa_df <- mask_by_state(
      csa_df,
      poly_states_col = "area_states",
      point_states    = csa_df[[state_col]],
      cols_to_na      = c("area_code")
    )
  }
  
  # ---- Assemble output ----
  # Build a plain data frame (no geometry) with year-suffixed column names so
  # multiple vintages can be safely column-bound onto the same points table.
  out <- data.frame(
    row_id = cbsa_df$row_id,
    stringsAsFactors = FALSE
  )
  
  out[[paste0("cbsa_code_",  year)]] <- as.character(cbsa_df$area_code)
  out[[paste0("cbsa_level_", year)]] <- as.character(cbsa_df$area_level)
  out[[paste0("csa_code_",   year)]] <- as.character(csa_df$area_code)
  
  out
}




format_year_ranges <- function(years) {
  #' Format a set of years into compact consecutive ranges (e.g., "2001:2003, 2006").
  #' Takes a vector of years (possibly unsorted and with duplicates) and returns a
  #' human-readable string where consecutive years are collapsed into "start:end"
  #' ranges and separated by ", ".
  #'
  #' @param years A numeric/integer vector of years (e.g., c(2001, 2002, 2004)).
  #'
  #' @return A single character string of formatted year ranges.
  
  # Sort years and remove duplicates so we can detect consecutive runs reliably.
  years <- sort(unique(years))
  
  # Identify boundaries between runs:
  # diff(years) > 1 indicates a gap (e.g., 2003 -> 2006), which breaks a consecutive run.
  # We store break indices in a way that makes slicing easy in the next step.
  breaks <- c(0L, which(diff(years) > 1L), length(years))
  
  # Convert each run into either:
  # - a single year (e.g., "2006"), or
  # - a "start:end" range (e.g., "2001:2003").
  runs <- mapply(
    function(start, end) {
      run <- years[(start + 1L):end]
      if (length(run) == 1L) {
        as.character(run)
      } else {
        paste0(run[1L], ":", run[length(run)])
      }
    },
    breaks[-length(breaks)],
    breaks[-1L],
    SIMPLIFY = TRUE
  )
  
  # Join multiple runs into a single comma-separated string.
  paste(runs, collapse = ", ")
}




parse_years <- function(avp) {
  #' Parse a formatted year-range string into an integer vector of individual years
  #'
  #' @param avp A single character string produced by `format_year_ranges()`,
  #'   e.g. `"2000:2023"`, `"2025"`, or `"2024:2025"`. Comma-separated segments
  #'   are supported (e.g. `"2000:2009, 2015"`).
  
  unlist(lapply(
    strsplit(avp, ",\\s*")[[1]],
    function(rng) {
      # Split on ":" to detect a range (two elements) vs. a single year (one element)
      parts <- as.integer(strsplit(trimws(rng), ":")[[1]])
      if (length(parts) == 2L) parts[1]:parts[2] else parts
    }
  ))
}




expected_vintages <- function(years) {
  #' Map a vector of years to their corresponding decennial census vintage labels.
  #' Uses the standard decennial period boundaries:
  #'   2000 -> 2000-2009, 2010 -> 2010-2019, 2020 -> 2020-2029
  #'
  #' @param years An integer vector of years, typically produced by `parse_years()`.
  #' @return A unique integer vector of decennial vintage labels (e.g. `c(2000L, 2010L)`).
  #'   Years outside all defined periods return `NA` from `fcase()` and are silently dropped
  #'   by `unique()`.
  
  unique(fcase(
    years >= 2000L & years <= 2009L, 2000L,
    years >= 2010L & years <= 2019L, 2010L,
    years >= 2020L & years <= 2029L, 2020L
  ))
}




parse_vintages <- function(v) {
  #' Parse a comma-separated vintage string into an integer vector
  #'
  #' @param v A character string of vintage years, e.g. `"2000, 2010"`. Handles
  #'   the sentinel values `NA` and `"None"` by returning an empty integer vector
  #'   so downstream `%in%` checks fail gracefully rather than erroring.
  #' @return An integer vector of vintage years, or `integer(0)` if `v` is `NA`
  #'   or `"None"`.
  
  if (is.na(v) || v == "None") return(integer(0))
  as.integer(strsplit(trimws(v), ",\\s*")[[1]])
}




check_alignment <- function(avp, vintages_str) {
  #' Check whether a boundary's recorded vintages cover all decennial periods
  #' implied by the archive year range.
  #'
  #' Intended for use with `mapply()` over rows of a summarised data.table where
  #' `archive_versions_present` has already been formatted by `format_year_ranges()`.
  #'
  #' @param avp A single character string of formatted archive years
  #'   (e.g. `"2000:2023"`), passed to `parse_years()`.
  #' @param vintages_str A single character string of recorded vintage years
  #'   (e.g. `"2000, 2010"`), passed to `parse_vintages()`. `NA` or `"None"`
  #'   returns `NA`.
  #' @return `TRUE` if every expected decennial vintage is present in
  #'   `vintages_str`; `FALSE` if one or more are missing; `NA` if no expected
  #'   vintages could be derived (e.g. years outside all defined periods).
  
  yrs <- parse_years(avp)
  exp <- expected_vintages(yrs)
  rec <- parse_vintages(vintages_str)
  # No mappable vintages -> alignment is indeterminate
  if (length(exp) == 0L) return(NA)
  all(exp %in% rec)
}




check_alignment_cbsa <- function(avp, vintages_str) {
  #' Check whether a CBSA/CSA boundary's recorded vintages cover all periods
  #' implied by the archive year range.
  #'
  #' Mirrors `check_alignment()` but resolves archive years against
  #' `cbsa_vintage_map` so the 2000-2009 period maps to the `2007` vintage label
  #' rather than `2000`.
  #'
  #' @param avp A single character string of formatted archive years
  #'   (e.g. `"2000:2023"`), parsed identically to `check_alignment()`.
  #' @param vintages_str A single character string of recorded CBSA/CSA vintage
  #'   years (e.g. `"2007, 2010"`). `NA` or `"None"` returns `NA`.
  #' @return `TRUE` if every expected CBSA vintage is present in `vintages_str`;
  #'   `FALSE` if one or more are missing; `NA` if `vintages_str` is `NA`/`"None"`
  #'   or no expected vintages could be derived.
  
  if (is.na(vintages_str) || vintages_str == "None") return(NA)
  years <- unlist(lapply(
    strsplit(avp, ",\\s*")[[1]],
    function(rng) {
      # Split on ":" to detect a range (two elements) vs. a single year (one element)
      parts <- as.integer(strsplit(trimws(rng), ":")[[1]])
      if (length(parts) == 2L) parts[1]:parts[2] else parts
    }
  ))
  # Join years against cbsa_vintage_map using a non-equi join on the period boundaries
  expected <- unique(cbsa_vintage_map[
    data.table(year = years),
    on = .(year_start <= year, year_end >= year),
    vintage
  ])
  recorded <- as.integer(strsplit(trimws(vintages_str), ",\\s*")[[1]])
  # No mappable vintages -> alignment is indeterminate
  if (length(expected) == 0L) return(NA)
  all(expected %in% recorded)
}




write_list_to_xlsx <- function(lst, path = "output.xlsx") {
  #' Write a named list of tables to a multi-sheet Excel workbook (.xlsx). Takes a 
  #' list where each element is a data.frame/tibble/data.table and writes each 
  #' element to its own worksheet in an Excel file. List names are used as sheet 
  #' names; unnamed/blank elements are assigned default names.
  #'
  #' @param lst A list of tabular objects (data.frame, tibble, or data.table).
  #'   Each list element becomes one worksheet.
  #' @param path Output file path for the Excel workbook. Defaults to "output.xlsx".
  #'
  #' @return Invisibly returns $$\texttt{TRUE}$$ on success; called for its side effect
  #'   (writing an $$\texttt{.xlsx}$$ file to disk).
  
  # Ensure the required package is available without attaching it to the search path.
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("Please install openxlsx: install.packages('openxlsx')")
  }
  
  # Ensure every list element has a usable worksheet name.
  # If names are missing or blank, generate "sheet_1", "sheet_2", ...
  if (is.null(names(lst)) || any(names(lst) == "")) {
    names(lst) <- paste0("sheet_", seq_along(lst))
  }
  
  # Create a new in-memory workbook.
  wb <- openxlsx::createWorkbook()
  
  # Add one worksheet per list element and write the corresponding table.
  for (nm in names(lst)) {
    openxlsx::addWorksheet(wb, nm)
    openxlsx::writeData(wb, nm, lst[[nm]])
  }
  
  # Save workbook to disk; overwrite existing file at 'path' if present.
  openxlsx::saveWorkbook(wb, path, overwrite = TRUE)
  
  invisible(TRUE)
}




write_list_to_duckdb <- function(lst,
                                 path,                      # analogous to xlsx path
                                 table_names = names(lst),  # optional override
                                 overwrite = TRUE,
                                 check_home_writable = TRUE,
                                 use_home_cache_if_writable = TRUE) {
  #' Write a list of tables to a single DuckDB database file. A lightweight 
  #' replacement for writing a multi-sheet Excel workbook. Each element of `lst` 
  #' is written as its own DuckDB table (analogous to an XLSX sheet) inside one 
  #' `.duckdb` file.
  #'
  #' This workflow does not require DuckDB extensions (it uses built-in DuckDB
  #' functionality). Optionally, the function can verify that the user's home
  #' directory is writable and, if so, set DuckDB's storage home there to provide
  #' a stable location for extension caching *if extensions are ever used*.
  #'
  #' @param lst A list of tabular objects, typically `data.frame`/`tibble`.
  #'   Each list element becomes one DuckDB table.
  #' @param path File path to the DuckDB database file to create/overwrite, e.g.
  #'   `"./results/qc_geo.duckdb"`.
  #' @param table_names Character vector of table names to use. Defaults to
  #'   `names(lst)`. If missing/blank, names are auto-generated as
  #'   `"sheet_1"`, `"sheet_2"`, ...
  #' @param overwrite Logical; passed to `DBI::dbWriteTable()`. If `TRUE`, tables
  #'   with the same name are replaced.
  #' @param check_home_writable Logical; if `TRUE`, attempts to create and delete a
  #'   small temp file under `~` to confirm the home directory is writable in the
  #'   current environment (useful on HPC compute nodes).
  #' @param use_home_cache_if_writable Logical; if `TRUE` and the home directory is
  #'   writable, sets `options(duckdb.storage.home = "~")`. This does not install
  #'   or load any extensions; it only chooses a stable cache location.
  #'
  #' @return Invisibly returns `TRUE` on success.
  
  # Basic validation ---------------------------------------------------------
  stopifnot(is.list(lst), length(lst) > 0)
  
  # Table names (like sheet names) ------------------------------------------
  # Prefer list names; otherwise generate sheet_1, sheet_2, ...
  if (is.null(table_names) || any(table_names == "")) {
    table_names <- paste0("sheet_", seq_along(lst))
  }
  stopifnot(length(table_names) == length(lst))
  
  # Ensure output directory exists ------------------------------------------
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  # Optional: confirm HOME is writable (useful on HPC) -----------------------
  # If writable, set DuckDB's storage home so any future extension caching
  # (if you ever use extensions) goes somewhere stable and user-writable.
  if (check_home_writable) {
    home <- path.expand("~")
    test_file <- file.path(home, paste0("duckdb_home_write_test_", Sys.getpid(), ".txt"))
    
    home_writable <- tryCatch({
      writeLines("test", test_file)
      unlink(test_file)
      TRUE
    }, error = function(e) FALSE)
    
    message("HOME: ", home)
    message("HOME writable: ", home_writable)
    
    if (home_writable && use_home_cache_if_writable) {
      options(duckdb.storage.home = home)
      message("Set options(duckdb.storage.home = \"", home, "\")")
    }
  }
  
  # Write tables into a single DuckDB file ----------------------------------
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  for (i in seq_along(lst)) {
    DBI::dbWriteTable(con, table_names[[i]], lst[[i]], overwrite = overwrite)
  }
  
  invisible(TRUE)
}




read_list_from_duckdb <- function(path,
                                  tables = NULL,            # NULL = read all tables
                                  check_home_writable = TRUE,
                                  use_home_cache_if_writable = TRUE) {
  #' Read tables from a DuckDB database file into a named list.
  #' Companion to write_list_to_duckdb(): each DuckDB table is returned as one
  #' list element (analogous to reading sheets from an XLSX workbook).
  #'
  #' @param path File path to the DuckDB database file, e.g. "./results/qc_geo.duckdb".
  #' @param tables Character vector of table names to read. If NULL, reads all tables.
  #' @param check_home_writable Logical; same intent as in write_list_to_duckdb().
  #' @param use_home_cache_if_writable Logical; same intent as in write_list_to_duckdb().
  #'
  #' @return A named list of data.frames (one per table).
  
  # Optional: confirm HOME is writable (useful on HPC) -----------------------
  if (check_home_writable) {
    home <- path.expand("~")
    test_file <- file.path(home, paste0("duckdb_home_write_test_", Sys.getpid(), ".txt"))
    
    home_writable <- tryCatch({
      writeLines("test", test_file)
      unlink(test_file)
      TRUE
    }, error = function(e) FALSE)
    
    message("HOME: ", home)
    message("HOME writable: ", home_writable)
    
    if (home_writable && use_home_cache_if_writable) {
      options(duckdb.storage.home = home)
      message("Set options(duckdb.storage.home = \"", home, "\")")
    }
  }
  
  # Connect read-only and pull tables ---------------------------------------
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  available <- DBI::dbListTables(con)
  
  if (is.null(tables)) {
    tables <- available
  } else {
    missing <- setdiff(tables, available)
    if (length(missing) > 0) {
      stop("These tables were not found in the DuckDB file: ",
           paste(missing, collapse = ", "))
    }
  }
  
  out <- setNames(vector("list", length(tables)), tables)
  for (nm in tables) {
    out[[nm]] <- DBI::dbReadTable(con, nm)
  }
  out
}




make_ranges <- function(dt, abi_col = "abi", chunk_size = 1000L) {
  #' Chunk by unique ABI (NOT row ranges). Returns a data.table with one row per 
  #' chunk:
  #' - start_abi/end_abi: positions in the unique-ABI vector
  #' - label: human-readable label
  #' - abi_list: list-column containing the ABI values in that chunk
  #'
  #' @param dt data.table/data.frame
  #' @param abi_col ABI column name
  #' @param chunk_size number of ABIs per chunk (defaults 1000)
  #'
  #' @return data.table with start_abi, end_abi, label, abi_list
  
  if (!abi_col %in% names(dt)) {
    stop(sprintf("make_ranges(): column '%s' not found in dt.", abi_col))
  }
  
  chunk_size <- as.integer(chunk_size)
  if (is.na(chunk_size) || chunk_size <= 0L) stop("chunk_size must be a positive integer.")
  
  # Pull unique ABIs (keeps first-seen order; low memory footprint)
  abi_u <- unique(data.table::as.data.table(dt)[[abi_col]])
  m <- length(abi_u)
  
  if (m == 0L) {
    return(data.table::data.table(
      start_abi = integer(),
      end_abi   = integer(),
      label     = character(),
      abi_list  = I(list())
    ))
  }
  
  starts <- seq.int(1L, m, by = chunk_size)
  ends   <- pmin.int(starts + chunk_size - 1L, m)
  
  data.table::data.table(
    start_abi = starts,
    end_abi   = ends,
    label     = paste0(
      format(starts, scientific = FALSE, trim = TRUE),
      " to ",
      format(ends, scientific = FALSE, trim = TRUE)
    ),
    abi_list  = lapply(seq_along(starts), function(i) abi_u[starts[i]:ends[i]])
  )
}




normalize_address <- function(x) {
  #' Normalize an address string for comparison.
  #' Oxygen labels (what this normalizer tries to do):
  #' - Standardize whitespace (trim + collapse multiple spaces)
  #' - Standardize the punctuation between $$\text{STATE}$$ and $$\text{ZIP}$$:
  #'     "AK 99803-2360"  and  "AK, 99803-2360"  both become "AK 99803-2360"
  #' - Leave other punctuation/case largely unchanged so we don't over-normalize
  #'
  #' @param x Character vector of addresses.
  #' @return Character vector of normalized addresses.
  
  # 1) Basic whitespace cleanup
  x <- trimws(x)
  x <- gsub("\\s+", " ", x)
  
  # 2) Remove a comma *only* when it appears between a 2-letter state and a ZIP
  #    Examples fixed:
  #    "..., AK, 99803-2360" -> "..., AK 99803-2360"
  #    "..., AK 99803-2360"  -> unchanged
  x <- gsub("([, ]\\b[A-Z]{2}),\\s*(\\d{5}(?:-\\d{4})?)\\b", "\\1 \\2", x)
  
  # 3) Cleanup any accidental space before commas (optional, but keeps things tidy)
  x <- gsub("\\s+,", ",", x)
  
  x
}




compare_tabs <- function(new_tab, old_tab,
                         new_addr_col = "best_address",
                         old_addr_col = "combined_address",
                         new_year_col = "archived_year",
                         old_year_col = "archived_year") {
  #' Compare "new" vs "old" tabs on (address, year) keys.
  #' 1) Validate that required columns exist in each table.
  #' 2) Build a key table for each input with two standardized columns:
  #'    - .addr = normalized address (from the specified address column)
  #'    - .year = archived year (from the specified year column)
  #' 3) Deduplicate keys (distinct) so comparisons are set-based.
  #' 4) Compute two set differences:
  #'    - Keys present in new_tab but not in old_tab
  #'    - Keys present in old_tab but not in new_tab
  #' 5) Return counts + a few example rows from each difference set.
  #'
  #' @param new_tab A data.frame/tibble containing the "new" data.
  #' @param old_tab A data.frame/tibble containing the "old" data.
  #' @param new_addr_col Column name in new_tab holding the address (default "best_address").
  #' @param old_addr_col Column name in old_tab holding the address (default "combined_address").
  #' @param new_year_col Column name in new_tab holding the year (default "archived_year").
  #' @param old_year_col Column name in old_tab holding the year (default "archived_year").
  #'
  #' @return A list with:
  #' - n_new_not_old: integer, number of (addr, year) keys in new but not old
  #' - n_old_not_new: integer, number of (addr, year) keys in old but not new
  #' - example_new_not_old: up to 10 example keys in new but not old
  #' - example_old_not_new: up to 10 example keys in old but not new
  #'
  #' @examples
  #' cmp <- compare_tabs(new_tab, old_tab)
  #' cmp$n_new_not_old
  #' cmp$example_new_not_old
  
  # --- Guardrails: fail fast if columns are missing ---
  stopifnot(
    new_addr_col %in% names(new_tab),
    old_addr_col %in% names(old_tab)
  )
  stopifnot(
    new_year_col %in% names(new_tab),
    old_year_col %in% names(old_tab)
  )
  
  # --- Build distinct key sets for each table ---
  new_keys <- new_tab %>%
    transmute(
      .addr = normalize_address(.data[[new_addr_col]]), # address standardized for comparison
      .year = .data[[new_year_col]]                     # keep year as-is (numeric/character)
    ) %>%
    distinct()
  
  old_keys <- old_tab %>%
    transmute(
      .addr = normalize_address(.data[[old_addr_col]]),
      .year = .data[[old_year_col]]
    ) %>%
    distinct()
  
  # --- Set differences (what's new? what's missing?) ---
  in_new_not_old <- anti_join(new_keys, old_keys, by = c(".addr", ".year"))
  in_old_not_new <- anti_join(old_keys, new_keys, by = c(".addr", ".year"))
  
  # --- Return counts + small samples for quick QC ---
  list(
    n_new_not_old       = nrow(in_new_not_old),
    n_old_not_new       = nrow(in_old_not_new),
    example_new_not_old = head(in_new_not_old, 10),
    example_old_not_new = head(in_old_not_new, 10)
  )
}




extract_range <- function(x) {
  #' Extract a numeric range (from/to) from a string like "234001 to 235000".
  #' Parses a character string containing a range expressed as
  #' $$\text{<from> to <to>}$$ (allowing arbitrary whitespace around "to"),
  #' and returns the endpoints as integers named `from` and `to`.
  #'
  #' @param x A character vector. Each element should contain two integers
  #'   separated by the word "to" (e.g., `"234001 to 235000"`).
  #'
  #' @return An integer vector of length 2 with names `from` and `to`.
  #'   If the pattern is not found, the returned values will be `NA_integer_`.
  
  m <- stringr::str_match(x, "(\\d+)\\s*to\\s*(\\d+)")
  c(from = as.integer(m[, 2]), to = as.integer(m[, 3]))
}




compile_parquet_folder <- function(subset_dir,
                                   pattern = "\\.parquet$",
                                   recursive = TRUE,
                                   read_one = function(f) {
                                     arrow::read_parquet(
                                       f,
                                       col_types = arrow::schema(...1 = arrow::null())
                                     )
                                   },
                                   abi_ref = NULL,
                                   church_dt = NULL,
                                   filter_states = FALSE,
                                   us_states = c(state.abb, "DC")) {
  #' Compile a folder of Parquet result files and generate QC summaries.
  #' Opens a directory of Parquet files as a single Arrow Dataset (lazy “compiled”
  #' handle) and computes several QC tables by iterating file-by-file with a
  #' progress bar. QC is computed on an in-memory tibble per file to preserve
  #' base-R semantics for $$nrow()$$, $$is.na()$$, and $$table()$$.
  #'
  #' ## ABI QC with optional state filtering
  #' If ABI QC is enabled (via `abi_ref` or `church_dt`), each file is assumed to
  #' correspond to a slice $$[from:to]$$ parsed from the filename (see `extract_range()`).
  #' The expected ABI set is $$\text{unique(abi_ref[from:to])}$$.
  #'
  #' If `filter_states = TRUE`, ABI QC restricts *both* expected and actual ABIs to
  #' an "allowed ABI universe" defined by:
  #' $$\text{abi is OK if all its rows satisfy state %in% us_states}.$$
  #'
  #' This makes the QC comparison reflect your external filtering logic (i.e., it
  #' does not penalize missing ABIs that are intentionally excluded by the state
  #' rule).
  #'
  #' @param subset_dir Character. Directory containing Parquet files.
  #' @param pattern Character. Regex used by `list.files()` to match Parquet files.
  #' @param recursive Logical. Whether to search `subset_dir` recursively.
  #' @param read_one Function. Given a file path, returns an Arrow table-like object
  #'   (default reads Parquet and skips column `...1`).
  #' @param abi_ref Optional vector. If supplied, performs an ABI range check against
  #'   slices of this reference vector based on filename ranges. If `NULL` and
  #'   `church_dt` is provided, `abi_ref` is derived internally as
  #'   $$\text{unique(church_dt$abi)}$$.
  #' @param church_dt Optional data.frame/tibble/data.table containing at least
  #'   columns `abi` and (if `filter_states=TRUE`) `state`. Used to (a) derive
  #'   `abi_ref` when missing, and (b) compute the allowed ABI universe.
  #' @param filter_states Logical. If TRUE, restrict ABI QC to ABIs whose rows in
  #'   `church_dt` satisfy $$\text{all(state %in% us_states)}$$.
  #' @param us_states Character vector of allowed US postal abbreviations. Defaults
  #'   to `c(state.abb, "DC")`.
  #'
  #' @return A list with two elements:
  #' \describe{
  #'   \item{data}{An `arrow::Dataset` for all Parquet files in `subset_dir`
  #'     (lazy; does not load all rows into memory).}
  #'   \item{qc}{A list of QC tibbles (some may be `NULL` if not applicable).}
  #' }
  
  # ---- 0) Validate inputs ---------------------------------------------------
  # If the user wants state filtering, we must have a reference table with abi/state.
  if (isTRUE(filter_states)) {
    if (is.null(church_dt)) {
      stop("filter_states=TRUE requires `church_dt` (e.g., church_2026_form_dt).")
    }
    if (!all(c("abi", "state") %in% names(church_dt))) {
      stop("`church_dt` must contain columns: abi, state when filter_states=TRUE.")
    }
  }
  
  # ---- 1) Discover candidate parquet files ---------------------------------
  files <- list.files(
    subset_dir,
    pattern = pattern,
    full.names = TRUE,
    recursive = recursive
  )
  if (length(files) == 0) stop("No parquet files found in: ", subset_dir)
  
  # ---- 2) Parse [from,to] ranges from filenames -----------------------------
  # We assume each file basename contains a substring like: "<from> to <to>"
  # which `extract_range()` parses into c(from=..., to=...).
  file_index <- tibble::tibble(file = files) %>%
    dplyr::mutate(
      base   = basename(file),
      rng    = purrr::map(base, extract_range),
      from   = purrr::map_int(rng, 1),
      to     = purrr::map_int(rng, 2),
      # Optional: extracted for labeling/diagnostics; not required for logic.
      arr_id = suppressWarnings(as.integer(stringr::str_match(base, "_slurmArray_(\\d+)")[, 2]))
    ) %>%
    dplyr::select(-rng) %>%
    dplyr::arrange(from, to, arr_id)
  
  if (any(is.na(file_index$from))) {
    bad <- file_index$file[is.na(file_index$from)]
    stop("Could not parse '<from> to <to>' range from:\n", paste(bad, collapse = "\n"))
  }
  
  # ---- 3) QC 1: ABI integrity check ----------------------------------------
  # Goal: ensure each file/chunk contains only ABIs expected for its [from,to] slice.
  qc_abi_results <- list()
  
  #' ABI QC for one file chunk, with optional "allowed states" filtering
  #'
  #' @param chunk A tibble/data.frame for one file, with column `abi`.
  #' @param from Integer. Start index parsed from filename.
  #' @param to Integer. End index parsed from filename.
  #' @param arr_id Integer-ish. Array identifier (used for labeling).
  #' @param file Character. File path (used for labeling/errors).
  #' @param abi_ref Optional vector defining the master ABI ordering; expected ABIs
  #'   are $$\text{unique(abi_ref[from:to])}$$. If `NULL` and `church_dt` is provided,
  #'   `abi_ref` is derived as $$\text{unique(church_dt$abi)}$$.
  #' @param church_dt Optional data.frame/tibble/data.table with columns `abi` and
  #'   `state`, used to compute the allowed ABI set when `filter_states=TRUE`.
  #' @param filter_states Logical. Toggle state-based filtering of ABIs.
  #' @param us_states Character vector of allowed state abbreviations.
  #' @param debug Logical. If TRUE, prints diagnostics about filtering and set sizes.
  #'
  #' @return A 1-row tibble summarizing ABI QC, or `NULL` if ABI QC is disabled.
  qc_abi <- function(chunk, from, to, arr_id, file,
                     abi_ref = NULL,
                     church_dt = NULL,
                     filter_states = FALSE,
                     us_states = c(state.abb, "DC"),
                     debug = FALSE) {
    
    # ABI QC disabled if we cannot construct a reference ABI vector.
    if (is.null(abi_ref)) {
      if (!is.null(church_dt)) {
        if (!("abi" %in% names(church_dt))) stop("`church_dt` must have an `abi` column.")
        abi_ref <- unique(church_dt$abi)
      } else {
        return(NULL)
      }
    }
    
    if (!"abi" %in% names(chunk)) stop("File has no 'abi' column:\n  ", file)
    
    # Expected ABIs are determined by the filename range slice of abi_ref.
    expected_abis <- unique(abi_ref[from:to])
    # Actual ABIs are what appear in the parquet chunk.
    actual_abis   <- unique(chunk$abi)
    
    # Optional: restrict both expected and actual ABIs to the ABI universe
    # that is "OK" under the allowed-states rule.
    #
    # This is the key fix that makes the comparison reflect your external filtering:
    # you should not count "missing" ABIs that are excluded by the state rule.
    abi_ok <- NULL
    if (isTRUE(filter_states)) {
      if (is.null(church_dt)) stop("qc_abi: filter_states=TRUE requires `church_dt`.")
      if (!all(c("abi", "state") %in% names(church_dt))) {
        stop("qc_abi: `church_dt` must contain columns: abi, state")
      }
      
      # Only compute ok-ness for ABIs relevant to this chunk.
      universe <- unique(c(expected_abis, actual_abis))
      
      if (inherits(church_dt, "data.table")) {
        abi_ok <- church_dt[
          abi %in% universe,
          .(ok = all(state %in% us_states)),
          by = abi
        ][ok == TRUE, abi]
      } else {
        abi_ok <- church_dt |>
          dplyr::filter(.data$abi %in% universe) |>
          dplyr::group_by(.data$abi) |>
          dplyr::summarise(ok = all(.data$state %in% us_states), .groups = "drop") |>
          dplyr::filter(.data$ok) |>
          dplyr::pull(.data$abi)
      }
      
      expected_abis <- intersect(expected_abis, abi_ok)
      actual_abis   <- intersect(actual_abis, abi_ok)
    }
    
    # ABIs present in the file but not expected for its slice is a hard error.
    unexpected <- setdiff(actual_abis, expected_abis)
    # ABIs expected for the slice but absent from the file are summarized.
    missing    <- setdiff(expected_abis, actual_abis)
    
    if (isTRUE(debug)) {
      message(
        "DEBUG qc_abi: file=", basename(file),
        " filter_states=", filter_states,
        " expected=", length(expected_abis),
        " actual=", length(actual_abis),
        " abi_ok=", if (is.null(abi_ok)) NA_integer_ else length(abi_ok),
        " missing=", length(missing),
        " unexpected=", length(unexpected)
      )
    }
    
    if (length(unexpected) > 0) {
      stop(
        sprintf(
          "ABI QC FAILED [%d to %d] — %d ABI(s) in chunk not found in abi_ref slice.\n",
          from, to, length(unexpected)
        ),
        sprintf("File: %s\n", file),
        "Unexpected ABIs: ", paste(head(unexpected, 10), collapse = ", "),
        if (length(unexpected) > 10) {
          sprintf(" ... and %d more.", length(unexpected) - 10)
        } else ""
      )
    }
    
    tibble::tibble(
      array             = sprintf("%03d", arr_id),
      file              = basename(file),
      from              = from,
      to                = to,
      n_expected_unique = length(expected_abis),
      n_actual_unique   = length(actual_abis),
      n_missing         = length(missing),
      qc_pass           = length(missing) == 0
    )
  }
  
  # ---- 4) QC 2: NA audit for census boundary/address fields -----------------
  qc_na_results <- list()
  
  qc_na_census_boundaries <- function(chunk, arr_id, file) {
    
    fixed_cols    <- c("address", "geoid_2000", "geoid_2010", "geoid_2020")
    wildcard_cols <- grep("^(cbsa_code_|cbsa_level_|csa_code_|zcta_)", names(chunk), value = TRUE)
    target_cols   <- intersect(c(fixed_cols, wildcard_cols), names(chunk))
    
    if (length(target_cols) == 0) return(NULL)
    
    if (!"address" %in% names(chunk)) {
      stop("File has no 'address' column (needed for unique-address QC):\n  ", file)
    }
    
    # Deduplicate to one record per address (keeps first occurrence)
    chunk_u <- dplyr::distinct(chunk, address, .keep_all = TRUE)
    na_counts <- purrr::map_int(target_cols, ~ sum(is.na(chunk_u[[.x]])))
    
    tibble::tibble(
      array   = sprintf("%03d", arr_id),
      column  = target_cols,
      n_addr  = nrow(chunk_u),
      n_na    = na_counts,
      pct_na  = round(100 * na_counts / nrow(chunk_u), 2)
    )
  }
  
  # ---- 5) QC 3: Frequency tables for verification/match flags ---------------
  flag_cols <- c("address_verified", "address_matched", "geolocation_verified", "geoid_match")
  
  qc_flag_results <- stats::setNames(vector("list", length(flag_cols)), flag_cols)
  for (nm in flag_cols) qc_flag_results[[nm]] <- NULL
  
  qc_flag_tables <- function(chunk, arr_id, file) {
    
    present <- intersect(flag_cols, names(chunk))
    if (length(present) == 0) return(NULL)
    
    if (!"address" %in% names(chunk)) {
      stop("File has no 'address' column (needed for unique-address QC):\n  ", file)
    }
    
    # Deduplicate to one record per address (keeps first occurrence)
    chunk_u <- dplyr::distinct(chunk, address, .keep_all = TRUE)
    
    purrr::map(present, function(col) {
      tbl        <- as.data.frame(table(chunk_u[[col]], useNA = "ifany"), stringsAsFactors = FALSE)
      names(tbl) <- c("value", "n")
      tbl$array  <- sprintf("%03d", arr_id)
      tbl$n_addr <- nrow(chunk_u)
      tbl$pct    <- round(100 * tbl$n / nrow(chunk_u), 2)
      tbl[, c("array", "value", "n", "n_addr", "pct")]
    }) |>
      rlang::set_names(present)
  }
  
  # ---- 6) Progress bar ------------------------------------------------------
  pb <- utils::txtProgressBar(min = 0, max = nrow(file_index), style = 3)
  on.exit(close(pb), add = TRUE)
  
  # ---- 7) Iterate files: read -> convert -> QC ------------------------------
  for (i in seq_len(nrow(file_index))) {
    f      <- file_index$file[[i]]
    from   <- file_index$from[[i]]
    to     <- file_index$to[[i]]
    arr_id <- file_index$arr_id[[i]]
    
    # Read parquet -> Arrow -> tibble so QC behaves like base R.
    chunk_arrow <- read_one(f)
    chunk <- tibble::as_tibble(chunk_arrow)
    
    # ABI QC (optional)
    r1 <- qc_abi(
      chunk, from, to, arr_id, f,
      abi_ref = abi_ref,
      church_dt = church_dt,
      filter_states = filter_states,
      us_states = us_states
      # debug = TRUE  # uncomment temporarily if you want diagnostics
    )
    if (!is.null(r1)) qc_abi_results[[length(qc_abi_results) + 1]] <- r1
    
    # NA QC (optional depending on columns)
    r2 <- qc_na_census_boundaries(chunk, arr_id, f)
    if (!is.null(r2)) qc_na_results[[length(qc_na_results) + 1]] <- r2
    
    # Flag QC
    r3 <- qc_flag_tables(chunk, arr_id, f)
    if (!is.null(r3)) {
      for (col in names(r3)) {
        qc_flag_results[[col]] <- dplyr::bind_rows(qc_flag_results[[col]], r3[[col]])
      }
    }
    
    utils::setTxtProgressBar(pb, i)
    
    rm(chunk_arrow, chunk)
    gc(FALSE)
  }
  
  # ---- 8) "Compiled" data handle (lazy) ------------------------------------
  data_compiled <- arrow::open_dataset(subset_dir, format = "parquet")
  
  # ---- 9) Return only data + qc --------------------------------------------
  list(
    data = data_compiled,
    qc   = list(
      abi_check            = if (length(qc_abi_results) > 0) dplyr::bind_rows(qc_abi_results) else NULL,
      address_verified     = qc_flag_results[["address_verified"]],
      address_matched      = qc_flag_results[["address_matched"]],
      geolocation_verified = qc_flag_results[["geolocation_verified"]],
      geoid_match          = qc_flag_results[["geoid_match"]],
      na_census_boundaries = if (length(qc_na_results) > 0) dplyr::bind_rows(qc_na_results) else NULL
    )
  )
}




compile_duckdb_folder <- function(subset_dir,
                                  pattern = "\\.db$",
                                  recursive = TRUE,
                                  abi_ref = NULL) {
  #' Compile DuckDB QC outputs from a folder (with progress, header cleanup, and ABI QC).
  #' Reads all DuckDB \code{.db} files in a directory (optionally recursively), loads each
  #' file via \code{read_list_from_duckdb()}, normalizes column names (including dotted
  #' headers), optionally performs an ABI integrity QC check, and then binds like-named
  #' tables across files.
  #'
  #' Column-name normalization:
  #' \itemize{
  #'   \item Renames \code{"Allow.USPS.API."} to \code{"Allow USPS API"}.
  #'   \item Replaces one-or-more periods with spaces (e.g., \code{"Any.Addresses.Line.1.NA"}
  #'         becomes \code{"Any Addresses Line 1 NA"}).
  #'   \item Collapses repeated whitespace and trims.
  #' }
  #'
  #' ABI QC (optional):
  #' When \code{abi_ref} is provided, the function checks every list element (table/data.frame)
  #' that contains an ABI column (case-insensitive match to \code{"abi"}). For each file and
  #' each ABI-bearing table, it compares the unique ABIs present to the expected ABIs from
  #' \code{abi_ref[from:to]}, where \code{from/to} are parsed from the filename pattern
  #' \code{"QC_<from> to <to>"}.
  #' Unexpected ABIs cause an error; missing ABIs are summarized in \code{out$qc_import}.
  #'
  #' @param subset_dir Directory containing DuckDB files.
  #' @param pattern Regex pattern passed to \code{list.files()} to identify DB files.
  #'   Defaults to \code{"\\\\.db$"}.
  #' @param recursive Logical; whether to search \code{subset_dir} recursively.
  #' @param abi_ref Optional reference vector of ABIs in the intended global order. If
  #'   provided, ABI integrity QC is enabled. If \code{NULL}, ABI QC is skipped.
  #'
  #' @return A named list. Each element corresponds to a table name returned by
  #'   \code{read_list_from_duckdb()}, with rows bound across all DB files. An additional
  #'   element \code{$qc_import} contains a tibble summarizing ABI QC results (or \code{NULL} if
  #'   ABI QC is disabled or no results are produced).
  #'
  #' @examples
  #' \dontrun{
  #' qc_address <- compile_duckdb_folder(
  #'   subset_dir = file.path(getwd(), data_root, "batch_array_18850425/Results/Address QC/"),
  #'   abi_ref    = unique(church_2026_form_dt$abi)
  #' )
  #'
  #' # View ABI QC summary
  #' qc_address$qc_import
  #'
  #' # Example: a normalized dotted header
  #' names(qc_address$qc1)
  #' }
  #'
  #' @export
  
  files <- list.files(
    subset_dir,
    pattern = pattern,
    full.names = TRUE,
    recursive = recursive
  )
  if (length(files) == 0) stop("No .db files found in: ", subset_dir)
  
  file_index <- tibble::tibble(file = files) %>%
    dplyr::mutate(
      base = basename(file),
      qc_from = as.integer(stringr::str_match(base, "QC_(\\d+)\\s*to\\s*(\\d+)")[, 2]),
      qc_to   = as.integer(stringr::str_match(base, "QC_(\\d+)\\s*to\\s*(\\d+)")[, 3]),
      arr_id  = suppressWarnings(as.integer(stringr::str_match(base, "_slurmArray_(\\d+)")[, 2]))
    ) %>%
    dplyr::arrange(qc_from, qc_to, arr_id)
  
  if (any(is.na(file_index$qc_from))) {
    bad <- file_index$file[is.na(file_index$qc_from)]
    stop("Could not parse QC range from:\n", paste(bad, collapse = "\n"))
  }
  
  # Progress bar (number of .db files)
  pb <- utils::txtProgressBar(min = 0, max = nrow(file_index), style = 3)
  on.exit(close(pb), add = TRUE)
  
  # Helper: recursively normalize column names across tibbles/data.frames inside a list
  # - Fixes the known odd header "Allow.USPS.API." -> "Allow USPS API"
  # - ONLY treats periods: converts one-or-more periods to single spaces
  # - Collapses repeated whitespace and trims
  # - Leaves underscores untouched
  fix_column_names <- function(x) {
    if (is.data.frame(x)) {
      nms <- names(x)
      
      # 1) Specific one-off fix first (exact match)
      nms[nms == "Allow.USPS.API."] <- "Allow USPS API"
      
      # 2) General cleanup for dotted headers ONLY
      nms <- gsub("\\.+", " ", nms)   # one-or-more periods -> single space
      nms <- gsub("\\s+", " ", nms)  # collapse multiple spaces
      nms <- trimws(nms)             # trim leading/trailing spaces
      
      names(x) <- nms
      return(x)
    }
    
    if (is.list(x)) return(lapply(x, fix_column_names))
    x
  }
  
  # Helper: find ALL list elements that are data.frames and have an ABI column
  # (ABI may appear as "abi", "ABI", "Abi", etc.)
  find_abi_tables <- function(chunk_list) {
    if (is.null(chunk_list) || !is.list(chunk_list)) return(list())
    
    out <- list()
    for (nm in names(chunk_list)) {
      obj <- chunk_list[[nm]]
      if (!is.data.frame(obj)) next
      
      nms <- names(obj)
      if (is.null(nms)) next
      
      if (any(tolower(nms) == "abi")) out[[nm]] <- obj
    }
    out
  }
  
  # ---- QC: ABI integrity check (case-insensitive ABI column) ----------------
  qc_abi_results <- list()
  
  qc_abi <- function(chunk, key, from, to, arr_id, file, abi_ref) {
    
    if (is.null(abi_ref)) return(NULL)  # ABI QC disabled if no reference provided
    if (is.null(chunk)) stop("ABI QC requested, but NULL chunk encountered in:\n  ", file)
    
    abi_col_idx <- which(tolower(names(chunk)) == "abi")
    if (length(abi_col_idx) == 0) {
      stop("ABI QC requested, but selected table has no ABI column (any case) in:\n  ", file,
           "\nList element: ", key)
    }
    # If multiple matches (rare), take the first deterministically
    abi_col <- names(chunk)[abi_col_idx[[1]]]
    
    expected_abis <- unique(abi_ref[from:to])
    actual_abis   <- unique(chunk[[abi_col]])
    unexpected    <- setdiff(actual_abis, expected_abis)
    missing       <- setdiff(expected_abis, actual_abis)
    
    if (length(unexpected) > 0) {
      stop(
        sprintf(
          "ABI QC FAILED [%d to %d] — %d ABI(s) not found in abi_ref slice.\n",
          from, to, length(unexpected)
        ),
        sprintf("File: %s\n", file),
        sprintf("List element: %s\n", key),
        sprintf("ABI column matched: %s\n", abi_col),
        "Unexpected ABIs: ", paste(head(unexpected, 10), collapse = ", "),
        if (length(unexpected) > 10) sprintf(" ... and %d more.", length(unexpected) - 10) else ""
      )
    }
    
    tibble::tibble(
      array             = sprintf("%03d", arr_id),
      file              = basename(file),
      key               = key,
      from              = from,
      to                = to,
      abi_col           = abi_col,
      n_expected_unique = length(expected_abis),
      n_actual_unique   = length(actual_abis),
      n_missing         = length(missing),
      qc_pass           = length(missing) == 0
    )
  }
  
  # ---- Read each .db and compute ABI QC per file ----------------------------
  chunks <- vector("list", nrow(file_index))
  for (i in seq_len(nrow(file_index))) {
    f      <- file_index$file[[i]]
    from   <- file_index$qc_from[[i]]
    to     <- file_index$qc_to[[i]]
    arr_id <- file_index$arr_id[[i]]
    
    chunk_list <- suppressMessages(
      suppressWarnings(
        read_list_from_duckdb(f)
      )
    ) |>
      fix_column_names()
    
    # ABI QC over ALL ABI-bearing list elements (optional)
    if (!is.null(abi_ref)) {
      abi_tables <- find_abi_tables(chunk_list)
      if (length(abi_tables) == 0) {
        stop("ABI QC requested, but no list element with an ABI column (any case) was found in:\n  ", f)
      }
      
      for (key in names(abi_tables)) {
        r1 <- qc_abi(abi_tables[[key]], key, from, to, arr_id, f, abi_ref)
        if (!is.null(r1)) qc_abi_results[[length(qc_abi_results) + 1]] <- r1
      }
    }
    
    chunks[[i]] <- chunk_list
    utils::setTxtProgressBar(pb, i)
  }
  
  # ---- Combine lists-of-tibbles across DBs by key name ----------------------
  keys <- Reduce(union, lapply(chunks, names))
  out  <- stats::setNames(vector("list", length(keys)), keys)
  
  for (k in keys) {
    pieces <- purrr::map(chunks, \(x) x[[k]]) |>
      purrr::compact()
    out[[k]] <- dplyr::bind_rows(pieces)
  }
  
  # Name fix again (belt-and-suspenders)
  out <- fix_column_names(out)
  
  # Attach QC output directly (tibble / NULL; not nested in a list)
  out$qc_import <- if (length(qc_abi_results) > 0) dplyr::bind_rows(qc_abi_results) else NULL
  
  out
}




write_qc_groups <- function(con, qc_groups,
                            prefixes = NULL,
                            on_missing = c("skip", "placeholder"),
                            verbose = TRUE) {
  #' Writes multiple *groups* of QC tables into an open DuckDB connection using a
  #' consistent naming convention: $$\texttt{<prefix>__<qc\_name>}$$
  #'
  #' This is useful when each batch (or cohort) has a list of QC tables, but some
  #' QC tables may be missing (`NULL`) in some groups. To avoid schema drift and
  #' keep downstream reads predictable, the function can create 0-row placeholder
  #' tables using a schema "template" learned from the first non-`NULL` instance
  #' of each QC table name across all groups.
  #'
  #' @param con A live DBI connection (e.g., from `DBI::dbConnect(duckdb::duckdb(), ...)`).
  #' @param qc_groups Named list of QC groups. Each element is itself a *named list*
  #'   of QC tables (data.frame-like objects) or `NULL`.
  #'   Example structure:
  #'   \itemize{
  #'     \item `qc_groups[["import_qc_18"]][["abi_check"]]` is a data.frame or `NULL`
  #'     \item `qc_groups[["import_qc_20"]][["abi_check"]]` is a data.frame or `NULL`
  #'   }
  #' @param prefixes Optional named character vector mapping group names to table prefixes.
  #'   If `NULL`, group names are used as-is.
  #'   Example: `c(import_qc_18 = "import_qc_18", import_qc_20 = "import_qc_20")`.
  #' @param on_missing What to do when a QC table is `NULL` for a group.
  #'   \itemize{
  #'     \item `"skip"`: do not write a table for that QC name if it is `NULL` and
  #'       no template schema can be inferred anywhere.
  #'     \item `"placeholder"`: write a placeholder 0-row table even if no template
  #'       exists (uses a trivial single column called `note`).
  #'   }
  #' @param verbose Logical. If `TRUE`, prints messages when skipping tables.
  #'
  #' @return Invisibly returns a list with:
  #' \itemize{
  #'   \item `templates`: named list of template data.frames used to create 0-row tables
  #'   \item `prefixes`: the resolved prefix mapping used
  #'   \item `groups`: the group names written (in iteration order)
  #' }
  #'
  #' @details
  #' Template inference rule: for each QC name (e.g., `"abi_check"`), the function
  #' scans groups in order and uses the *first* non-`NULL` table with $$ncol(df) > 0$$
  #' as the schema template. Placeholders are then created via:
  #' $$\texttt{tmpl[0, , drop = FALSE]}$$
  #' which yields a 0-row data.frame preserving column names and types as best as R allows.
  #'
  #' Each table is written with `overwrite = TRUE`.
  #'
  #' @examples
  #' \dontrun{
  #' qc_groups <- list(
  #'   import_qc_18 = list(abi_check = df18, address_verified = NULL),
  #'   import_qc_20 = list(abi_check = df20, address_verified = df20_addr)
  #' )
  #'
  #' write_qc_groups(con, qc_groups, on_missing = "placeholder")
  #' # Writes:
  #' # import_qc_18__abi_check, import_qc_18__address_verified (0-row placeholder)
  #' # import_qc_20__abi_check, import_qc_20__address_verified
  #' }
  
  # Normalize argument choices
  on_missing <- match.arg(on_missing)
  
  # Basic input validation
  stopifnot(is.list(qc_groups), length(qc_groups) > 0)
  stopifnot(all(nzchar(names(qc_groups))))
  
  # Resolve prefixes: by default, use group names as prefixes
  if (is.null(prefixes)) {
    prefixes <- setNames(names(qc_groups), names(qc_groups))
  } else {
    stopifnot(is.character(prefixes), all(names(qc_groups) %in% names(prefixes)))
  }
  
  # --- Build templates across ALL groups -----------------------------------
  # Goal: for each QC table name (e.g., "abi_check"), find a representative
  # non-NULL data.frame to use as a schema template for 0-row placeholders.
  all_qc_names <- unique(unlist(lapply(qc_groups, names)))
  
  templates <- list()
  for (nm in all_qc_names) {
    for (g in names(qc_groups)) {
      obj <- qc_groups[[g]][[nm]]
      if (!is.null(obj)) {
        df <- as.data.frame(obj)
        if (ncol(df) > 0) {
          templates[[nm]] <- df
          break
        }
      }
    }
  }
  
  # --- Write each group -----------------------------------------------------
  for (g in names(qc_groups)) {
    qc_list <- qc_groups[[g]]
    prefix  <- prefixes[[g]]
    
    stopifnot(is.list(qc_list))
    if (is.null(names(qc_list))) {
      stop("QC list for group '", g, "' must be a *named* list.")
    }
    
    for (nm in names(qc_list)) {
      obj <- qc_list[[nm]]
      
      # Decide what to write for this group + QC name
      if (is.null(obj)) {
        tmpl <- templates[[nm]]
        
        if (is.null(tmpl)) {
          # No data anywhere to infer a schema
          if (on_missing == "skip") {
            if (verbose) {
              message("Skipping ", prefix, "__", nm, " (NULL and no template anywhere)")
            }
            next
          } else {
            # Placeholder with minimal schema
            df <- data.frame(note = character(0))
          }
        } else {
          # Placeholder that preserves the inferred schema (0 rows, same columns)
          df <- tmpl[0, , drop = FALSE]
        }
        
      } else {
        # Real table exists for this group
        df <- as.data.frame(obj)
        
        # Defensive: skip empty-schema objects
        if (ncol(df) == 0) {
          if (verbose) message("Skipping ", prefix, "__", nm, " (0 columns)")
          next
        }
      }
      
      # Write to DuckDB as <prefix>__<qc_name>
      DBI::dbWriteTable(con, paste0(prefix, "__", nm), df, overwrite = TRUE)
    }
  }
  
  invisible(list(
    templates = templates,
    prefixes  = prefixes,
    groups    = names(qc_groups)
  ))
}




import_church_db <- function(db_path,
                             import_data = c("all", "data", "qc"),
                             data_table  = "data",
                             qc_prefixes = c("import_qc_18", "import_qc_20"),
                             read_only   = TRUE) {
  #' Import church-closures DuckDB tables (data + QC) with minimal dependencies.
  #' Reads tables from a DuckDB database file located at `db_path`.
  #' Designed to avoid `dplyr`/`dbplyr` and return a simple R list:
  #' - optionally the main compiled `data` table
  #' - QC tables grouped by prefix (e.g., `import_qc_18`, `import_qc_20`)
  #'
  #' QC tables are expected to follow the naming pattern:
  #'   <prefix>__<qc_name>
  #' For example: `import_qc_20__abi_check` becomes accessible as
  #' `res$import_qc_20$abi_check`.
  #'
  #' @param db_path Character scalar. Path to the DuckDB database directory/file.
  #' @param import_data Character. What to import:
  #'   \itemize{
  #'     \item `"all"`: import both the main data table and QC tables
  #'     \item `"data"`: import only the main data table
  #'     \item `"qc"`: import only QC tables (grouped by `qc_prefixes`)
  #'   }
  #' @param data_table Character scalar. Name of the main data table (default `"data"`).
  #' @param qc_prefixes Character vector. QC table prefixes to import (defaults to
  #'   `c("import_qc_18","import_qc_20")`).
  #' @param read_only Logical. Passed to DuckDB connection; should generally stay `TRUE`.
  #'
  #' @return A named list.
  #' - If `import_data` includes `"data"`, the list contains element `$data` (a data.frame).
  #' - If `import_data` includes `"qc"`, the list contains one element per prefix in
  #'   `qc_prefixes` (e.g., `$import_qc_18`, `$import_qc_20`), each a named list of
  #'   QC data.frames keyed by the suffix after `__`.
  #'
  #' @examples
  #' \dontrun{
  #' # QC only
  #' res_qc <- import_church_db(out_db, import_data = "qc")
  #' names(res_qc$import_qc_20)
  #' res_qc$import_qc_20$abi_check
  #'
  #' # Data only
  #' res_data <- import_church_db(out_db, import_data = "data")
  #' head(res_data$data)
  #'
  #' # Everything
  #' res_all <- import_church_db(out_db, import_data = "all")
  #' }
  
  # Validate/standardize `import_data`
  import_data <- match.arg(import_data)
  
  # Hard dependencies (kept minimal on purpose)
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required.")
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("Package 'duckdb' is required.")
  }
  
  # Open DuckDB connection to the database at `db_path`
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = read_only)
  
  # Always clean up connection, even if an error occurs mid-import.
  # This helps avoid: "Connection already working on another query"
  on.exit({
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    try(duckdb::duckdb_shutdown(), silent = TRUE)
  }, add = TRUE)
  
  # Discover available tables (we use this to find QC tables by prefix)
  tabs <- DBI::dbListTables(con)
  
  # The result container we will populate and return
  res <- list()
  
  # ---- Import main compiled data table (optional) ----
  if (import_data %in% c("all", "data")) {
    if (!data_table %in% tabs) {
      stop(sprintf("Data table '%s' not found in DB.", data_table))
    }
    res$data <- DBI::dbReadTable(con, data_table)
  }
  
  # ---- Import QC tables (optional) ----
  # For each prefix (e.g., "import_qc_20"), import all tables matching:
  #   ^import_qc_20__
  # Then strip the prefix from the list element names so you can do:
  #   res$import_qc_20$abi_check
  if (import_data %in% c("all", "qc")) {
    for (px in qc_prefixes) {
      # All QC tables for this prefix
      qc_tabs <- grep(paste0("^", px, "__"), tabs, value = TRUE)
      
      # Pre-allocate list for speed and set clean names (suffix after "__")
      out <- vector("list", length(qc_tabs))
      names(out) <- sub(paste0("^", px, "__"), "", qc_tabs)
      
      # Read each QC table into memory
      for (i in seq_along(qc_tabs)) {
        out[[i]] <- DBI::dbReadTable(con, qc_tabs[[i]])
      }
      
      # Store per-prefix QC list at res[[px]] (e.g., res$import_qc_20)
      res[[px]] <- out
    }
  }
  
  res
}




rbind_qc <- function(a, b) {
  #' Row-bind two QC tables with column alignment (NULL-safe).
  #' Takes two tabular objects (typically data.frames) and returns a single
  #' data.frame created by row-binding them. If the two inputs have different
  #' columns, missing columns are added and filled with `NA` so that `rbind()`
  #' succeeds. `NULL` inputs are treated as “missing tables”.
  #'
  #' @param a A data.frame-like object (data.frame, tibble, etc.) or `NULL`.
  #' @param b A data.frame-like object (data.frame, tibble, etc.) or `NULL`.
  #'
  #' @return
  #' - If both `a` and `b` are `NULL`, returns `NULL`.
  #' - Otherwise returns a base `data.frame` containing rows from `a` then `b`,
  #'   with the union of their columns. Missing columns are filled with `NA`.
  #'
  #' @details
  #' Column order in the result is the union of column names from `a` and `b`
  #' (as computed by `union()`), and row names are dropped to avoid duplicates.
  
  # Handle fully-missing inputs early
  if (is.null(a) && is.null(b)) return(NULL)
  
  # If only one side exists, return it (ensuring base data.frame)
  if (is.null(a)) return(as.data.frame(b))
  if (is.null(b)) return(as.data.frame(a))
  
  # Coerce both inputs to base data.frames (ensures consistent behavior)
  a <- as.data.frame(a)
  b <- as.data.frame(b)
  
  # Compute the full set of columns we need in the result
  all_cols <- union(names(a), names(b))
  
  # Add missing columns to each input, filled with NA
  for (cc in setdiff(all_cols, names(a))) a[[cc]] <- NA
  for (cc in setdiff(all_cols, names(b))) b[[cc]] <- NA
  
  # Reorder columns identically before binding rows
  a <- a[all_cols]
  b <- b[all_cols]
  
  # Drop row names to prevent duplicated/meaningless rownames after rbind
  rownames(a) <- NULL
  rownames(b) <- NULL
  
  # Row-bind (a on top of b)
  rbind(a, b)
}




flag_boxplot <- function(df, title, x_levels = NULL) {
  #' Boxplot + jitter for QC flag distributions across arrays/files.
  #' Creates a compact visualization for QC flag summaries (e.g., `address_verified`,
  #' `geoid_match`) where each row represents a batch/array and `pct` is the percent
  #' of unique addresses in a given flag category. The plot shows:
  #' - a boxplot summarizing the distribution of `pct` by flag value, and
  #' - jittered points showing each batch/array observation.
  #'
  #' If `x_levels` is supplied, the x-axis ordering is forced and missing values
  #' in `value` are displayed explicitly as the category `"NA"`.
  #'
  #' @param df A data.frame/data.table/tibble containing (at minimum) columns:
  #'   `value` (flag category) and `pct` (percentage, on 0–100 scale).
  #' @param title Character. Plot title.
  #' @param x_levels Optional character vector. If provided, sets the order of the
  #'   x-axis categories. An `"NA"` level is appended automatically to ensure missing
  #'   values are retained as a visible category.
  #'
  #' @return A `ggplot2` object.
  
  # Convert to data.table for fast in-place mutation (does not modify `df` in caller)
  d <- data.table::as.data.table(df)
  
  # ---- Force x-axis order (and show NA as its own category) -----------------
  # Many QC flag columns may contain NA. When x_levels is provided, we:
  #   1) recode NA -> "NA" (string)
  #   2) set factor levels so plotting order is consistent across figures
  #   3) keep "NA" as a visible category via drop = FALSE in scales
  if (!is.null(x_levels)) {
    d[, value := data.table::fifelse(is.na(value), "NA", as.character(value))]
    d[, value := factor(value, levels = c(x_levels, "NA"))]
  }
  
  ggplot2::ggplot(d, ggplot2::aes(x = value, y = pct, fill = value)) +
    ggplot2::geom_boxplot(alpha = 0.6, outlier.shape = NA) +
    ggplot2::geom_jitter(ggplot2::aes(colour = value), width = 0.15, size = 2, alpha = 0.8) +
    ggplot2::scale_fill_brewer(palette = "Set2", drop = FALSE) +
    ggplot2::scale_colour_brewer(palette = "Set2", drop = FALSE) +
    ggplot2::scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, 10)) +
    ggplot2::labs(
      title = title,
      x     = "Verification Status",
      y     = "% of Unique Addresses"
    ) +
    ggplot2::theme_minimal(base_size = 13) +
    ggplot2::theme(
      legend.position = "none",
      plot.title      = ggplot2::element_text(face = "bold", size = 11),
      axis.text.x     = ggplot2::element_text(angle = 25, hjust = 1)
    )
}




join_places_with_zip_fix <- function(dt, places_dt, zip_city_lookup) {
  #' Join city/state records to a places table, with ZIP-based fallback for missing 
  #' coordinates. Performs a two-step enrichment of a city-level dataset with 
  #' longitude/latitude:
  #' (1) primary join on (state, city) to `places_dt`;
  #' (2) for rows still missing lon/lat, uses `zipcode` to look up standardized 
  #'     city/state from `zip_city_lookup`, then re-joins to `places_dt` to 
  #'     recover coordinates.
  #'
  #' @param dt A data.table/data.frame containing (at minimum) columns: `state`, `city`, `zipcode`.
  #'           `state` and `city` should be comparable to `places_dt` keys (often uppercase/trimmed).
  #' @param places_dt A data.table/data.frame keyed by `state` and `city`, with columns `lon` and `lat`.
  #' @param zip_city_lookup A lookup table (data.frame/data.table) with columns `zip`, `city`, `state_id`
  #'        used to map ZIP codes to a standardized city/state.
  #'
  #' @return A data.table with all original columns plus `lon` and `lat` filled when possible.
  #'
  #' @details
  #' Only rows where `lon` or `lat` are NA after the primary join are considered for ZIP rematching.
  #' When writing back, coordinates are updated only if the original `lon`/`lat` are NA.
  #'
  #' @examples
  #' # na_joined <- join_places_with_zip_fix(na_by_city, places_dt, zip_city_lookup)
  #' # po_joined <- join_places_with_zip_fix(po_by_city, places_dt, zip_city_lookup)
  #'
  #' @export
  
  # Defensive copy to avoid modifying caller's object by reference
  dt <- data.table::copy(dt)
  
  # ---- 1) Primary join: (state, city) -> lon/lat -----------------------------
  # Left join keeps all rows in dt, adds lon/lat where there is a match in places_dt
  out <- merge(dt, places_dt, by = c("state", "city"), all.x = TRUE)
  
  # ---- 2) ZIP-based rematch for rows still missing lon/lat -------------------
  # Identify only rows that still lack coordinates
  need <- out[is.na(lon) | is.na(lat)]
  if (nrow(need) == 0L) return(out)
  
  # Build a standardized ZIP -> (city,state) lookup:
  # - ZIP padded to 5 characters
  # - city/state uppercased and trimmed for consistent matching
  zip_lu <- data.table::as.data.table(zip_city_lookup)[, `:=`(
    zip       = sprintf("%05s", zip),
    zip_city  = toupper(trimws(city)),
    zip_state = toupper(trimws(state_id))
  )]
  
  # Join ZIP info onto the subset needing fixes (by dt$zipcode -> zip_lu$zip)
  need <- merge(
    need,
    zip_lu[, .(zip, zip_city, zip_state)],
    by.x = "zipcode", by.y = "zip",
    all.x = TRUE
  )
  
  # Use ZIP-derived city/state when available; otherwise fall back to original
  need[, `:=`(
    state_zip = data.table::fifelse(!is.na(zip_state), zip_state, state),
    city_zip  = data.table::fifelse(!is.na(zip_city),  zip_city,  city)
  )]
  
  # Rematch to places_dt using the ZIP-derived (state_zip, city_zip)
  # Store candidate coordinates as lon_zip/lat_zip to avoid overwriting yet
  need <- merge(
    need,
    places_dt[, .(state, city, lon_zip = lon, lat_zip = lat)],
    by.x = c("state_zip", "city_zip"),
    by.y = c("state", "city"),
    all.x = TRUE
  )
  
  # ---- 3) Write back coordinates only where original lon/lat are NA ----------
  out[need,
      `:=`(
        lon = data.table::fifelse(is.na(lon), i.lon_zip, lon),
        lat = data.table::fifelse(is.na(lat), i.lat_zip, lat)
      ),
      on = .(state, city, zipcode)
  ]
  
  out
}






