## ----------------------------------------------------------------
## Define functions used in the Step 2 script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 2nd, 2026
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
##    7. make_zip5_candidates: USPS/lookup data sometimes disagrees when a ZIP 
##       has leading/trailing zeros. This helper:
##          1) normalizes input to a 5-digit ZIP (keeps leading zeros),
##          2) counts edge zeros (leading + trailing),
##          3) strips ONLY those edge zeros to get the core,
##          4) rebuilds a sequence of candidate ZIPs by moving zeros one-by-one
##             from the front to the back.
## 
##    8. generate_usps_token: Requests an OAuth access token from USPS using the 
##       client credentials grant. Intended for use by `validate_usps_address()`.
##       
##    9. validate_usps_address: Validate and standardize a US address via the 
##       USPS Addresses API (v3). Obtains an OAuth token via 
##       \code{generate_usps_token()} (client credentials flow), then calls the 
##       USPS Addresses v3 endpoint to validate and standardize the supplied 
##       address. Returns a one-row tibble of the preferred USPS-formatted 
##       address on success.
##       
##       Source: https://developers.usps.com/addressesv3
##       Example: https://github.com/USPS/api-examples
##       
##   10. census_geo_show_options: Show available Census Geocoder benchmarks and 
##       vintages. Downloads the Census Geocoder benchmark list, optionally 
##       filters it, then downloads the vintages available for each benchmark 
##       displayed. Results are printed and also returned invisibly as a list 
##       of tibbles.
## 
##   11. census_geo_make_tries: Build a `tries` list from benchmarkName + 
##       vintageName pairs, Converts a human-readable specification 
##       (benchmarkName + vintageName strings) into the `tries` structure 
##       required by `validate_geolocation()`.
##       
##   12. build_addr_geo_url: Build a U.S. Census Geocoder request URL for 
##       structured address geographies Constructs the URL for the Census 
##       Geocoder endpoint \code{/geocoder/geographies/address} using a 
##       structured address (street/city/state/zip) and an explicit benchmark 
##       + vintage.
##       
##   13. call_census_geocoder: Call the Census Geocoder and parse the JSON 
##       response. Issues a GET request to the provided Census Geocoder URL and 
##       parses the returned JSON payload. The function marks the call as 
##       successful (\code{ok=TRUE}) only if:
##          - HTTP status code is 200, and
##          - \code{result$addressMatches} exists and contains at least 
##                    one match.
## 
##   14. select_best_match: Select the best candidate address match from a Census 
##       Geocoder response. Given a parsed Census Geocoder API response 
##       containing one or more candidate address matches, applies a prioritized 
##       decision procedure to select a single best match. Designed to be called 
##       immediately after a successful \code{call_census_geocoder()} result 
##       inside \code{validate_geolocation()}.
## 
##   15. resolve_vintage_id: Resolve a Census Geocoder vintage value to a 
##       numeric vintage id. Converts a vintage value — either a numeric id or 
##       a vintage name string — into the numeric id expected by the Census 
##       Geocoder API. When a name string is supplied, the function hits the 
##       \code{/geocoder/vintages} endpoint for the given benchmark and matches 
##       by \code{vintageName}. Results are cached in a caller-supplied 
##       environment so the endpoint is only hit once per unique benchmark per 
##       \code{validate_geolocation()} call.
##       
##   16. validate_geolocation: GGeocode an address (Census Geocoder) and return 
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
##   17. read_state_gpkgs_for_data: Read per-state TIGER block GeoPackages for 
##       the unique states present in a dataset. This helper finds the unique 
##       states represented in `data`, locates each state's output GeoPackage 
##       in `out_root`, and reads *all layers* from each GeoPackage.
## 
##   18. add_decennial_geoid_block: Add decennial Census block GEOIDs 
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
##   19. decode_zcta: Decode (Assign) ZCTA Codes to Point Locations. Given a 
##       set of candidate point locations and a ZCTA polygon layer (e.g. 2000, 
##       2010, or 2020 vintage), this function performs a point-in-polygon 
##       spatial join and returns the ZCTA code for each point.
## 
##       Performance: The state-based pre-filter (`state_col` + `area_states`) 
##       is applied to the raw `zcta_sf` object *before* any CRS transformation, 
##       column subsetting, or spatial join. This means all subsequent 
##       operations work on a much smaller polygon set, which is the primary 
##       speed lever for large national ZCTA layers.
## 
##       CRS: Points are re-projected into the CRS of `zcta_sf` before the join.
##       The original `cand_sf` object is not modified.
## 
##       Duplicate matches: `largest = TRUE` in `sf::st_join` ensures at most one
##       polygon match is retained per point. Duplicate matches are rare but 
##       can occur at polygon boundaries or with invalid geometries.
## 
##       Geometry warning suppression: The common sf warning about attributes 
##       being "spatially constant throughout all geometries" is suppressed; it 
##       is benign for ZCTA layers because `area_code` is constant per feature.
## 
##   20. decode_cbsa_csa: Decode (Assign) CBSA and CSA Codes to Point Locations. 
##       Given a set of candidate point locations and a combined CBSA/CSA 
##       polygon layer, this function performs two point-in-polygon spatial 
##       joins — one for Core Based Statistical Areas (CBSAs) and one for 
##       Combined Statistical Areas (CSAs) — and returns the corresponding codes 
##       and CBSA metropolitan/micropolitan level for each point.
## 
##       Performance: The state-based pre-filter (`state_col` + `area_states`) 
##       is applied to the raw `cbsa_csa_sf` object *before* any CRS 
##       transformation, polygon splitting, or spatial join. All subsequent 
##       operations therefore work on a much smaller polygon set, which is the 
##       primary speed lever for large national CBSA/CSA layers.
## 
##       Two-pass joining: After the pre-filter, the polygon layer is split into
##       CBSA and CSA subsets and each is joined independently. This allows 
##       CBSA-only attributes (`area_level`) to be handled cleanly without 
##       polluting the CSA result.
## 
##       Post-join state filter: A second, row-level state check is applied 
##       after each join via the internal `filter_by_state()` helper. This 
##       guards against the rare edge case where a point near a state border 
##       could be matched to a polygon whose `area_states` does not include the 
##       point's own state.
## 
##       CRS: Points are re-projected into the native CRS of the (now-filtered)
##       `cbsa_csa_sf` layer. The original `cand_sf` object is not modified.
## 
##       Duplicate matches: `largest = TRUE` in `sf::st_join` ensures at most 
##       one polygon match is retained per point, resolving rare boundary 
##       ambiguities by keeping the largest-area match.
## 
##       Geometry warning suppression: The common sf warning about attributes 
##       being "spatially constant throughout all geometries" is suppressed; it 
##       is benign here because `area_code` and `area_level` are constant per 
##       feature.
## 
##   21. format_year_ranges: Format a set of years into compact consecutive 
##       ranges (e.g., "2001:2003, 2006"). Takes a vector of years (possibly 
##       unsorted and with duplicates) and returns a human-readable string where 
##       consecutive years are collapsed into "start:end" ranges and separated 
##       by ", ".
## 
##   22. write_list_to_xlsx: Write a named list of tables to a multi-sheet Excel 
##       workbook (.xlsx). Takes a list where each element is a 
##       data.frame/tibble/data.table and writes each element to its own 
##       worksheet in an Excel file. List names are used as sheet names; 
##       unnamed/blank elements are assigned default names.
## 
##   23. write_list_to_duckdb: Write a list of tables to a single DuckDB 
##       database file. A lightweight replacement for writing a multi-sheet 
##       Excel workbook. Each element of `lst` is written as its own DuckDB 
##       table (analogous to an XLSX sheet) inside one `.duckdb` file.
## 
##       This workflow does not require DuckDB extensions (it uses built-in 
##       DuckDB functionality). Optionally, the function can verify that the 
##       user's home directory is writable and, if so, set DuckDB's storage home 
##       there to provide a stable location for extension caching *if extensions 
##       are ever used*.


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
  #' @param node An integer representing the starting node in the undirected graph. 
  #'             Each node represents similar addresses defined by the 
  #'             `stringdist(method = "jw")` function.
  #'             
  #' @param visited A logical vector indicating whether a node has been visited.
  #' 
  #' @param address_graph A list where each element contains the indices of
  #'                      its neighboring nodes.
  #'
  #' @return A vector containing all nodes in the connected component of the graph.
  
  
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



#' Old version... keep temporarily
#' validate_usps_address <- function(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "") {
#'   #' @description
#'   #' Calls the USPS Addresses API (v3) to validate/standardize an address and returns
#'   #' a one-row tibble of the preferred USPS-formatted address. On failure or if no
#'   #' address is found, returns an empty tibble (0 rows).
#'   #'
#'   #' @details
#'   #' This function obtains an OAuth token via `generate_usps_token()` (client credentials),
#'   #' then performs a GET request to the USPS Addresses v3 endpoint.
#'   #'
#'   #' USPS docs: https://developers.usps.com/addressesv3
#'   #'
#'   #' @param consumer_key Character. USPS API Consumer Key (client_id).
#'   #' @param consumer_secret Character. USPS API Consumer Secret (client_secret).
#'   #' @param address1 Character. Street address line 1.
#'   #' @param address2 Character. Secondary address (apt/suite/unit), default "".
#'   #' @param city Character. City.
#'   #' @param state Character. State abbreviation (e.g., "CT").
#'   #' @param zip5 Character. 5-digit ZIP code.
#'   #' @param zip4 Character. Optional 4-digit ZIP+4 extension, default "".
#'   #'
#'   #' @return
#'   #' A tibble with columns:
#'   #' `address_line_1`, `address_line_2`, `city`, `state`, `zipcode`, `zipcode_ext`.
#'   #' Returns an empty tibble on request failure or if USPS returns no `address` object.
#'   
#'   # 1) Get OAuth token (uses your existing generate_usps_token())
#'   token <- generate_usps_token(consumer_key, consumer_secret)
#'   
#'   # 2) USPS Addresses v3 endpoint
#'   base_url <- "https://apis.usps.com/addresses/v3/address"
#'   
#'   # 3) Build query parameters (ZIP5 required by the API call)
#'   params <- list(
#'     streetAddress    = address1,
#'     secondaryAddress = address2,
#'     city             = city,
#'     state            = state,
#'     ZIPCode          = zip5
#'   )
#'   
#'   # 4) Add ZIP+4 if provided; enforce exactly 4 digits
#'   if (nzchar(zip4)) {
#'     if (!grepl("^[0-9]{4}$", zip4)) stop("Invalid ZIPPlus4 format. Must be exactly 4 digits.")
#'     params$ZIPPlus4 <- zip4
#'   }
#'   
#'   # 5) Build the full request URL (query string included)
#'   request_url <- modify_url(base_url, query = params)
#'   
#'   # 6) Call the API with Bearer token auth
#'   resp <- GET(
#'     url = request_url,
#'     add_headers(
#'       accept = "application/json",
#'       Authorization = paste("Bearer", token)
#'     )
#'   )
#'   
#'   # 7) If request failed, warn and return an empty result (instead of NULL)
#'   if (status_code(resp) != 200) {
#'     warning(
#'       "USPS API request failed. Status: ", status_code(resp),
#'       " Body: ", content(resp, "text", encoding = "UTF-8")
#'     )
#'     return(tibble())
#'   }
#'   
#'   # 8) Parse JSON response
#'   parsed <- fromJSON(content(resp, "text", encoding = "UTF-8"),
#'                      simplifyVector = TRUE)
#'   
#'   # 9) Extract the address payload; if none, warn and return empty result
#'   addr <- parsed$address
#'   if (is.null(addr)) {
#'     warning("No valid addresses were found by the USPS API.")
#'     return(tibble::tibble())
#'   }
#'   
#'   # 10) Return a standardized one-row tibble with consistent column names
#'   tibble(
#'     address_line_1 = addr$streetAddress %||% "",
#'     address_line_2 = addr$secondaryAddress %||% "",
#'     city           = addr$city %||% "",
#'     state          = addr$state %||% "",
#'     zipcode        = addr$ZIPCode %||% "",
#'     zipcode_ext    = addr$ZIPPlus4 %||% ""
#'   )
#' }




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
    resp <- tryCatch(httr::GET(url), error = function(e) e)
    
    if (inherits(resp, "error")) {
      message(sprintf(
        "Skipping attempt: vintage lookup request failed for benchmark=%s — %s",
        benchmark, conditionMessage(resp)
      ))
      return(skip("vintage_lookup_network_error"))
    }
    
    # Catch unexpected HTTP status codes (e.g. invalid benchmark value).
    if (httr::status_code(resp) != 200) {
      message(sprintf(
        "Skipping attempt: vintage lookup returned HTTP %s for benchmark=%s.",
        httr::status_code(resp), benchmark
      ))
      return(skip(paste0("vintage_lookup_http_", httr::status_code(resp))))
    }
    
    # Catch JSON parse failures.
    txt <- httr::content(resp, "text", encoding = "UTF-8")
    v   <- tryCatch(
      jsonlite::fromJSON(txt, simplifyVector = TRUE)$vintages,
      error = function(e) {
        message("Skipping attempt: failed to parse vintages JSON — ", conditionMessage(e))
        NULL
      }
    )
    
    if (is.null(v)) return(skip("vintage_lookup_parse_error"))
    
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
    return(skip("vintage_name_not_found"))
  }
  
  as.numeric(hit$id[1])
}




#' Old version... keep temporarily
#' validate_geolocation <- function(street, city, state, zip,
#'                                  tries = list(
#'                                    list(benchmark = 2020, vintage = "Census2020_Census2020"),
#'                                    list(benchmark = 2020, vintage = "Census2010_Census2020"),
#'                                    list(benchmark = 4,    vintage = "Current_Current"),
#'                                    list(benchmark = 8,    vintage = "Current_ACS2025")
#'                                    ),
#'                                  quiet = FALSE) {
#'   #' Geocode an address (Census Geocoder) and return the best match, trying 
#'   #' multiple benchmark/vintage pairs. This function queries the U.S. Census 
#'   #' Geocoder "geographies/address" endpoint using a structured address 
#'   #' (street/city/state/zip). It tries a prioritized sequence of 
#'   #' benchmark/vintage combinations until it gets at least one candidate match, 
#'   #' then applies a "best-candidate" selection procedure:
#'   #' \enumerate{
#'   #'   \item If exactly one candidate, take it.
#'   #'   \item If multiple candidates, prefer those whose ZIP matches the input ZIP.
#'   #'   \item If still ambiguous, use \code{find_similar_addresses()} (assumed to exist in your codebase)
#'   #'         to pick the most similar candidate to the input address string.
#'   #'   \item If similarity logic does not resolve, fall back to the first candidate.
#'   #' }
#'   #'
#'   #' This is designed to validate and lock in a lon/lat for an address before you 
#'   #' later assign decennial geographies via TIGER/Line shapefiles (point-in-polygon).
#'   #'
#'   #' @param street Character scalar. Street address line (e.g., "55 Whitney Ave").
#'   #' @param city Character scalar. City name (e.g. "New Haven").
#'   #' @param state Character scalar. State postal abbreviation (e.g., "CT").
#'   #' @param zip Character scalar. ZIP code; may be 5-digit or ZIP+4 (e.g., "06510" or "06510-1234").
#'   #' @param tries List. Each element is a list with fields:
#'   #'   \describe{
#'   #'     \item{benchmark}{Numeric benchmark code used by the Census Geocoder.}
#'   #'     \item{vintage}{Either a numeric vintage \code{id} or a vintage name string (e.g., "Census2020_Census2020").}
#'   #'   }
#'   #'   The function tries these in order and stops at the first attempt yielding matches.
#'   #' @param quiet Logical. If \code{FALSE}, prints each attempt and whether it matched.
#'   #'
#'   #' @return A list with components:
#'   #'   \describe{
#'   #'     \item{ok}{Logical. \code{TRUE} if any attempt produced a match.}
#'   #'     \item{best}{If \code{ok=TRUE}, a list describing the selected best match, including
#'   #'                 \code{benchmark}, \code{vintage_input}, \code{vintage_id}, \code{matched_address},
#'   #'                 \code{lon}, \code{lat}, \code{geographies}, and \code{n_candidates}. Otherwise \code{NULL}.}
#'   #'     \item{parsed_response}{If \code{ok=TRUE}, the full parsed JSON response for the successful attempt; else \code{NULL}.}
#'   #'     \item{attempts}{A list of attempt metadata (attempt index, benchmark/vintage, status, URL, etc.) for debugging/audit.}
#'   #'   }
#'   #'
#'   #' @details
#'   #' This function depends on the following objects being defined in your environment:
#'   #' \itemize{
#'   #'   \item \code{build_addr_geo_url()} – constructs the request URL for the address-geographies endpoint.
#'   #'   \item \code{call_census_geocoder()} – executes the request and parses JSON, returning \code{ok}, \code{parsed}, etc.
#'   #'   \item \code{find_similar_addresses()} – your existing similarity matcher used in best-candidate selection.
#'   #'   \item The infix operators \code{\%||\%} and \code{\%!in\%} (as defined earlier in your script).
#'   #' }
#'   #'
#'   #' @examples
#'   #' \dontrun{
#'   #' res <- validate_geolocation(
#'   #'   street = "55 Whitney Ave",
#'   #'   city   = "New Haven",
#'   #'   state  = "CT",
#'   #'   zip    = "06510"
#'   #' )
#'   #'
#'   #' if (res$ok) {
#'   #' `succeeded_i <- which(vapply(res$attempts, function(a) isTRUE(a$ok), logical(1)))[1]
#'   #' 
#'   #' `data.frame(
#'   #'    matched_address = res$best$matched_address,
#'   #'    lon = res$best$lon, 
#'   #'    lat = res$best$lat,
#'   #'    attempt_i     = succeeded_i,
#'   #'    benchmark     = res$best$benchmark,
#'   #'    vintage_input = res$best$vintage_input,
#'   #'    vintage_id    = res$best$vintage_id,
#'   #'    stringsAsFactors = FALSE
#'   #' ``)
#'   #' } else {
#'   #'  do.call(rbind, lapply(res$attempts, as.data.frame))
#'   #' }
#'   #' }
#'   #' @export
#'   
#'   # Cache benchmark -> vintage tables so we only hit /geocoder/vintages once per 
#'   # benchmark.
#'   vintage_cache <- new.env(parent = emptyenv())
#'   
#'   # Convert a vintage value into a numeric vintage id.
#'   # - If user supplies numeric id, use it directly.
#'   # - If user supplies a vintage name string, look up the id for the given benchmark.
#'   resolve_vintage_id <- function(benchmark, vintage) {
#'     if (is.numeric(vintage) && length(vintage) == 1) return(vintage)
#'     
#'     if (is.null(vintage) || !nzchar(as.character(vintage))) {
#'       stop("vintage must be provided as an ID (numeric) or a vintage name string.")
#'     }
#'     
#'     key <- paste0("bmk_", benchmark)
#'     
#'     # Populate cache for this benchmark if needed.
#'     if (!exists(key, envir = vintage_cache, inherits = FALSE)) {
#'       url <- httr::modify_url(
#'         "https://geocoding.geo.census.gov/geocoder/vintages",
#'         query = list(format = "json", benchmark = benchmark)
#'       )
#'       
#'       resp <- httr::GET(url)
#'       if (httr::status_code(resp) != 200) {
#'         stop("Failed to look up vintages for benchmark=", benchmark,
#'              " (status ", httr::status_code(resp), ").")
#'       }
#'       
#'       txt <- httr::content(resp, "text", encoding = "UTF-8")
#'       v <- jsonlite::fromJSON(txt, simplifyVector = TRUE)$vintages
#'       assign(key, v, envir = vintage_cache)
#'     }
#'     
#'     # Resolve name -> id.
#'     v_df <- get(key, envir = vintage_cache, inherits = FALSE)
#'     hit <- v_df[v_df$vintageName == as.character(vintage), , drop = FALSE]
#'     
#'     if (nrow(hit) == 0) {
#'       stop("Vintage name '", vintage, "' not found for benchmark=", benchmark, ".")
#'     }
#'     
#'     as.numeric(hit$id[1])
#'   }
#'   
#'   # Track every attempt (for audit/debugging).
#'   all_attempts <- vector("list", length(tries))
#'   
#'   # Iterate through prioritized benchmark/vintage attempts.
#'   for (i in seq_along(tries)) {
#'     
#'     # Attempt definition.
#'     t <- tries[[i]]
#'     bmk <- t$benchmark
#'     vin_id <- resolve_vintage_id(bmk, t$vintage)
#'     
#'     # Build URL and call API.
#'     url <- build_addr_geo_url(
#'       street = street, city = city, state = state, zip = zip,
#'       benchmark = bmk, vintage = vin_id
#'     )
#'     
#'     out <- call_census_geocoder(url)
#'     
#'     # Store attempt metadata.
#'     all_attempts[[i]] <- list(
#'       i = i,
#'       benchmark = bmk,
#'       vintage_input = t$vintage,
#'       vintage_id = vin_id,
#'       ok = out$ok,
#'       status = out$status,
#'       url = out$url
#'     )
#'     
#'     if (!quiet) {
#'       cat(sprintf(
#'         "Try %d: benchmark=%s, vintage=%s (id=%s) -> %s\n",
#'         i, bmk, as.character(t$vintage), as.character(vin_id),
#'         ifelse(out$ok, "MATCH", "no match")
#'       ))
#'     }
#'     
#'     # If this attempt matched, choose the best candidate and return results.
#'     if (out$ok) {
#'       best_match <- select_best_match(out$parsed, street, city, state, zip)
#'       if (is.null(best_match)) next
#'       
#'       return(list(
#'         ok = TRUE,
#'         
#'         # Curated, single best candidate (the one you will use downstream).
#'         best = list(
#'           benchmark = bmk,
#'           vintage_input = t$vintage,
#'           vintage_id = vin_id,
#'           matched_address = best_match$matchedAddress %||% NA_character_,
#'           lon = best_match$coordinates$x %||% NA_real_,
#'           lat = best_match$coordinates$y %||% NA_real_,
#'           geographies = best_match$geographies,
#'           n_candidates = length(out$parsed$result$addressMatches)
#'         ),
#'         
#'         # Full successful response payload (useful for diagnostics/auditing).
#'         parsed_response = out$parsed,
#'         
#'         # Attempt-by-attempt log.
#'         attempts = all_attempts
#'       ))
#'     }
#'   }
#'   
#'   # No attempt matched.
#'   list(ok = FALSE, best = NULL, parsed_response = NULL, attempts = all_attempts)
#' }






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
  #' Decode (Assign) CBSA and CSA Codes to Point Locations. Given a set of 
  #' candidate point locations and a combined CBSA/CSA polygon layer, this 
  #' function performs two point-in-polygon spatial joins — one for Core Based 
  #' Statistical Areas (CBSAs) and one for Combined Statistical Areas (CSAs) — 
  #' and returns the corresponding codes and CBSA metropolitan/micropolitan 
  #' level for each point.
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
  #'                        when `state_col` is supplied.}
  #'                    }
  #' @param year        Single numeric value (e.g. `2015`). Appended as a suffix to
  #'                    the output column names: `cbsa_code_<year>`,
  #'                    `cbsa_level_<year>`, and `csa_code_<year>`.
  #' @param state_col   Character scalar or `NULL`. Name of a column in `cand_sf`
  #'                    holding state identifiers (e.g. FIPS codes or postal
  #'                    abbreviations). When supplied *and* `cbsa_csa_sf` contains
  #'                    an `area_states` column, all polygons are pre-filtered to
  #'                    only those whose `area_states` overlaps the states present
  #'                    in `cand_sf` before any CRS or spatial work is performed —
  #'                    the primary speed optimisation for large national layers.
  #'                    Defaults to `NULL` (no filtering).
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
  #' **Post-join state filter:** A second, row-level state check is applied after
  #' each join via the internal `filter_by_state()` helper. This guards against
  #' the rare edge case where a point near a state border could be matched to a
  #' polygon whose `area_states` does not include the point's own state.
  #'
  #' **CRS:** Points are re-projected into the native CRS of the (now-filtered)
  #' `cbsa_csa_sf` layer. The original `cand_sf` object is not modified.
  #'
  #' **Duplicate matches:** `largest = TRUE` in `sf::st_join` ensures at most one
  #' polygon match is retained per point, resolving rare boundary ambiguities by
  #' keeping the largest-area match.
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
  #'   # Skip state filtering entirely
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
  
  # ---- Post-join state filtering helper ----
  # Applied after each spatial join as a secondary guard: removes matched rows
  # where the polygon's area_states does not include the point's own state.
  # Polygons with NA area_states are always retained (no state info to filter on).
  # Uses the same hyphen-sentinel pattern as the pre-filter above.
  filter_by_state <- function(joined_df, poly_states_col, point_states) {
    ps <- as.character(joined_df[[poly_states_col]])
    ok <- is.na(ps) | stringr::str_detect(paste0("-", ps, "-"), paste0("-", point_states, "-"))
    joined_df[ok, , drop = FALSE]
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
    cbsa_df <- filter_by_state(cbsa_df, "area_states", as.character(cbsa_df[[state_col]]))
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
    csa_df <- filter_by_state(csa_df, "area_states", as.character(csa_df[[state_col]]))
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










