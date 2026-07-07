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
##    9. validate_usps_address: Calls the USPS Addresses API (v3) to 
##       validate/standardize an address and returns a one-row tibble of the 
##       preferred USPS-formatted address. On failure or if no address is found, 
##       returns an empty tibble (0 rows).
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
##          \itemize{
##              \item HTTP status code is 200, and
##              \item \code{result$addressMatches} exists and contains at least 
##                    one match.
##          }
##       
##   14. validate_geolocation: Geocode an address (Census Geocoder) and return 
##       the best match, trying multiple benchmark/vintage pairs. This function 
##       queries the U.S. Census Geocoder "geographies/address" endpoint using 
##       a structured address (street/city/state/zip). It tries a prioritized 
##       sequence of benchmark/vintage combinations until it gets at least one 
##       candidate match, then applies a "best-candidate" selection procedure:
##          \enumerate{
##              \item If exactly one candidate, take it.
##              \item If multiple candidates, prefer those whose ZIP matches the 
##                    input ZIP.
##              \item If still ambiguous, use \code{find_similar_addresses()} 
##                    (assumed to exist in your codebase) to pick the most 
##                    similar candidate to the input address string.
##              \item If similarity logic does not resolve, fall back to the 
##                    first candidate.
##          }
##              
##       This is designed to validate and lock in a lon/lat for an address before 
##       you later assign decennial geographies via TIGER/Line shapefiles 
##       (point-in-polygon).
## 
##   15. read_state_gpkgs_for_data: Read per-state TIGER block GeoPackages for 
##       the unique states present in a dataset. This helper finds the unique 
##       states represented in `data`, locates each state's output GeoPackage 
##       in `out_root`, and reads *all layers* from each GeoPackage.
## 
##   16. add_decennial_geoid_block: Add decennial Census block GEOIDs 
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
##   17. decode_zcta: Decode (assign) ZCTA to points using a ZCTA polygon layer 
##       (fast sf join). Given a set of candidate point locations and a ZCTA 
##       polygon layer (e.g. 2000/2010/2020), this function performs a 
##       point-in-polygon join and returns the ZCTA code for each point.
## 
##       Implementation notes:
##          - Points are transformed into the CRS of the ZCTA layer before the join.
##          - `largest = TRUE` ensures only one polygon match is retained per 
##            point if multiple polygons match (rare; can occur for boundary 
##            cases or invalid geometries).
##          - The common sf warning about attributes being "spatially constant 
##            throughout all geometries" is suppressed; it is typically benign 
##            for ZCTA layers because `area_code` is constant per feature.


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




validate_geolocation <- function(street, city, state, zip,
                                 tries = list(
                                   list(benchmark = 2020, vintage = "Census2020_Census2020"),
                                   list(benchmark = 2020, vintage = "Census2010_Census2020"),
                                   list(benchmark = 4,    vintage = "Current_Current"),
                                   list(benchmark = 8,    vintage = "Current_ACS2025")
                                   ),
                                 quiet = FALSE) {
  #' Geocode an address (Census Geocoder) and return the best match, trying 
  #' multiple benchmark/vintage pairs. This function queries the U.S. Census 
  #' Geocoder "geographies/address" endpoint using a structured address 
  #' (street/city/state/zip). It tries a prioritized sequence of 
  #' benchmark/vintage combinations until it gets at least one candidate match, 
  #' then applies a "best-candidate" selection procedure:
  #' \enumerate{
  #'   \item If exactly one candidate, take it.
  #'   \item If multiple candidates, prefer those whose ZIP matches the input ZIP.
  #'   \item If still ambiguous, use \code{find_similar_addresses()} (assumed to exist in your codebase)
  #'         to pick the most similar candidate to the input address string.
  #'   \item If similarity logic does not resolve, fall back to the first candidate.
  #' }
  #'
  #' This is designed to validate and lock in a lon/lat for an address before you 
  #' later assign decennial geographies via TIGER/Line shapefiles (point-in-polygon).
  #'
  #' @param street Character scalar. Street address line (e.g., "55 Whitney Ave").
  #' @param city Character scalar. City name (e.g. "New Haven").
  #' @param state Character scalar. State postal abbreviation (e.g., "CT").
  #' @param zip Character scalar. ZIP code; may be 5-digit or ZIP+4 (e.g., "06510" or "06510-1234").
  #' @param tries List. Each element is a list with fields:
  #'   \describe{
  #'     \item{benchmark}{Numeric benchmark code used by the Census Geocoder.}
  #'     \item{vintage}{Either a numeric vintage \code{id} or a vintage name string (e.g., "Census2020_Census2020").}
  #'   }
  #'   The function tries these in order and stops at the first attempt yielding matches.
  #' @param quiet Logical. If \code{FALSE}, prints each attempt and whether it matched.
  #'
  #' @return A list with components:
  #'   \describe{
  #'     \item{ok}{Logical. \code{TRUE} if any attempt produced a match.}
  #'     \item{best}{If \code{ok=TRUE}, a list describing the selected best match, including
  #'                 \code{benchmark}, \code{vintage_input}, \code{vintage_id}, \code{matched_address},
  #'                 \code{lon}, \code{lat}, \code{geographies}, and \code{n_candidates}. Otherwise \code{NULL}.}
  #'     \item{parsed_response}{If \code{ok=TRUE}, the full parsed JSON response for the successful attempt; else \code{NULL}.}
  #'     \item{attempts}{A list of attempt metadata (attempt index, benchmark/vintage, status, URL, etc.) for debugging/audit.}
  #'   }
  #'
  #' @details
  #' This function depends on the following objects being defined in your environment:
  #' \itemize{
  #'   \item \code{build_addr_geo_url()} – constructs the request URL for the address-geographies endpoint.
  #'   \item \code{call_census_geocoder()} – executes the request and parses JSON, returning \code{ok}, \code{parsed}, etc.
  #'   \item \code{find_similar_addresses()} – your existing similarity matcher used in best-candidate selection.
  #'   \item The infix operators \code{\%||\%} and \code{\%!in\%} (as defined earlier in your script).
  #' }
  #'
  #' @examples
  #' \dontrun{
  #' res <- validate_geolocation(
  #'   street = "55 Whitney Ave",
  #'   city   = "New Haven",
  #'   state  = "CT",
  #'   zip    = "06510"
  #' )
  #'
  #' if (res$ok) {
  #' `succeeded_i <- which(vapply(res$attempts, function(a) isTRUE(a$ok), logical(1)))[1]
  #' 
  #' `data.frame(
  #'    matched_address = res$best$matched_address,
  #'    lon = res$best$lon, 
  #'    lat = res$best$lat,
  #'    attempt_i     = succeeded_i,
  #'    benchmark     = res$best$benchmark,
  #'    vintage_input = res$best$vintage_input,
  #'    vintage_id    = res$best$vintage_id,
  #'    stringsAsFactors = FALSE
  #' ``)
  #' } else {
  #'  do.call(rbind, lapply(res$attempts, as.data.frame))
  #' }
  #' }
  #' @export
  
  # Cache benchmark -> vintage tables so we only hit /geocoder/vintages once per 
  # benchmark.
  vintage_cache <- new.env(parent = emptyenv())
  
  # Convert a vintage value into a numeric vintage id.
  # - If user supplies numeric id, use it directly.
  # - If user supplies a vintage name string, look up the id for the given benchmark.
  resolve_vintage_id <- function(benchmark, vintage) {
    if (is.numeric(vintage) && length(vintage) == 1) return(vintage)
    
    if (is.null(vintage) || !nzchar(as.character(vintage))) {
      stop("vintage must be provided as an ID (numeric) or a vintage name string.")
    }
    
    key <- paste0("bmk_", benchmark)
    
    # Populate cache for this benchmark if needed.
    if (!exists(key, envir = vintage_cache, inherits = FALSE)) {
      url <- httr::modify_url(
        "https://geocoding.geo.census.gov/geocoder/vintages",
        query = list(format = "json", benchmark = benchmark)
      )
      
      resp <- httr::GET(url)
      if (httr::status_code(resp) != 200) {
        stop("Failed to look up vintages for benchmark=", benchmark,
             " (status ", httr::status_code(resp), ").")
      }
      
      txt <- httr::content(resp, "text", encoding = "UTF-8")
      v <- jsonlite::fromJSON(txt, simplifyVector = TRUE)$vintages
      assign(key, v, envir = vintage_cache)
    }
    
    # Resolve name -> id.
    v_df <- get(key, envir = vintage_cache, inherits = FALSE)
    hit <- v_df[v_df$vintageName == as.character(vintage), , drop = FALSE]
    
    if (nrow(hit) == 0) {
      stop("Vintage name '", vintage, "' not found for benchmark=", benchmark, ".")
    }
    
    as.numeric(hit$id[1])
  }
  
  # Select a single "best" candidate from parsed_response$result$addressMatches.
  # Implements your decision rules:
  #   (1) if only one candidate: take it
  #   (2) else: prefer ZIP matches
  #   (3) else: similarity matching with find_similar_addresses()
  #   (4) else: first candidate
  select_best_match <- function(parsed_response, street, city, state, zip) {
    
    api_result <- parsed_response$result$addressMatches
    if (is.null(api_result) || length(api_result) == 0) return(NULL)
    
    # Normalize ZIP: if ZIP+4, keep only the 5-digit ZIP for matching.
    if (!is.null(zip) && grepl("^\\d{5}-\\d{4}$", zip)) {
      zip <- sub("-.*$", "", zip)
    }
    
    # Standardized input address string for comparisons.
    target_address <- stringr::str_flatten(
      stringr::str_to_upper(c(street, city, state, zip)),
      ", "
    )
    
    # If exactly one candidate, accept.
    if (length(api_result) == 1) return(api_result[[1]])
    
    # Pull candidate fields (safe extraction).
    cand_addr <- vapply(api_result, function(x) x$matchedAddress %||% NA_character_, character(1))
    cand_zip  <- vapply(api_result, function(x) x$addressComponents$zip %||% NA_character_, character(1))
    
    # Step 1: prefer ZIP match if that yields exactly one candidate.
    zip_hits <- which(cand_zip %in% zip)
    
    if (length(zip_hits) == 1) {
      return(api_result[[zip_hits]])
    } else {
      
      # If multiple ZIP matches, restrict to those before similarity logic.
      if (length(zip_hits) > 1) {
        api_result <- api_result[zip_hits]
        cand_addr  <- cand_addr[zip_hits]
        cand_zip   <- cand_zip[zip_hits]
      }
      
      # Similarity tie-breaker using your existing function.
      comparisons <- c(target_address, cand_addr)
      
      threshold <- 0.2
      repeat {
        match <- find_similar_addresses(comparisons, threshold = threshold)
        
        too_many_line1 <- any(vapply(match, length, integer(1)) > 2)
        
        # Stop when narrowed sufficiently, OR threshold hits 0,
        # OR (threshold < 0.2 and all are singletons).
        if (!too_many_line1 || threshold <= 0 ||
            (threshold < 0.2 && all(vapply(match, length, integer(1)) == 1))) break
        
        # Tighten threshold and try again.
        threshold <- max(0, threshold - 0.01)
      }
      
      # If we found a cluster of exactly two addresses that includes the target,
      # choose the *other* address in that pair.
      clustered <- which(vapply(match, length, integer(1)) == 2)
      
      if (length(clustered) > 0 && any(match[[clustered[1]]] %in% target_address)) {
        matched <- unlist(match[clustered], use.names = FALSE) %>%
          .[. %!in% target_address]
        
        hit <- which(cand_addr %in% matched)[1]
        if (!is.na(hit)) return(api_result[[hit]])
      }
      
      # Final fallback: first candidate returned by API.
      return(api_result[[1]])
    }
  }
  
  # Track every attempt (for audit/debugging).
  all_attempts <- vector("list", length(tries))
  
  # Iterate through prioritized benchmark/vintage attempts.
  for (i in seq_along(tries)) {
    
    # Attempt definition.
    t <- tries[[i]]
    bmk <- t$benchmark
    vin_id <- resolve_vintage_id(bmk, t$vintage)
    
    # Build URL and call API.
    url <- build_addr_geo_url(
      street = street, city = city, state = state, zip = zip,
      benchmark = bmk, vintage = vin_id
    )
    
    out <- call_census_geocoder(url)
    
    # Store attempt metadata.
    all_attempts[[i]] <- list(
      i = i,
      benchmark = bmk,
      vintage_input = t$vintage,
      vintage_id = vin_id,
      ok = out$ok,
      status = out$status,
      url = out$url
    )
    
    if (!quiet) {
      cat(sprintf(
        "Try %d: benchmark=%s, vintage=%s (id=%s) -> %s\n",
        i, bmk, as.character(t$vintage), as.character(vin_id),
        ifelse(out$ok, "MATCH", "no match")
      ))
    }
    
    # If this attempt matched, choose the best candidate and return results.
    if (out$ok) {
      best_match <- select_best_match(out$parsed, street, city, state, zip)
      if (is.null(best_match)) next
      
      return(list(
        ok = TRUE,
        
        # Curated, single best candidate (the one you will use downstream).
        best = list(
          benchmark = bmk,
          vintage_input = t$vintage,
          vintage_id = vin_id,
          matched_address = best_match$matchedAddress %||% NA_character_,
          lon = best_match$coordinates$x %||% NA_real_,
          lat = best_match$coordinates$y %||% NA_real_,
          geographies = best_match$geographies,
          n_candidates = length(out$parsed$result$addressMatches)
        ),
        
        # Full successful response payload (useful for diagnostics/auditing).
        parsed_response = out$parsed,
        
        # Attempt-by-attempt log.
        attempts = all_attempts
      ))
    }
  }
  
  # No attempt matched.
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




add_cbsa_csa_codes <- function(cand_sf,
                               core_areas,
                               keep_names = FALSE,
                               state_col = "state") {
  #' Add CBSA/CSA codes (and CBSA MEMI/level) and ZCTA codes to candidate points by vintage.
  #'
  #' Uses layers in `core_areas`:
  #' - CBSA/CSA: `cbsa_csa_2007`, `cbsa_csa_2010`, `cbsa_csa_2020`
  #' - ZCTA:     `zcta_2000`,     `zcta_2010`,     `zcta_2020`
  #'
  #' CBSA/CSA output columns per year:
  #' - cbsa_code_YYYY
  #' - cbsa_level_YYYY
  #' - csa_code_YYYY
  #' - optionally: cbsa_name_YYYY, csa_name_YYYY
  #'
  #' ZCTA output columns per year:
  #' - zcta_YYYY  (from ZCTA `area_code`)
  #'
  #' @param cand_sf sf POINT layer; must include `row_id` and a state column.
  #' @param core_areas Named list that includes the layers listed above.
  #' @param keep_names Logical; if TRUE, include CBSA/CSA name columns per year.
  #' @param state_col Character; name of candidate state column (USPS abbrev).
  #'
  #' @return A data.frame (geometry dropped) with appended CBSA/CSA and ZCTA fields by year.
  
  # ---- Validate inputs ----
  if (!inherits(cand_sf, "sf")) stop("`cand_sf` must be an sf object.")
  if (!("row_id" %in% names(cand_sf))) stop("`cand_sf` must contain column: row_id.")
  if (!(state_col %in% names(cand_sf))) stop("`cand_sf` must contain column: ", state_col)
  if (!is.list(core_areas) || is.null(names(core_areas))) {
    stop("`core_areas` must be a *named* list with names like 'cbsa_csa_2007' and 'zcta_2020'.")
  }
  
  cbsa_csa_years <- c(2007, 2010, 2020)
  zcta_years     <- c(2000, 2010, 2020)
  
  get_col_or_na <- function(df, col) {
    if (col %in% names(df)) {
      x <- df[[col]]
      if (is.list(x)) {
        x <- vapply(
          x,
          function(z) if (length(z) == 0) NA_character_ else as.character(z[[1]]),
          character(1)
        )
      }
      as.character(x)
    } else {
      rep(NA_character_, nrow(df))
    }
  }
  
  base <- sf::st_drop_geometry(cand_sf)
  
  # --------------------------------------------------------------------------
  # [A] CBSA/CSA (2007/2010/2020)
  # --------------------------------------------------------------------------
  for (yr in cbsa_csa_years) {
    
    layer_name <- paste0("cbsa_csa_", yr)
    if (!layer_name %in% names(core_areas)) {
      warning("Missing layer ", layer_name, " in `core_areas`. Skipping year ", yr, ".")
      next
    }
    
    combo <- core_areas[[layer_name]]
    if (!inherits(combo, "sf")) stop(layer_name, " must be an sf object.")
    
    miss <- setdiff(c("area_type", "area_code"), names(combo))
    if (length(miss) > 0) stop(layer_name, " is missing columns: ", paste(miss, collapse = ", "))
    
    pts_x <- sf::st_transform(cand_sf, sf::st_crs(combo))
    
    joined <- sf::st_join(
      pts_x,
      dplyr::select(combo, dplyr::any_of(c("area_type", "area_code", "area_name", "area_level", "area_states"))),
      join = sf::st_within,
      left = TRUE
    ) |>
      sf::st_drop_geometry() |>
      dplyr::mutate(
        area_type = tolower(.data$area_type),
        area_type = dplyr::if_else(.data$area_type %in% c("cbsa", "csa"), .data$area_type, NA_character_),
        area_states = dplyr::coalesce(
          as.character(.data$area_states),
          stringr::str_extract(as.character(.data$area_name), "(?<=, )([A-Z]{2}(?:-[A-Z]{2})*)$")
        ),
        state_ok =
          !is.na(.data[[state_col]]) &
          !is.na(.data$area_states) &
          stringr::str_detect(
            paste0("-", .data$area_states, "-"),
            paste0("-", as.character(.data[[state_col]]), "-")
          )
      ) |>
      dplyr::filter(!is.na(.data$area_type), .data$state_ok) |>
      dplyr::select(-state_ok)
    
    wide <- joined |>
      dplyr::group_by(.data$row_id, .data$area_type) |>
      dplyr::summarise(
        area_code  = dplyr::first(stats::na.omit(.data$area_code)),
        area_name  = if ("area_name"  %in% names(joined)) dplyr::first(stats::na.omit(.data$area_name))  else NA_character_,
        area_level = if ("area_level" %in% names(joined)) dplyr::first(stats::na.omit(.data$area_level)) else NA_character_,
        .groups = "drop"
      ) |>
      tidyr::pivot_wider(
        names_from  = "area_type",
        values_from = c("area_code", "area_name", "area_level"),
        names_sep   = "_"
      )
    
    # Create year-specific output columns (do this AFTER the pipe so `wide` exists)
    wide[[paste0("cbsa_code_", yr)]]  <- get_col_or_na(wide, "area_code_cbsa")
    wide[[paste0("cbsa_level_", yr)]] <- get_col_or_na(wide, "area_level_cbsa")
    wide[[paste0("csa_code_", yr)]]   <- get_col_or_na(wide, "area_code_csa")
    
    if (keep_names) {
      wide[[paste0("cbsa_name_", yr)]] <- get_col_or_na(wide, "area_name_cbsa")
      wide[[paste0("csa_name_", yr)]]  <- get_col_or_na(wide, "area_name_csa")
    }
    
    keep <- c("row_id",
              paste0("cbsa_code_",  yr),
              paste0("cbsa_level_", yr),
              paste0("csa_code_",   yr))
    if (keep_names) keep <- c(keep, paste0("cbsa_name_", yr), paste0("csa_name_", yr))
    
    base <- dplyr::left_join(base, dplyr::select(wide, dplyr::any_of(keep)), by = "row_id")
  }
  
  # --------------------------------------------------------------------------
  # [B] ZCTA (2000/2010/2020)
  # --------------------------------------------------------------------------
  for (yr in zcta_years) {
    
    layer_name <- paste0("zcta_", yr)
    if (!layer_name %in% names(core_areas)) {
      warning("Missing layer ", layer_name, " in `core_areas`. Skipping year ", yr, ".")
      next
    }
    
    zcta <- core_areas[[layer_name]]
    if (!inherits(zcta, "sf")) stop(layer_name, " must be an sf object.")
    if (!("area_code" %in% names(zcta))) stop(layer_name, " is missing column: area_code")
    
    pts_x <- sf::st_transform(cand_sf, sf::st_crs(zcta))
    
    joined <- sf::st_join(
      pts_x,
      dplyr::select(zcta, dplyr::any_of("area_code")),
      join = sf::st_within,
      left = TRUE
    ) |>
      sf::st_drop_geometry()
    
    out <- joined |>
      dplyr::group_by(.data$row_id) |>
      dplyr::summarise(
        zcta_code = dplyr::first(stats::na.omit(.data$area_code)),
        .groups = "drop"
      )
    
    out[[paste0("zcta_", yr)]] <- as.character(out$zcta_code)
    out <- out |>
      dplyr::select(.data$row_id, !!paste0("zcta_", yr))
    
    base <- dplyr::left_join(base, out, by = "row_id")
  }
  
  base
}




decode_zcta <- function(cand_sf, zcta_sf, zcta_colname = "zcta") {
  #' Decode (assign) ZCTA to points using a ZCTA polygon layer (fast sf join).
  #' Given a set of candidate point locations and a ZCTA polygon layer 
  #' (e.g. 2000/2010/2020), this function performs a point-in-polygon join and 
  #' returns the ZCTA code for each point.
  #'
  #' Implementation notes:
  #' - Points are transformed into the CRS of the ZCTA layer before the join.
  #' - `largest = TRUE` ensures only one polygon match is retained per point if multiple
  #'   polygons match (rare; can occur for boundary cases or invalid geometries).
  #' - The common sf warning about attributes being "spatially constant throughout all
  #'   geometries" is suppressed; it is typically benign for ZCTA layers because `area_code`
  #'   is constant per feature.
  #'
  #' @param cand_sf An `sf` object with POINT geometry. Must include a unique `row_id` column.
  #' @param zcta_sf An `sf` object with (MULTI)POLYGON geometry. Must include an `area_code`
  #'   column containing the ZCTA identifier (often 5-digit, may include leading zeros).
  #' @param zcta_colname Character scalar. Name of the output ZCTA column (default: `"zcta"`).
  #'
  #' @return A data.frame with columns:
  #' \describe{
  #'   \item{row_id}{Candidate identifier copied from `cand_sf$row_id`.}
  #'   \item{<zcta_colname>}{ZCTA code (character) from `zcta_sf$area_code`, or `NA` if no match.}
  #' }
  #'
  #' @examples
  #' \dontrun{
  #'   # core_areas$zcta_2000 has polygons with area_code
  #'   z2000 <- decode_zcta_fast(cand_sf, core_areas$zcta_2000, "zcta_2000")
  #'   out <- dplyr::left_join(sf::st_drop_geometry(cand_sf), z2000, by = "row_id")
  #' }
  
  # ---- Input checks ----
  if (!inherits(cand_sf, "sf")) stop("`cand_sf` must be an sf object.")
  if (!inherits(zcta_sf, "sf")) stop("`zcta_sf` must be an sf object.")
  if (!("row_id" %in% names(cand_sf))) stop("`cand_sf` must contain column: row_id.")
  if (!("area_code" %in% names(zcta_sf))) stop("`zcta_sf` must contain column: area_code.")
  if (!is.character(zcta_colname) || length(zcta_colname) != 1L || nchar(zcta_colname) == 0L) {
    stop("`zcta_colname` must be a non-empty character scalar.")
  }
  
  # ---- CRS alignment ----
  # Transform points to the ZCTA CRS (assumes cand_sf already has a valid CRS set).
  pts <- sf::st_transform(cand_sf, sf::st_crs(zcta_sf))
  
  # ---- Point-in-polygon join (fast; avoids explicit R loops) ----
  suppressWarnings({
    joined <- sf::st_join(
      pts,
      # Keep only the attribute we need from the polygon layer.
      zcta_sf[, "area_code", drop = FALSE],
      join = sf::st_within,
      left = TRUE,
      largest = TRUE
    )
  })
  
  # ---- Return geometry-free result ----
  out <- sf::st_drop_geometry(joined)[, c("row_id", "area_code")]
  
  # Rename the ZCTA column and ensure it is character (preserves leading zeros).
  names(out)[2] <- zcta_colname
  out[[zcta_colname]] <- as.character(out[[zcta_colname]])
  
  out
}
