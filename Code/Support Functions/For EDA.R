## ----------------------------------------------------------------
## Define functions used in the data exploration script.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: April 10th, 2026
## 
## Description: In addition to the general-purpose functions defined in another
##              script, the following functions are used to complete the EDA of
##              the raw data.
##
## Functions:
##    1. preprocess_address: This function standardizes the format of an 
##       address string to facilitate checking for address similarity. It 
##       performs the following steps:
##          1. Converts all characters to lowercase.
##          2. Normalizes spaces around commas and retains commas.
##          3. Removes all non-alphanumeric characters except for commas and spaces.
##          4. Normalizes multiple spaces to a single space.
##          5. Trims leading and trailing whitespace.
## 
##    2. find_components: This function performs Depth-First Search (DFS) to 
##       find all nodes in the connected component. It's used to identify similar 
##       addresses within a specified tolerance range, creating unique groups. 
##       Utilized in the `find_similar_addresses()` function.
## 
##    3. find_similar_addresses: This function groups addresses based on their 
##       similarity using a specified threshold. It preprocesses the addresses, 
##       builds a similarity graph, and identifies groups of similar addresses.
## 
##    4. build_zip_city_lookup: Takes the Simplemaps `uscities` dataset (e.g., 
##       `simplemaps_uscities_basicv1.90`) and creates a lookup table with 
##       **one row per 5-digit ZIP code**, mapping each ZIP to a single city/state.
## 
##    5. get_city_info: Looks up city name(s) for one or more ZIP codes in 
##       `zip_city_lookup`, converts them to uppercase, de-duplicates, and 
##       returns a single comma-separated string. If no matches are found, 
##       returns "No Matches Found: " followed by the ZIPs provided to `zip` 
##       (normalized to 5 digits where possible).


## ----------------------------------------------------------------
## FUNCTIONS

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





