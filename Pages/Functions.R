## ----------------------------------------------------------------
## Define the coding parameters used in the environment.
##
##      Authors: Shelby Golden, MS from Yale's YSPH DSDE group
## Date Created: May 15th, 2025
## 
## Description: All custom functions used in the raw data cleaning
##              and preparation process. Much of this content was written
##              with the assistance of Yale's AI Clarity.
##
## Functions
##    1. preprocess_address: To check address similarity, we first standardize 
##       their format by changing the caps to lowercase, removing special 
##       characters (non-alphanumeric), and then standardizing the spacing. 
##       This function allows the presence of commas, as they are common 
##       components of addresses and simplify parsing down the line.
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
##    4. find_first_one: Finds the date column name where the first 1 
##       occurs. Used for arranging the rows associated with one ABI 
##       in descending order: i.e. older address to recent address.
##
##    5. validate_geocoordinates: This function validates an address by querying 
##       the Census Geocoder API and retrieves the geographical coordinates 
##       (latitude and longitude) of the address. The source of the data is the 
##       Census Geocoder API: https://geocoding.geo.census.gov/geocoder/
##
##    6. generate_usps_token: This function generates an OAuth token from the 
##       USPS API using the provided consumer key and consumer secret. It is 
##       primarily used in the `validate_usps_address()` function.
##
##    7. validate_usps_address: This function validates an address by querying 
##       the Census Geocoder API and retrieves the geographical coordinates 
##       (latitude and longitude) of the address. The source of the data is the 
##       Census Geocoder API.
##       Source: https://developers.usps.com/addressesv3
##       Example: https://github.com/USPS/api-examples
## 
##    8. validate_usps_address: This function validates an address using the 
##       USPS Addresses API 3.0 and retrieves the preferred USPS address for it.
##
##    9. validate_geocoordinates_google: This function validates an address by 
##       querying the Google Geocoding API and retrieves the geographical 
##       coordinates (latitude and longitude) of the address. The source of the 
##       data is the Google Geocoding API: 
##       https://developers.google.com/maps/documentation/geocoding/start
## 
##   10. find_date_combinations: This function finds combinations of dates with 
##       a fixed gap (in years) from a given range of dates.
##
##   11. fill_zeros: This function replaces occurrences of one, two, or three 
##       zeros between ones in a binary string with ones, while maintaining 
##       the original string length.
##
##   12. fill_zeros_with_progress: This fills in zeros between patterns of ones, 
##       updates the progress bar, and applies the `fill_zeros` function to the 
##       group.
##
##   13. check_all_counts_0_or_1: This function summarizes the selected year 
##       columns (if the columns are preceded by a "20") by summing them, and 
##       checks if all counts in these columns are either 0 or 1.
##
##   14. process_with_progress: This function processes a group within a data 
##       frame, updates the progress bar, and applies a specified function to 
##       the group.
##
##   15. process_with_progress: This function processes a group within a data 
##       frame, updates the progress bar, and applies a specified function to 
##       the group.
##
##   16. is_acronym_or_city: This function checks if a given string is an 
##       acronym or a city name using regular expressions for acronyms and a US 
##       city database for city names.
##
##   17. capture_warnings: This function evaluates an expression and captures 
##       any warnings generated during the evaluation. 
##
##   18. get_census_tract_geocoder: This function takes an address string,
##       splits it  into components, and uses the Census Geocoder API to get
##       census tract information for that address.
##       Source: https://geocoding.geo.census.gov/geocoder/
##
##   19. get_tracts_for_year: This inner function used by 
##       find_census_info_tidycensus() retrieves census tract data for a 
##       specified year.
##
##   20. find_census_info_tidycensus: This function finds the census tract, 
##       county, and state information for specified addresses across the 2000, 
##       2010, and 2020 decennial census periods. Uses the tidycensus package to 
##       reference the Census Bureau's database.
##       
##       tidycensus: https://walker-data.com/tidycensus/index.html
##
##   21. decide_reference: This function finds the relevant reference period for 
##       a given date range based on two methods: using the ending year to 
##       decide, or using the reference period that spans the given date range 
##       the most.
##
##   22. replace_trailing_zeros: To calculate the persistence lookahead 
##       correctly for subsets with an end date near the last date recorded in 
##       the dataset, we arbitrarily add 1's to replace the last one to three 
##       0's. These are not kept in the dataset, they are only a temporary 
##       measure in an effort to not distort the result.
## 
##   23. calculate_persistence: This function calculates the persistence ratio 
##       of a church being open in a subset period compared to the full span of 
##       time recorded.
##
##   24. mutate_with_progress: This function processes each group within a data 
##       frame by converting specified columns while updating the progress bar 
##       to track completion. It ensures all values above zero are converted to 
##       1, and zeroes remain unchanged.

## ----------------------------------------------------------------
## FUNCTIONS

preprocess_address <- function(address) {
  #' @description 
  #' To check address similarity, we first standardize their format by changing
  #' the caps to lowercase, removing special characters (non-alphanumeric),
  #' and then standardizing the spacing. This function allows the presence of
  #' commas, as they are common components of addresses and simplify parsing
  #' down the line.
  #'
  #' @param address the string containing the address.
  #' 
  #' @returns the cleaned, standardized address.
  
  
  # Convert to lowercase.
  address <- tolower(address)
  
  # Normalize spaces around commas and retain commas in addresses.
  address <- gsub("\\s*,\\s*", ", ", address)
  
  # Remove all characters except alphanumeric characters, commas, and spaces.
  address <- gsub("[^a-z0-9, ]", "", address)
  
  # Normalize spaces again just in case
  address <- gsub("\\s+", " ", address)
  
  return(trimws(address))
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



validate_geocoordinates_geoCoder <- function(address) {
  #' @description
  #' This function validates an address by querying the Census Geocoder API and 
  #' retrieves the geographical coordinates (latitude and longitude) of the 
  #' address. The source of the data is the Census Geocoder API.
  #' 
  #' Source: https://geocoding.geo.census.gov/geocoder/
  #' 
  #' @param address A character string representing the full address in the format 
  #'                "Street, City, State ZIP".
  #' 
  #' @return A data frame containing the validated formatted address, latitude, 
  #'         and longitude. If the address is invalid or no matches are found, 
  #'         it returns with a warning.
  #' 
  #' @examples
  #' validate_geocoordinates("1600 Amphitheatre Parkway, Mountain View, CA 94043")
  #' validate_geocoordinates("200 MARYLAND AVE NE, STE 302, WASHINGTON, DC 20002-5724")
  
  # Split the address into its components (assuming comma-separated format).
  address_components <- unlist(strsplit(address, ', '))
  if (length(address_components) < 3) {
    warning("Invalid address format. Please provide a full address.")
    return(NULL)
  }
  
  # Handle address components based on length
  if (length(address_components) == 4) {
    street <- paste(address_components[1], address_components[2], sep = ", ")
    city <- address_components[3]
    state_zip <- unlist(strsplit(address_components[4], ' '))
  } else {
    street <- address_components[1]
    city <- address_components[2]
    state_zip <- unlist(strsplit(address_components[3], ' '))
  }
  
  # Validate state and ZIP
  if (length(state_zip) < 2) {
    warning("Invalid state and ZIP code format. Please provide a full address.")
    return(NULL)
  }
  
  state <- state_zip[1]
  zip <- state_zip[2]
  
  # Construct the request URL for the Census Geocoder API.
  base_url <- "https://geocoding.geo.census.gov/geocoder/geographies/address"
  request_url <- paste0(
    base_url, "?format=json&benchmark=Public_AR_Current&vintage=Census2020_Current",
    "&street=", URLencode(street),
    "&city=", URLencode(city),
    "&state=", URLencode(state),
    "&zip=", URLencode(zip)
  )
  
  # Send GET request to the API.
  response <- GET(request_url)
  
  # Check for a successful response.
  if (status_code(response) != 200) {
    warning("Failed to fetch data from Census Geocoder API. Status code: ", status_code(response))
    return(NULL)
  }
  
  # Parse the JSON response.
  content_text <- content(response, "text", encoding = "UTF-8")
  parsed_response <- fromJSON(content_text)
  
  # Validate response structure.
  if (!("result" %in% names(parsed_response)) || 
      !("addressMatches" %in% names(parsed_response$result)) || 
      length(parsed_response$result$addressMatches) == 0) {
    warning("Census Geocoder API error: Address not found or no matches.")
    return(NULL)
  }
  
  address_matches <- parsed_response$result$addressMatches
  
  # Check if address_matches is empty.
  if (length(address_matches) == 0) {
    warning("Census Geocoder API error: Address matches list is empty.")
    return(NULL)
  }
  
  # Get the first match
  address_match <- address_matches
  
  # Correct path to `coordinates` in `Census Blocks`
  if ("geographies" %in% names(address_match) && 
      "Census Blocks" %in% names(address_match$geographies) &&
      length(address_match$geographies$`Census Blocks`) > 0) {
    
    coordinates_info <- address_match$geographies$`Census Blocks`[[1]]
    
    if (!is.null(coordinates_info$INTPTLAT) && !is.null(coordinates_info$INTPTLON)) {
      lat <- as.numeric(coordinates_info$INTPTLAT)
      lon <- as.numeric(coordinates_info$INTPTLON)
      
    } else {
      warning("Census Geocoder API error: 'coordinates' fields INTPTLAT or INTPTLON not found.")
      return(NULL)
    }
  } else {
    warning("Census Geocoder API error: 'coordinates' not found in the 'Census Blocks' section.")
    return(NULL)
  }
  
  # Ensure presence of formatted address.
  if ( !("matchedAddress" %in% names(address_match)) ) {
    warning("Census Geocoder API error: 'matchedAddress' not found in response.")
    return(NULL)
  }
  
  formatted_address <- address_match$matchedAddress
  
  # Return the results including coordinates.
  result_data <- data.frame(
    Address = formatted_address,
    Latitude = lat,
    Longitude = lon
  )
  
  return(result_data)
}



generate_usps_token <- function(consumer_key, consumer_secret) {
  #' @description
  #' This function generates an OAuth token from the USPS API using the provided 
  #' consumer key and consumer secret. It is primarily used in the 
  #' `validate_usps_address()` function.
  #'
  #' @param consumer_key A character string representing your USPS Web Tools API 
  #'                     Consumer Key.
  #'                     
  #' @param consumer_secret A character string representing your USPS Web Tools 
  #'                        API Consumer Secret.
  #'
  #' @return A character string containing the OAuth token.
  
  
  # Define the OAuth token endpoint URL.
  oauth_url <- "https://apis.usps.com/oauth2/v3/token"
  
  # Define the request body using the correct format.
  request_body <- list(
    client_id = consumer_key,
    client_secret = consumer_secret,
    grant_type = "client_credentials"
  )
  
  # Send a POST request to the OAuth endpoint with the required headers and 
  # JSON body.
  response <- POST(
    url = oauth_url,
    add_headers("Content-Type" = "application/json"),
    body = toJSON(request_body, auto_unbox = TRUE),
    encode = "json"
  )
  
  # Check if the response status code is not 200 (success), then stop and report 
  # the status code and response content.
  if (status_code(response) != 200) {
    stop("Failed to obtain OAuth token. Status code: ", status_code(response), ", Response content: ", content(response, "text"))
  }
  
  # Extract the OAuth token from the response content.
  token <- content(response, "parsed")$access_token
  return(token)
}



validate_usps_address <- function(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "") {
  #' @description
  #' This function validates an address using the USPS Addresses API 3.0 and 
  #' retrieves the preferred USPS address for it. It requires the consumer
  #' key and secret, which can only be created by a USPS Developer account
  #' holder.
  #' 
  #' Source: https://developers.usps.com/addressesv3
  #' Example: https://github.com/USPS/api-examples
  #'
  #' @param consumer_key Your USPS Web Tools API Consumer Key.
  #' @param consumer_secret Your USPS Web Tools API Consumer Secret.
  #' @param address2 Required address line 2.
  #' @param address1 Optional address line 1.
  #' @param city City name.
  #' @param state State abbreviation.
  #' @param zip5 5-digit ZIP code.
  #' @param zip4 Optional 4-digit ZIP code extension.
  #'
  #' @return A data frame containing the preferred USPS formatted address.
  #'
  #' @examples
  #' consumer_key <- "<your_consumer_key>"  # Replace with your USPS Web Tools Consumer Key
  #' consumer_secret <- "<your_consumer_secret>"  # Replace with your USPS Web Tools Consumer Secret
  #' validate_usps_address(consumer_key, consumer_secret, "1600 Amphitheatre Parkway", "", "Mountain View", "CA", "94043")
  #' 
  
  # Generate OAuth token
  token <- generate_usps_token(consumer_key, consumer_secret)
  
  # Correct USPS API Endpoint
  base_url <- "https://apis.usps.com/addresses/v3/address"
  
  # Build Query Parameters
  params <- list(
    streetAddress = address1,
    secondaryAddress = address2,
    city = city,
    state = state,
    ZIPCode = zip5
  )
  
  # Include ZIPPlus4 if needed and validate format
  if (zip4 != "") {
    if (grepl("^[0-9]{4}$", zip4)) {
      params$ZIPPlus4 <- zip4
    } else {
      stop("Invalid ZIPPlus4 format. Must be exactly 4 digits.")
    }
  }
  
  # Construct the full URL
  request_url <- modify_url(base_url, query = params)
  
  # GET Request with OAuth Token
  response <- GET(
    url = request_url,
    add_headers(
      "accept" = "application/json",
      "Authorization" = paste("Bearer", token)
    )
  )
  
  if (status_code(response) != 200) {
    warning("Failed to fetch data from USPS API. Status code: ", status_code(response), ", Response content: ", content(response, "text", encoding = "UTF-8"))
    return(NULL)
  }
  
  # Parse the JSON response
  parsed_response <- fromJSON(content(response, "text", encoding = "UTF-8"))
  
  # Validate and extract the address data accurately
  if (!is.null(parsed_response$address)) {
    address <- parsed_response$address
    return(data.frame(
      Address1 = ifelse(!is.null(address$streetAddress), address$streetAddress, ""),
      Address2 = ifelse(!is.null(address$secondaryAddress), address$secondaryAddress, ""),
      City = ifelse(!is.null(address$city), address$city, ""),
      State = ifelse(!is.null(address$state), address$state, ""),
      Zip5 = ifelse(!is.null(address$ZIPCode), address$ZIPCode, ""),
      Zip4 = ifelse(!is.null(address$ZIPPlus4), address$ZIPPlus4, "")
    ))
  } else {
    warning("No valid addresses were found by the USPS API.")
    return(NULL)
  }
}



validate_geocoordinates_google <- function(api_key, address) {
  #
  # THIS ONE IS NOT FUNCTIONING. REQUIRES A PROJECT: https://developers.google.com/maps/documentation/geocoding/start
  #
  #' @description
  #' This function validates an address by querying the Google Geocoding API and 
  #' retrieves the geographical coordinates (latitude and longitude) of the address. 
  #' The source of the data is the Google Geocoding API:
  #' https://developers.google.com/maps/documentation/geocoding/start
  #' 
  #' @param api_key Your Google API key.
  #' @param address A character string representing the full address in the format 
  #'                "Street, Address Line 2 (optional), City, State ZIP".
  #' 
  #' @return A data frame containing the validated formatted address, latitude, 
  #'         and longitude. If the address is invalid or no matches are found, 
  #'         it returns with a warning.
  #' 
  #' @examples
  #' api_key <- "<your_google_api_key>"  # Replace with your Google API Key
  #' validate_geocoordinates_google(api_key, "1600 Amphitheatre Parkway, Mountain View, CA 94043")
  #' validate_geocoordinates_google(api_key, "200 MARYLAND AVE NE, STE 302, WASHINGTON, DC 20002-5724")
  
  # Construct the request URL for the Google Geocoding API
  base_url <- "https://maps.googleapis.com/maps/api/geocode/json"
  request_url <- paste0(
    base_url,
    "?address=", URLencode(address),
    "&key=", api_key
  )
  
  # Send GET request to the API
  response <- GET(request_url)
  
  # Check for a successful response
  if (status_code(response) != 200) {
    warning("Failed to fetch data from Google Geocoding API. Status code: ", status_code(response))
    return(NULL)
  }
  
  # Parse the JSON response
  content_text <- content(response, "text", encoding = "UTF-8")
  parsed_response <- fromJSON(content_text)
  
  # Validate response structure
  if (parsed_response$status != "OK") {
    warning("Google Geocoding API error: Address not found or no matches: ", parsed_response$status)
    return(NULL)
  }
  
  # Get the first result
  result <- parsed_response$results[[1]]
  
  # Extract latitude and longitude
  lat <- result$geometry$location$lat
  lon <- result$geometry$location$lng
  
  # Extract formatted address
  formatted_address <- result$formatted_address
  
  # Return the results including coordinates
  result_data <- data.frame(
    Address = formatted_address,
    Latitude = lat,
    Longitude = lon
  )
  
  return(result_data)
}



find_date_combinations <- function(date_range, gap) {
  #' @description
  #' This function finds combinations of dates with a fixed gap (in years) from 
  #' a given range of dates.
  #'
  #' @param date_range A vector of date strings in the format "YYYY".
  #' @param gap An integer representing the fixed gap in years.
  #'
  #' @return A data frame containing combinations of start and end dates with 
  #'         the specified gap.

  
  # Initialize an empty list to store combinations
  combinations <- list()
  
  # Iterate through the dates.
  for (i in seq_along(date_range)) {
    for (j in seq_along(date_range)) {
      
      # Check if the gap between dates is equal to the specified gap in years.
      # Only keep possible matches that are non-redundant.
      if ( (as.numeric(date_range[i] - date_range[j]) > 0) & (as.numeric(date_range[i] - date_range[j]) == gap) ) {
        combinations <- append(combinations, list(c(date_range[j], date_range[i])))
      }
    }
  }
  
  # Convert list to data frame and set column names
  combinations_df <- do.call(rbind, combinations) %>% as.data.frame() %>%
    `colnames<-`(c("startDate", "endDate"))
  
  return(combinations_df)
}



fill_zeros <- function(input_string) {
  #' @description
  #' This function replaces occurrences of one, two, or three zeros between 
  #' ones in a binary string with ones, while maintaining the original 
  #' string length.
  #'
  #' @param input_string A character string representing the binary input.
  #'
  #' @return A character string with specified zeros replaced by ones.
  
  
  # Define the patterns to match one, two, or three zeros between ones and
  # the corresponding replacements to maintain original length (1s replacing 0s)
  patterns <- c("10{1}1", "10{2}1", "10{3}1")
  replacements <- c("111", "1111", "11111")
  
  # Initialize the result_string with the input string.
  result_string <- input_string
  prev_string <- ""
  
  # Repeat replacement until no more changes occur.
  while (result_string != prev_string) {
    prev_string <- result_string
    # Iterate over each pattern and replacement.
    for (i in seq_along(patterns)) {
      # Replace pattern with the corresponding replacement.
      result_string <- str_replace_all(result_string, patterns[i], replacements[i])
    }
  }
  
  return(result_string)
}



fill_zeros_with_progress <- function(pb, .data) {
  #' @description
  #' This fills in zeros between patterns of ones, updates the progress 
  #' bar, and applies the `fill_zeros` function to the group.
  #'
  #' @param pb A `progress_bar` object from the `progress` package.
  #' @param .data A data frame representing the current group to be processed.
  #'
  #' @return A data frame with the results of `check_all_counts_0_or_1` applied to the group.
  
  # Update the progress bar by one tick
  pb$tick()
  
  # Apply the check_all_counts_0_or_1 function to the current group
  fill_zeros(.data)
}



check_all_counts_0_or_1 <- function(data) {
  #' @description
  #' This function summarizes the selected year columns (if the columns are 
  #' preceded by a "20") by summing them, and checks if all counts in these 
  #' columns are either 0 or 1.
  #'
  #' @param data A data frame containing the year columns.
  #'
  #' @return A data frame with a new column `all_counts_0_or_1` indicating whether all counts
  #' in the selected year columns are 0 or 1 for each row.
  
  data %>%
    # Summarize the selected year columns (columns starting with "20") by 
    # summing them.
    summarise(across(starts_with("20"), sum)) %>%
    # Ensure subsequent operations are performed row-wise.
    rowwise() %>%
    # Add a new column `all_counts_0_or_1`; TRUE if all counts are 0 or 1, 
    # FALSE otherwise.
    mutate(all_counts_0_or_1 = all(across(starts_with("20"), ~ . %in% c(0, 1)))) %>%
    # Remove row-wise grouping to avoid unintentional side effects.
    ungroup()
}



process_with_progress <- function(pb, .data, func) {
  #' @description
  #' This function processes a group within a data frame while updating 
  #' the progress bar and applying a specified function to the group.
  #'
  #' @param pb A progress bar object from the `progress` package.
  #' @param .data A data frame representing the current group to be processed.
  #' @param func A function to be applied to the current group.
  #'
  #' @return The processed result from applying the function.
  
  # Update the progress bar.
  pb$tick()
  
  # Apply the specified function and return the result.
  result <- func(.data)
  
  return(result)
}



process_with_progress_txt <- function(pb, .data, func, i) {
  #' @description
  #' This function processes a group within a data frame, updates the base R 
  #' progress bar, and applies a specified function to the group.
  #'
  #' @param pb A progress bar object from base R.
  #' @param .data A data frame representing the current group to be processed.
  #' @param func A function to be applied to the current group.
  #' @param i The index of the current progress.
  #'
  #' @return A data frame with the results of the specified function applied to t
  #'         he group.
  
  # Update the progress bar by one tick
  setTxtProgressBar(pb, i)
  
  # Apply the specified function to the current group and return the result
  func(.data)
}



is_acronym_or_city <- function(string) {
  #' @description
  #' This function checks if a given string is an acronym or a city name using
  #' regular expressions for acronyms and a US city database for city names.
  #' Source: https://simplemaps.com/data/us-cities
  #'
  #' @param string A character string to be checked.
  #'
  #' @return A character string indicating whether the input is an "Acronym", 
  #'         "City", or "Unknown".
  
  # US city data is loaded from a CSV file outside the function.
  us_cities <- us_cities %>%
    mutate(city = tolower(city))
  
  # Check if the string is a city
  is_city <- function(s) {
    return(tolower(s) %in% tolower(us_cities$city))
  }
  
  # Check if the string is an acronym
  is_acronym <- function(s) {
    # Define conditions for identifying acronyms
    is_short <- nchar(s) <= 4 && !grepl("\\s", s)         # Short length and no spaces
    has_periods <- grepl("^[A-Z](\\.[A-Z])+\\.?$", s)  # Matches patterns like U.S.A. or A.B.C.
    not_word <- !s %in% c("ST", "AVE", "BLVD", "RD", "DR", "LN", "CT")  # Common address terms
    
    return((is_short || has_periods) && not_word)
  }
  
  # Determine if the input string is a city or an acronym
  if (is_city(string)) {
    return("City")
  } else if (is_acronym(string)) {
    return("Acronym")
  } else {
    return("Unknown")
  }
}



capture_warnings <- function(expr) {
  #' @description
  #' This function evaluates an expression and captures any warnings generated 
  #' during the evaluation.
  #'
  #' @param expr An expression to be evaluated.
  #' 
  #' @return A list containing the result of the evaluated expression and a 
  #'         list of captured warnings.
  #'
  #' @note quote(function(parameters)): The quote function captures the
  #'       expression function(parameters) as is, to be evaluated later.
  #'
  #' @examples
  #' # Example usage with a custom function that generates warnings
  #' my_function <- function(x) {
  #'   if (x < 0) {
  #'     warning("x is negative!")
  #'   }
  #'   if (x == 0) {
  #'     warning("x is zero!")
  #'   }
  #'   return(x + 1)
  #' }
  #' 
  #' # Capture warnings and result
  #' result <- capture_warnings(quote(my_function(-1)))
  #' print(result$result)    # Output: 0
  #' print(result$warnings)  # Output: "x is negative!"
  #' 
  #' result <- capture_warnings(quote(my_function(0)))
  #' print(result$result)    # Output: 1
  #' print(result$warnings)  # Output: "x is zero!"
  
  
  # Initialize an empty list to store warnings
  warnings <- list()
  
  # Use withCallingHandlers to evaluate the expression and capture warnings
  result <- withCallingHandlers(
    expr = {
      eval(expr)  # Evaluate the expression
    },
    warning = function(w) {
      # Append the warning message to the warnings list
      warnings <<- c(warnings, conditionMessage(w))
      # Suppress the warning
      invokeRestart("muffleWarning")
    }
  )
  
  # Return a list containing the result and captured warnings
  list(result = result, warnings = warnings)
}



get_census_tract_geocoder <- function(address_str, vintage = "Census2020_Current", benchmark = "Public_AR_Current") {
  #' @description
  #' This function takes an address string, splits it into components, and uses 
  #' the Census Geocoder API to get census tract information for that address.
  #' 
  #' Source: https://geocoding.geo.census.gov/geocoder/
  #'
  #' @param address_str A character string representing the address. The format 
  #'                    should be "Street, Address Line 2, City, State, ZIP" 
  #'                    (Address Line 2 is optional).
  #'                    
  #' @param vintage A character string representing the census vintage. 
  #'                Defaults to "Census2020_Current". 
  #'                
  #'                Census2000_Current: Most recent data from the 2000 Census.
  #'                Census2010_Current: Most recent data from the 2010 Census.
  #'                Census2020_Current: Most recent data from the 2020 Census.
  #'                Current_Current: Most recent data available, including 
  #'                                 updates beyond major census years.
  #'                
  #'                
  #'                ACS2018_Current: Data from the 2018 American Community Survey.
  #'                ACS2019_Current: Data from the 2019 American Community Survey.
  #'                etc.
  #'                
  #' @param benchmark A character string representing the benchmark data. 
  #'                  Defaults to "Public_AR_Current".
  #' 
  #' @return A data frame containing the original address, census vintage,
  #'         census tract number, county name, and state.
  
  
  # Regular expression to handle optional Address Line 2 and optional ZIP code extension
  address_pattern <- "^(.*?),\\s*(.*?,\\s*)?(.*),\\s*([A-Z]{2})\\s*(\\d{5}(?:-\\d{4})?)$"
  
  # Match and extract address components using the regular expression
  address_match <- regmatches(address_str, regexec(address_pattern, address_str))
  
  # Check if the match is successful and extract components
  if (length(address_match) == 1 && length(address_match[[1]]) == 6) {
    street <- address_match[[1]][2]
    address_line_2 <- address_match[[1]][3]
    city <- address_match[[1]][4]
    state <- address_match[[1]][5]
    zip <- address_match[[1]][6]
  } else {
    stop("Invalid address format. Please provide address in format: 'Street, Address Line 2 (optional), City, State ZIP'.")
  }
  
  # Combine street and address line 2 into a single line for the API request
  full_street <- paste(street, address_line_2, sep = ", ")
  
  # US Census Bureau's Geocoder API information.
  base_url <- "https://geocoding.geo.census.gov/geocoder/geographies/address"
  # Construct the request URL with the provided address components
  request_url <- paste0(
    base_url, "?format=json&benchmark=", benchmark, "&vintage=", vintage,
    "&street=", URLencode(full_street),
    "&city=", URLencode(city),
    "&state=", URLencode(state),
    "&zip=", URLencode(zip)
  )
  
  # Get the response from the API
  response <- GET(request_url)
  
  # Check for a successful response.
  if (status_code(response) != 200) {
    # Stop execution and display an error message if the API call fails.
    message(
      "Failed to fetch data from Census Geocoder API. Status code: ", status_code(response),
      ". Response: ", content(response, "text", encoding = "UTF-8")
    )
    return(NULL)
  }
  
  # Parse the JSON response.
  content_text <- content(response, "text", encoding = "UTF-8")
  parsed_response <- fromJSON(content_text, simplifyDataFrame = FALSE)
  
  # Check if there are address matches in the response.
  if (!("result" %in% names(parsed_response)) || 
      !("addressMatches" %in% names(parsed_response$result)) ||
      length(parsed_response$result$addressMatches) == 0) {
    message("Invalid address: Address not found or no matches.")
    return(NULL)
  }
  
  address_matches <- parsed_response$result$addressMatches
  address_match <- address_matches[[1]]
  
  # Access the 'geographies' part of the response.
  if (!("geographies" %in% names(address_match))) {
    message("Invalid address: 'geographies' not found in response.")
    return()
  }
  
  geographies <- address_match$geographies
  
  # Extract Census Tracts within geographies.
  census_tracts <- geographies$`Census Tracts`
  if (is.null(census_tracts) || length(census_tracts) == 0) {
    message("Invalid address: Census Tracts not found in response.")
    return(NULL)
  }
  
  census_tract <- census_tracts[[1]]$TRACT
  if (is.null(census_tract)) {
    message("Invalid address: Census Tract not found.")
    return(NULL)
  }
  
  # Extract Counties within geographies.
  counties <- geographies$Counties
  if (is.null(counties) || length(counties) == 0) {
    message("Invalid address: Counties not found in response.")
    return(NULL)
  }
  
  county <- counties[[1]]$NAME
  if (is.null(county)) {
    message("Invalid address: County not found.")
    return(NULL)
  }
  
  # Return and print the results as a data frame.
  result <- data.frame(
    Address = address_str,         # Original address string
    Census = vintage,              # The census vintage used
    Census_Tract = census_tract,   # The census tract number
    County = county,               # The county name
    State = state                  # The state
  )
  
  result
}



get_tracts_for_year_and_state <- function(year, state) {
  #' @description
  #' This inner function used by find_census_info_tidycensus() retrieves census 
  #' tract data for a specified year.
  #' 
  #' @param year The year for which to retrieve census tract data.
  #' @return An sf object containing the census tract geometries.
  #tracts_sf <- suppressMessages(tracts(state = state, year = year, class = "sf"))
  
  tracts_sf <- tryCatch({
    suppressMessages(tracts(state = state, year = year, class = "sf"))
  }, error = function(e) {
    warning(paste("Error retrieving tracts for year:", year, "and state:", state, "-", e$message))
    return(NULL)  # Return NULL if there's an error
  })
  
  if (is.null(tracts_sf)) {
    return(NULL)  # Return early if tracts_sf is NULL
  }
  
  # Adjust column names for different years
  if (year == 2000) {
    tracts_sf <- tracts_sf %>%
      mutate(GEOID = CTIDFP00,
             STATEFP = STATEFP00,
             COUNTYFP = COUNTYFP00,
             NAMELSAD = NAMELSAD00)
  } else if (year == 2010) {
    tracts_sf <- tracts_sf %>%
      mutate(GEOID = GEOID10,
             STATEFP = STATEFP10,
             COUNTYFP = COUNTYFP10,
             NAMELSAD = NAMELSAD10)
  } else if (year == 2020) {
    tracts_sf <- tracts_sf %>%
      mutate(GEOID = GEOID,
             STATEFP = STATEFP,
             COUNTYFP = COUNTYFP,
             NAMELSAD = NAMELSAD)
  }
  
  if (is.null(st_crs(tracts_sf))) {
    warning(paste("CRS not found for tracts of year:", year, "and state:", state))
    return(NULL)
  }
  
  return(tracts_sf)
}



find_census_info_tidycensus <- function(address_data, years) {
  #' @description
  #' This function finds the census tract, county, and state information for 
  #' specified addresses across the 2000, 2010, and 2020 decennial census periods.
  #' Uses the tidycensus package to reference the Census Bureau's database.
  #' 
  #' tidycensus: https://walker-data.com/tidycensus/index.html
  #'
  #' @param address_data A data frame containing the address information with 
  #'                     columns 'address', 'lat', and 'lon'.
  #'                     
  #' @param years A vector of years for which to retrieve census information. 
  #'              Supports 2000, 2010, and 2020.
  #' 
  #' @return A data frame with address, tract, county, state, and year 
  #'         information for each specified address.
  
  if (any(is.na(address_data$lat)) | any(is.na(address_data$lon))) {
    # Filter out rows with missing coordinates
    address_data <- address_data %>% filter(!is.na(lat) & !is.na(lon))
    message("NA's detected in the geolocation columns, lon and lat.")
  }
  
  # Initialize an empty list to store results for each year
  all_results <- list()
  
  # Initialize counter
  counter <- 1
  
  # Loop through each year and state to process the data
  for (year in years) {
    # Load state boundaries to dynamically determine the state for each address
    states_sf <- st_as_sf(tigris::states(year = year, class = "sf"))
    
    # Append state information to address_data based on latitude and longitude
    address_data_sf <- st_as_sf(address_data, coords = c("lon", "lat"), crs = 4326)
    address_data_sf <- st_transform(address_data_sf, st_crs(states_sf))
    address_data_sf <- st_join(address_data_sf, states_sf, join = st_within)
    
    # Extract state FIPS codes
    address_data <- address_data %>% mutate(state_fips = address_data_sf$STATEFP)
    
    for (state in unique(address_data$state_fips)) {
      # Check if state FIPS code is valid
      if (is.na(state)) {
        warning(paste("Invalid state FIPS code detected:", state))
        next  # Skip this iteration if state FIPS code is invalid
      }
      
      # Retrieve tracts data for the current year and state
      tracts_sf <- get_tracts_for_year_and_state(year, state)
      
      if (is.null(tracts_sf)) {
        next  # Skip processing if tracts_sf is NULL
      }
      
      # Filter addresses for the current state
      addresses_in_state <- filter(address_data, state_fips == state)
      addresses_in_state_sf <- st_as_sf(addresses_in_state, coords = c("lon", "lat"), crs = 4326)
      
      # Transform geocoded points to match the tracts CRS
      addresses_in_state_sf <- st_transform(addresses_in_state_sf, st_crs(tracts_sf))
      
      # Perform spatial join to determine which tract each point is in
      joined_sf <- st_join(addresses_in_state_sf, tracts_sf, join = st_within)
      
      # Select relevant columns for output and add the year
      metadata_cols <- setdiff(names(address_data), c("lat", "lon", "address", "state_fips"))
      result <- joined_sf %>%
        select(any_of(c(metadata_cols, "address", "GEOID", "NAMELSAD", "STATEFP", "COUNTYFP"))) %>%
        rename(
          State = STATEFP,
          County = COUNTYFP,
          Tract = GEOID,
          Tract_Name = NAMELSAD
        ) %>%
        mutate(Year = year)
      
      # Store the result
      all_results[[paste0(year, "_", state)]] <- result
      
      # Update the progress bar
      setTxtProgressBar(pb, counter)
      counter <- counter + 1
    }
  }
  
  # Combine results from all years and states
  final_result <- bind_rows(all_results)
  
  # Close the progress bar
  if (!is.null(pb)) {
    close(pb)
  }
  
  return(final_result)
}

# Example usage
address_data <- data.frame(
  abi = c(478545),
  area = c("Area #1"),
  address = c("77 BROADWAY, CHELSEA, MA 02150-2607"),
  lat = c(42.38799),
  lon = c(-71.04257)
)



decide_reference <- function(start_year, end_year, method = "ending") {
  #' @description
  #' This function finds the relevant reference period for a given date range
  #' based on two methods: using the ending year to decide, or using the
  #' reference period that spans the given date range the most.
  #'
  #' @param start_year A numeric value representing the start year of the date 
  #'                   range.
  #'                   
  #' @param end_year A numeric value representing the end year of the date range.
  #' 
  #' @param method A character string indicating the method to use: "ending" to
  #'               use the ending year to decide the reference, or "spanning"
  #'               to use the reference that the span of dates covers the most.
  #'               Default is "ending".
  #'
  #' @return A character string indicating the reference period (one of
  #'         "2000-2009", "2010-2019", "2020-2029").
  #'

  
  # Define reference periods as numeric ranges.
  reference_periods <- list(
    "2000 Census" = c(2000, 2009),
    "2010 Census" = c(2010, 2019),
    "2020 Census" = c(2020, 2029)
  )
  
  # Method 1: Use the ending year to decide the reference.
  if (method == "ending") {
    ref <- end_year
  }
  
  # Method 2: Use the reference that the span of years covers the most.
  else if (method == "spanning") {
    # Calculate the overlap with each period and choose the one with the 
    # longest overlap.
    overlaps <- sapply(reference_periods, function(period) {
      overlap_start <- max(start_year, period[1])
      overlap_end <- min(end_year, period[2])
      overlap_years <- overlap_end - overlap_start + 1
      return(max(overlap_years, 0))
    })
    ref <- names(which.max(overlaps))
  }
  
  # Determine appropriate reference period based on the decided reference year.
  selected_reference <- NULL
  for (period in names(reference_periods)) {
    period_range <- reference_periods[[period]]
    if (ref >= period_range[1] & ref <= period_range[2]) {
      selected_reference <- period
      break
    }
  }
  
  return(selected_reference)
}



replace_zero_ends <- function(s, leading = FALSE, trailing = TRUE) {
  #' @description
  #' Replace leading or trailing one to three 0's with 1's. For robustness, 
  #' the function also allows the user to toggle filling in leading or trailing 
  #' zeros independently. By default, it fills in the trailing ones only.
  #' 
  #' @param s The entire span of responses available in the dataset compiled
  #'          as one character string.
  #' 
  #' @return The same character string with trailing 0's near the end replaced
  #'         with a 1.
  
  
  # Use gsub to replace the trailing 0's with 1's of the specified pattern.
  if( trailing == TRUE ) {
    s <- gsub("10(?=$|1)", "11", s, perl = TRUE)
    s <- gsub("100(?=$|1)", "111", s, perl = TRUE)
    s <- gsub("1000(?=$|1)", "1111", s, perl = TRUE)
  }
  
  # Replace leading 0s after the first 1.
  if( leading == TRUE ) {
    s <- gsub("(?<=1)000(?=0)", "111", s, perl = TRUE)
    s <- gsub("(?<=1)00(?=0)", "11", s, perl = TRUE)
    s <- gsub("(?<=1)0(?=0)", "1", s, perl = TRUE)
    
  }
  
  return(s)
}



calculate_persistence <- function(subset_flag, full_flag) {
  #' @description
  #' This function calculates the persistence ratio of a church being open
  #' in a subset period compared to the full span of time recorded.
  #'
  #' @param subset_flag A character string representing the open/closed status 
  #'                    of the organization for the subset period.
  #' 
  #' @param full_flag A character string representing the open/closed status of 
  #'                  the organization for the full period.
  #'
  #' @return A numeric value representing the persistence ratio. Returns NA if the 
  #'         full period consists entirely of 0's.
  
  
  # If the full flag is all zeros, return 0
  if (nchar(gsub("0", "", full_flag)) == 0) {
    return(0)
  }
  
  # Calculate densities of 1's in the full period and the subset period
  full_density <- nchar(gsub("0", "", full_flag)) / nchar(full_flag)
  subset_density <- nchar(gsub("0", "", subset_flag)) / nchar(subset_flag)
  
  # Handle cases where the subset_flag is all zeros
  if (subset_density == 0) {
    return(0)
  }
  
  # Adjust the calculation for a skewed result
  if (subset_density > 0 && subset_density == nchar(subset_flag)) {
    return(subset_density / total_open_years)
  }
  
  # Scale by the proportion of the period lengths
  scaled_short_open_years <- (subset_density / nchar(subset_flag)) * nchar(full_flag)
  
  # Calculate persistence ratio by comparing densities
  persistence_ratio <- scaled_short_open_years * full_density
  
  return(persistence_ratio)
}



mutate_with_progress <- function(df, cols_to_convert, grouping_cols, conversion_func, pb) {
  #' @description
  #' This function processes each group within a data frame by converting 
  #' specified columns while updating the progress bar to track completion. 
  #' It ensures all values above zero are converted to 1, and zeroes remain 
  #' unchanged.
  #'
  #' @param df A data frame to be processed.
  #' 
  #' @param cols_to_convert A vector of column names specifying which columns 
  #'                        to convert.
  #'                        
  #' @param grouping_cols A vector of column names used to group the data frame.
  #' @param conversion_func A function to apply the conversion to the specified 
  #'                        columns.
  #' 
  #' @param pb A text progress bar object from the 'progress' package.
  #'
  #' @return A processed data frame with the specified columns converted 
  #'         and the progress bar updated.
  
  
  # Internal function to update progress bar and apply conversion
  process_with_progress_grp <- function(group, group_idx) {
    process_with_progress_txt(pb, group, function(df) {
      conversion_func(df, cols_to_convert)
    }, i = group_idx)
  }
  
  # Split the data frame into groups and initialize progress tracking
  grouped_df <- group_split(df, across(all_of(grouping_cols)))
  
  # Apply the conversion and progress tracking to each group
  results <- map_dfr(seq_along(grouped_df), ~ process_with_progress_grp(grouped_df[[.]], .x))
  
  return(results)
}







