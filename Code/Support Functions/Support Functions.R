## ----------------------------------------------------------------
## Define the coding parameters used in the environment.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: April 8th, 2026
## 
## Description: All custom functions used in the raw data cleaning
##              and preparation process. Much of this content was written
##              with the assistance of Yale's AI Clarity.
##
## Functions
##
##    1. validate_geocoordinates_geoCoder: This function validates an address by
##       querying the Census Geocoder API and retrieves the geographical 
##       coordinates (latitude and longitude) of the address. The source of the
##       data is the Census Geocoder API.
##       Source: https://geocoding.geo.census.gov/geocoder/
##
## ARCHIVE. validate_geocoordinates_google: This function validates an address by 
##       querying the Google Geocoding API and retrieves the geographical 
##       coordinates (latitude and longitude) of the address. The source of the 
##       data is the Google Geocoding API: 
##       https://developers.google.com/maps/documentation/geocoding/start
## 
##    2. find_date_combinations: This function finds combinations of dates with 
##       a fixed gap (in years) from a given range of dates.
##
##    3. fill_zeros: This function replaces occurrences of one, two, or three 
##       zeros between ones in a binary string with ones, while maintaining 
##       the original string length.
##
##    4. fill_zeros_with_progress: This fills in zeros between patterns of ones, 
##       updates the progress bar, and applies the `fill_zeros` function to the 
##       group.
##
##
##
##
##    5. is_acronym_or_city: This function checks if a given string is an 
##       acronym or a city name using regular expressions for acronyms and a US 
##       city database for city names.
##
##    6. get_census_tract_geocoder: This function takes an address string,
##       splits it  into components, and uses the Census Geocoder API to get
##       census tract information for that address.
##       Source: https://geocoding.geo.census.gov/geocoder/
##
##    7. get_tracts_for_year: This inner function used by 
##       find_census_info_tidycensus() retrieves census tract data for a 
##       specified year.
##
##    8. find_census_info_tidycensus: This function finds the census tract, 
##       county, and state information for specified addresses across the 2000, 
##       2010, and 2020 decennial census periods. Uses the tidycensus package to 
##       reference the Census Bureau's database.
##       
##       tidycensus: https://walker-data.com/tidycensus/index.html
##
##    9. decide_reference: This function finds the relevant reference period for 
##       a given date range based on two methods: using the ending year to 
##       decide, or using the reference period that spans the given date range 
##       the most.
##
##   10. replace_trailing_zeros: To calculate the persistence lookahead 
##       correctly for subsets with an end date near the last date recorded in 
##       the dataset, we arbitrarily add 1's to replace the last one to three 
##       0's. These are not kept in the dataset, they are only a temporary 
##       measure in an effort to not distort the result.
## 
##   11. calculate_persistence: This function calculates the persistence ratio 
##       of a church being open in a subset period compared to the full span of 
##       time recorded.
##

## ----------------------------------------------------------------
## FUNCTIONS



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
  #' validate_geocoordinates_geoCoder("1600 Amphitheatre Parkway, Mountain View, CA 94043")
  #' validate_geocoordinates_geoCoder("200 MARYLAND AVE NE, STE 302, WASHINGTON, DC 20002-5724")
  
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




validate_geocoordinates_google <- function(api_key, address) {
  # ARCHIVE THIS FUNCTION
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










