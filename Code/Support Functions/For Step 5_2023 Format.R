## ----------------------------------------------------------------
## Define functions used in the Step 5 script for the 2023 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 28th, 2026
## 
## Description: In addition to the general-purpose functions defined in another
##              script, the following functions are used to complete Step 5 of
##              the data cleaning and validation process, as identified through
##              exploratory data analysis. 
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions
## 
##    1. get_census_tract_geocoder: This function takes an address string,
##       splits it  into components, and uses the Census Geocoder API to get
##       census tract information for that address.
## 
##       Source: https://geocoding.geo.census.gov/geocoder/
## 
##    2. get_tracts_for_year_and_state: Helper function used by 
##       `find_census_geographies_sf()` to fetch TIGER/Line census tract 
##       polygons for a given vintage year and state, and standardize key 
##       identifier columns (GEOID, STATEFP, COUNTYFP, NAMELSAD) across vintages
##       for downstream spatial joins.
## 
##    3. find_census_geographies_sf: For each requested vintage year, assign 
##       state/county/tract identifiers to geocoded address points (lat/lon) 
##       by spatially joining points to TIGER/Line state and tract boundary 
##       polygons.

## ----------------------------------------------------------------
## FUNCTIONS

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
  #' Helper function used by find_census_geographies_sf() to fetch TIGER/Line
  #' census tract polygons for a given vintage year and state, and standardize
  #' key identifier columns (GEOID, STATEFP, COUNTYFP, NAMELSAD) across vintages
  #' for downstream spatial joins.
  #'
  #' @param year The vintage year for which to retrieve tract boundary polygons
  #'             (e.g., 2000, 2010, 2020).
  #' @param state A state identifier passed to tigris::tracts() (e.g., state FIPS
  #'              code) to limit tract retrieval to that state.
  #'
  #' @return An sf object containing tract geometries for the requested year/state,
  #'         with standardized columns GEOID, STATEFP, COUNTYFP, and NAMELSAD.
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




find_census_geographies_sf <- function(address_data, years) {
  #' @description
  #' For each requested vintage year, assign state/county/tract identifiers to
  #' geocoded address points (lat/lon) by spatially joining points to TIGER/Line
  #' state and tract boundary polygons.
  #'
  #' @param address_data A data frame containing the address information with
  #'                     columns 'address', 'lat', and 'lon' (lon/lat in WGS84).
  #'
  #' @param years A vector of years for which to retrieve census geography
  #'              information (e.g., 2000, 2010, 2020).
  #'
  #' @return An (sf) data frame with address + metadata columns and the matched
  #'         tract GEOID, tract name, state FIPS, county FIPS, and year.
  
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
    # Load state polygons for the given year and use them to assign each point a state
    states_sf <- st_as_sf(tigris::states(year = year, class = "sf"))
    
    # Convert lon/lat to point geometry, align CRS to the state polygons, then spatially join points -> states
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
      
      # Retrieve tract polygons for this year/state (standardizing key columns inside the helper)
      tracts_sf <- get_tracts_for_year_and_state(year, state)
      
      if (is.null(tracts_sf)) {
        next  # Skip processing if tracts_sf is NULL
      }
      
      # Filter addresses for the current state
      addresses_in_state <- filter(address_data, state_fips == state)
      addresses_in_state_sf <- st_as_sf(addresses_in_state, coords = c("lon", "lat"), crs = 4326)
      
      # Transform point geometry to match the tract polygon CRS
      addresses_in_state_sf <- st_transform(addresses_in_state_sf, st_crs(tracts_sf))
      
      # Spatially join points -> tracts to identify the containing tract polygon for each point
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







