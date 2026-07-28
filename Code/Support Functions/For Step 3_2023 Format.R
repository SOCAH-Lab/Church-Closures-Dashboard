## ----------------------------------------------------------------
## Define functions used in the Step 3 script for the 2023 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 24th, 2026
## 
## Description: In addition to the general-purpose functions defined in another
##              script, the following functions are used to complete Step 3 of
##              the data cleaning and validation process, as identified through
##              exploratory data analysis.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions
##
##    1. validate_geocoordinates_geoCoder: This function validates an address by
##       querying the Census Geocoder API and retrieves the geographical 
##       coordinates (latitude and longitude) of the address. The source of the
##       data is the Census Geocoder API.
## 
##       Source: https://geocoding.geo.census.gov/geocoder/


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







