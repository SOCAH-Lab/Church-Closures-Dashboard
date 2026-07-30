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
##    1. find_date_combinations: This function finds combinations of dates with 
##       a fixed gap (in years) from a given range of dates.
##
##    2. fill_zeros: This function replaces occurrences of one, two, or three 
##       zeros between ones in a binary string with ones, while maintaining 
##       the original string length.
##
##    3. fill_zeros_with_progress: This fills in zeros between patterns of ones, 
##       updates the progress bar, and applies the `fill_zeros` function to the 
##       group.
##
##    4. is_acronym_or_city: This function checks if a given string is an 
##       acronym or a city name using regular expressions for acronyms and a US 
##       city database for city names.
##
##    5. decide_reference: This function finds the relevant reference period for 
##       a given date range based on two methods: using the ending year to 
##       decide, or using the reference period that spans the given date range 
##       the most.
##
##    6. replace_trailing_zeros: To calculate the persistence lookahead 
##       correctly for subsets with an end date near the last date recorded in 
##       the dataset, we arbitrarily add 1's to replace the last one to three 
##       0's. These are not kept in the dataset, they are only a temporary 
##       measure in an effort to not distort the result.
## 
##    7. calculate_persistence: This function calculates the persistence ratio 
##       of a church being open in a subset period compared to the full span of 
##       time recorded.
##

## ----------------------------------------------------------------
## FUNCTIONS

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










