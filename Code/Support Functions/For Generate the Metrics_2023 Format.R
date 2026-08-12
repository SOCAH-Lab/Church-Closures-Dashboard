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
##    2. decide_reference: This function finds the relevant reference period for 
##       a given date range based on two methods: using the ending year to 
##       decide, or using the reference period that spans the given date range 
##       the most.
##
##    3. fill_zeros: This function replaces occurrences of one, two, or three 
##       zeros between ones in a binary string with ones, while maintaining 
##       the original string length.
##
##    4. fill_zeros_with_progress: This fills in zeros between patterns of ones, 
##       updates the progress bar, and applies the `fill_zeros` function to the 
##       group.
## 
##    5. add_fips_codes: Add FIPS Codes to a Dataframe. Merges a dataframe with 
##       a ZIP-FIPS mapping to append FIPS codes, then duplicates each row for 
##       each decennial census year (2000, 2010, 2020).
## 
##    6. get_persistence: This function calculates the persistence of churches 
##       over a specified period. It uses either a lookahead method or a ratio 
##       method to determine the persistence.
## 
##    7. count_closures: This function counts the number of churches that close 
##       and reopen within a specified period. A closure is defined as any 
##       period that starts with a 1 and is followed by at least four 0's. A 
##       reopening is defined as at least two 1's (11 or 101) followed by a 
##       period of at least four 0's.
## 
##    9. replace_zero_ends: Replace leading or trailing one to three 0's with 
##       1's. For robustness, the function also allows the user to toggle 
##       filling in leading or trailing zeros independently. By default, it 
##       fills in the trailing ones only.
## 
##    8. calculate_persistence: This function calculates the persistence ratio 
##       of a church being open in a subset period compared to the full span of 
##       time recorded.
## 
##    9. fips_combination_exists: This function verifies the existence of a 
##       state and county FIPS combination for a given decennial year. Depending 
##       on the specified level ("state" or "county"), it handles both state and 
##       county-level data. Errors during API calls are suppressed, and a 
##       logical value is returned indicating whether the combination exists.
## 
##   10. fetch_population_data: This function fetches unique population data for 
##       given combinations of census year, state FIPS, and county FIPS. If 
##       data doesn't exist for the given year, it fetches from the nearest 
##       available decennial year. It handles both state and county levels, 
##       ensuring no invalid NA values are passed when not required.
## 
##   11. calculate_closure_rates: This function calculates closure rates per 
##       10,000 persons using pre-fetched population data, handling both county 
##       and state levels dynamically, incorporating progress tracking. The 
##       function can handle missing populations by setting them to NA or using 
##       alternative population data from the nearest decennial year.
## 
##   12. fips_combination_exists_sf: This function verifies the existence of 
##       land area data for a state and county FIPS combination for a given 
##       decennial year. Depending on the specified level ("state" or "county"), 
##       it handles both state and county-level data. Errors during API calls 
##       are suppressed, and a logical value is returned indicating whether the 
##       land area data exists.
##
##   13. fetch_sf_data: This function fetches unique land area data for given 
##       combinations of census year, state FIPS, and county FIPS. If data 
##       doesn't exist for the given year, it fetches from the nearest available 
##       decennial year. It handles both state and county levels, ensuring no 
##       invalid NA values are passed when not required.
##
##   14. calculate_closure_rates_per_sq_mile: This function calculates closure 
##       rates per square mile using pre-fetched population and land area data, 
##       handling both county and state levels dynamically, incorporating 
##       progress tracking. The function can handle missing populations and 
##       land areas by setting them to NA or using alternative data from the 
##       nearest decennial year.
## 
##   15. reorder_columns: Reorders the columns of a given data frame based on 
##       specified metric types and date ranges. The columns are first ordered 
##       by the provided metric types and then by date ranges within each metric 
##       type.
## 
##   16. combine_geocoding: This function combines geocoding information with 
##       decennial census data for both state and county levels. It iteratively 
##       processes each unique decennial period present in the input data and 
##       merges the corresponding geographic shape files with the data based on 
##       FIPS codes.

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




add_fips_codes <- function(df, zip_fips_mapping) {
  #' @description
  #' Add FIPS Codes to a Dataframe. Merges a dataframe with a ZIP-FIPS mapping 
  #' to append FIPS codes, then duplicates each row for each decennial census 
  #' year (2000, 2010, 2020).
  #'
  #' @param df A \code{data.frame} containing at least the columns \code{state}
  #'   and \code{zipcode}. The \code{state} column will be coerced to uppercase
  #'   and \code{zipcode} will be zero-padded to 5 digits.
  #' @param zip_fips_mapping A \code{data.frame} containing ZIP-to-FIPS mapping
  #'   data. Must include a \code{zipcode} column used as the join key.
  #'
  #' @return A \code{data.frame} derived from \code{df} with the following
  #'   modifications:
  #'   \itemize{
  #'     \item FIPS code columns appended via a left join on \code{zipcode}.
  #'     \item Rows duplicated for each decennial census year
  #'       (\code{2000}, \code{2010}, \code{2020}).
  #'     \item A new column \code{decennial_census} indicating the census year.
  #'   }
  #'
  #' @details
  #' The function performs a many-to-many left join, meaning a single ZIP code
  #' may match multiple FIPS codes. Each matched row is then replicated three
  #' times — once per decennial census year. Ensure that \code{zip_fips_mapping}
  #' is filtered or deduplicated beforehand if a many-to-many join is not desired.
  
  # Ensure the state column is uppercase and zip codes are padded with leading zeros
  df <- df %>%
    mutate(state = toupper(state), zipcode = sprintf("%05d", as.numeric(zipcode)))
  
  # Merge the dataframe with the ZIP-FIPS mapping data
  df <- df %>%
    left_join(zip_fips_mapping, by = "zipcode", relationship = "many-to-many")
  
  # Create a list of decennial census years
  years <- c(2000, 2010, 2020)
  
  # Duplicate the data for each decennial census year
  df <- df %>%
    slice(rep(1:n(), each = length(years))) %>%
    mutate(decennial_census = rep(years, times = n()/length(years)))
  
  return(df)
}




get_persistence <- function(dt, period_start, period_end, method = "ratio") {
  #' @description
  #' This function calculates the persistence of churches over a specified
  #' period. It uses either a lookahead method or a ratio method to determine
  #' the persistence.
  #'
  #' @param dt A data frame containing the yearly open/closed status of churches.
  #'           Column names cannot start with "20" unless they are the years
  #'           recorded.
  #' 
  #' @param period_start An integer representing the start year of the period.
  #' 
  #' @param period_end An integer representing the end year of the period.
  #' 
  #' @param method A character string specifying the method to use for persistence 
  #'               calculation. Defaults to "ratio". Options are "ratio" for 
  #'               calculating based on the density of 1's, or "lookahead" to 
  #'               handle trailing zeros.
  #'
  #' @return A data frame with the calculated persistence added as a new column. 
  #'         The column name reflects the period (e.g., "persistence_2001-2010").
  
  
  # Index the start and end dates and get the full length of time recorded.
  pos_start <- period_start - 1999L - 1
  pos_end <- period_end - 1999L - 1
  len_period <- period_end - period_start + 1
  fullnchar <- 21L
  
  # Force the ratio method if the ending date is the same as the last date recorded.
  if( method == "lookahead" & period_end == "2021" ) {
    method = "ratio"
    message("Persistence cannot be accurately calculated by the lookahead method when the end date is the same as the last date recorded. Switching to the alternative method.")
  }
  
  # Create a character string covering the span of dates to examine.
  year_columns <- as.character(seq(period_start, period_end))
  
  if( method == "lookahead" ) {
    dt <- dt %>% 
      rowwise() %>%
      # Compress the responses over the entire period reflected into one
      # string of 1's and 0's for each entry.
      mutate(
        fullString_ = paste(across(starts_with("20")), collapse = ""),
        # Only with a lookahead, artificially fill the trailing one to three 0's 
        # with a 1 so as to not under count persistence near the last date recorded.
        fullString_ = replace_zero_ends(fullString_),
        # Subset the fullString_ variable into the user-provided year range.
        subString_ = stringi::stri_sub(fullString_, pos_start, pos_end),
        # Only with a lookahead, include the subset following the  user-provided 
        # year range for persistence evaluation.
        subString_after = stringi::stri_sub(fullString_, pos_end + 1, fullnchar),
        # Calculate the persistence.
        persistence_ = as.integer((grepl("1", subString_) & grepl("1", subString_after)) | grepl("1$", subString_))
      ) %>%
      ungroup() %>% 
      # Clean-up the final table.
      select(-fullString_, -subString_, -subString_after) %>%
      rename_with(~ str_c("persistence_", period_start, "-", period_end), "persistence_") %>%
      as.data.frame()
    
  } else if( method == "ratio" ) {
    dt <- dt %>% 
      rowwise() %>%
      mutate(
        # Compress the responses over the entire period reflected into one
        # string of 1's and 0's for each entry.
        fullString_ = paste(across(starts_with("20")), collapse = ""),
        # Subset the fullString_ variable into the user-provided year range.
        subString_ = stringi::stri_sub(fullString_, pos_start, pos_end),
        # Calculate the persistence using the ratio method that weights importance
        # of 1's that are closer to the end of the string and contextualizes
        # the substring to the full string.
        persistence_ = {
          ratios <- mapply(calculate_persistence, subString_, fullString_)
          ifelse(is.na(ratios) | ratios < 1, 0, 1)
        }
      ) %>%
      ungroup() %>%
      # Clean-up the final table.
      select(-fullString_, -subString_) %>%
      rename_with(~ str_c("persistence_", period_start, "-", period_end), "persistence_") %>%
      as.data.frame()
    
  }
  
  return(dt)
}




count_closures <- function(dt, period_start, period_end) {
  #' @description
  #' This function counts the number of churches that close and reopen within a 
  #' specified period. A closure is defined as any period that starts with a 1 
  #' and is followed by at least four 0's. A reopening is defined as at least 
  #' two 1's (11 or 101) followed by a period of at least four 0's.
  #'
  #' @param dt A data frame containing the yearly open/closed status of 
  #'           organizations.
  #'           
  #' @param period_start An integer representing the start year of the period.
  #' @param period_end An integer representing the end year of the period.
  #'
  #' @return A data frame with the counts of closures and reopenings for the 
  #'         specified period.
  
  
  # Define the positions for substring extraction.
  pos_start <- period_start - 1999L - 1
  pos_end <- period_end - 1999L - 1
  
  dt <- dt %>%
    rowwise() %>%
    mutate(
      # Compress the responses over the entire period reflected into one
      # string of 1's and 0's for each entry.
      fullString_ = paste(across(starts_with("20")), collapse = ""),
      # Subset the fullString_ variable into the user-provided year range.
      subString_ = stringi::stri_sub(fullString_, pos_start, pos_end),
      # Count the single 1's surrounded by zeros.
      single_count_ = str_count(subString_, "(?<=0)10{3,}(?=0)"),
      # Count closures, defined to be at least four contiguous 0's. Subtract any 
      # over counts from standalone 1's that do not constitute as a reopening,
      # and force the values to be zero if negative due to standalone 1's.
      closure_count_ = pmax(str_count(subString_, "10{4,}") - single_count_, 0),
      # Force counts to be at most equals to closure_count_ and no less than 
      # closure_count_ - 1.
      reopening_count_ = ifelse(closure_count_ == 0, 0, pmax(pmin(closure_count_, str_count(subString_, "0{4,}(?=11|101)")), closure_count_ - 1))
    ) %>%
    ungroup() %>%
    # Clean-up the final table.
    select(-fullString_, -subString_, -single_count_) %>%
    rename_with(~ str_c("closure_count_", period_start, "-", period_end), "closure_count_") %>%
    rename_with(~ str_c("reopening_count_", period_start, "-", period_end), "reopening_count_") %>%
    as.data.frame()
  
  return(dt)
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



fips_combination_exists <- function(state_fips, county_fips, year, level) {
  #' @description
  #' This function verifies the existence of a state and county FIPS combination 
  #' for a given decennial year. Depending on the specified level ("state" or 
  #' "county"), it handles both state and county-level data. Errors during API 
  #' calls are suppressed, and a logical value is returned indicating whether 
  #' the combination exists.
  #'
  #' @param state_fips A character string specifying the state FIPS code.
  #' @param county_fips A character string specifying the county FIPS code. 
  #'                    Can be NULL for state-level data.
  #'                    
  #' @param year An integer specifying the decennial census year (e.g., 2000, 
  #'             2010, 2020).
  #'             
  #' @param level A character string specifying the data level ("county" or "state").
  #'
  #' @return A logical value indicating whether the specified FIPS combination exists for the given year. Returns TRUE if exists, FALSE otherwise.
  
  
  tryCatch({
    if (level == "state") {
      # Fetch data for state level.
      geo_data <- suppressWarnings(suppressMessages(
        get_decennial(
          geography = "state", 
          variables = c("P001001"),
          year = year,
          state = state_fips
        )
      ))
    } else {
      geo_data <- suppressWarnings(suppressMessages(
        # Fetch data for county level.
        get_decennial(
          geography = "county", 
          variables = c("P001001"),
          year = year,
          state = state_fips,
          county = county_fips
        )
      ))
    }
    # Check if data exists.
    return(nrow(geo_data) > 0)
  }, error = function(e) {
    # Suppress API error messages and return FALSE.
    FALSE
  })
}




fetch_population_data <- function(unique_combos, geography, use_acs, level, allow_alternative = FALSE) {
  #' @description
  #' This function fetches unique population data for given combinations of 
  #' census year, state FIPS, and county FIPS. If data doesn't exist for the 
  #' given year, it fetches from the nearest available decennial year. It 
  #' handles both state and county levels, ensuring no invalid NA values are 
  #' passed when not required.
  #'
  #' @param unique_combos A data frame with unique combinations of 
  #'                      decennial_census, state_fips, and county_fips. For 
  #'                      state-level data, county_fips can be NA.
  #'                      
  #' @param geography A character string specifying the geography level 
  #'                  ("county" or "state").
  #'                  
  #' @param use_acs A logical value indicating whether to use ACS data (TRUE) 
  #'                or decennial Census data (FALSE).
  #'                
  #' @param level A character string specifying the data level ("county" or "state").
  #' @param allow_alternative A logical value indicating whether to fetch populations 
  #'                          from alternative decennial years if missing (TRUE) or 
  #'                          not (FALSE).
  #'
  #' @return A data frame with retrieved population data for each unique combination. The data frame 
  #'         includes columns: state_fips, county_fips (if applicable), decennial_census, population, 
  #'         and a column indicating if an alternative year was used.
  
  message(sprintf("Fetching the population values. ACS set to: '%s'", use_acs))
  
  # Define the alternative decennial years within the function
  alternatives <- data.frame(
    decennial_year = c(2000, 2010, 2020),
    alternative_1 = c(1990, 2000, 2010),
    alternative_2 = c(2010, 2020, 2000)
  )
  
  # Function to fetch ACS or Decennial Census data for a given year, state, and county/state
  fetch_data <- function(year, state_fips, county_fips, level) {
    # Validate FIPS combination before API call
    if (!fips_combination_exists(state_fips, county_fips, year, level)) {
      message(paste("The FIPS combination", state_fips, county_fips, "does not exist for the year", year))
      return(list(population = NA, alternative_used = FALSE))
    }
    
    result <- tryCatch({
      if (is.na(county_fips) || level == "state") county_fips <- NULL  # Set county_fips to NULL for state level
      
      # Fetch data from ACS if specified
      if (use_acs) {
        if (level == "state") {
          pop <- suppressWarnings(suppressMessages(
            get_acs(geography = "state", variables = "B01003_001", year = year, state = state_fips, survey = "acs5")
          ))
        } else {
          pop <- suppressWarnings(suppressMessages(
            get_acs(geography = "county", variables = "B01003_001", year = year, state = state_fips, county = county_fips, survey = "acs5")
          ))
        }
      } else {  # Fetch data from Decennial Census if specified
        if (level == "state") {
          pop <- suppressWarnings(suppressMessages(
            get_decennial(geography = "state", variables = "P001001", year = year, state = state_fips)
          ))
        } else {
          pop <- suppressWarnings(suppressMessages(
            get_decennial(geography = "county", variables = "P001001", year = year, state = state_fips, county = county_fips)
          ))
        }
      }
      
      if (nrow(pop) == 0) stop("No data found")
      
      population <- ifelse(use_acs, sum(pop$estimate, na.rm = TRUE), sum(pop$value, na.rm = TRUE))
      return(list(population = population, alternative_used = FALSE))
    }, error = function(e) {
      return(list(population = NA, alternative_used = FALSE))
    })
  }
  
  # Initialize the progress bar
  pb <- txtProgressBar(min = 0, max = nrow(unique_combos), style = 3)
  
  # Fetch population data for each combination, with progress tracking
  pop_data <- unique_combos %>%
    mutate(temp = pmap(list(decennial_census, state_fips, county_fips), function(decennial_census, state_fips, county_fips) {
      result <- fetch_data(decennial_census, state_fips, county_fips, level)
      
      # Handle missing population data by using alternative years
      if (allow_alternative == TRUE && is.na(result$population)) {
        alternative_years <- alternatives %>%
          filter(decennial_year == decennial_census) %>%
          select(-decennial_year) %>%
          unlist()
        
        # Attempt to fetch population data from alternative years
        for (alternative_year in alternative_years) {
          if (!is.na(alternative_year)) {
            result <- fetch_data(alternative_year, state_fips, county_fips)
            if (!is.na(result$population)) {
              message(paste("Applied alternative year", alternative_year, "for decennial year", decennial_census, "state", state_fips, "county", county_fips))
              result$alternative_used <- TRUE
              break
            }
          }
        }
        
        # If all alternatives fail, set population to NA and print a warning
        if (is.na(result$population)) {
          warning(paste("Population data not found for any alternative years for decennial year", decennial_census, "state", state_fips, "county", county_fips))
        }
      }
      
      setTxtProgressBar(pb, getTxtProgressBar(pb) + 1)
      return(result)
    }))
  
  if (allow_alternative) {
    pop_data <- pop_data %>%
      mutate(
        population = map_dbl(temp, "population"),
        alternative_used = map_lgl(temp, "alternative_used")
      ) %>%
      select(-temp)
  } else {
    pop_data <- pop_data %>%
      mutate(population = map_dbl(temp, "population")) %>%
      mutate(alternative_used = FALSE) %>%
      select(-temp)
  }
  
  close(pb)
  return(pop_data)
}




calculate_closure_rates <- function(dt, geography, use_acs = TRUE, level = "county", allow_alternative = FALSE) {
  #' @description
  #' This function calculates closure rates per 10,000 persons using pre-fetched 
  #' population data, handling both county and state levels dynamically, 
  #' incorporating progress tracking. The function can handle missing populations 
  #' by setting them to NA or using alternative population data from the nearest 
  #' decennial year.
  #'
  #' @param dt A data frame containing the closure counts, reopening counts, 
  #'           and persistence counts.
  #'
  #' @param geography A character string specifying the geography level 
  #'                  ("county" or "state").
  #'
  #' @param use_acs A logical value indicating whether to use ACS data (TRUE) 
  #'                or decennial Census data (FALSE).
  #'
  #' @param level A character string specifying data level ("county" or "state").
  #'
  #' @param allow_alternative A logical value indicating whether to allow missing 
  #'                          populations to be set to NA or use alternative 
  #'                          populations from the nearest decennial year 
  #'                          (default = FALSE).
  #'
  #' @return A data frame with closure counts and calculated closure rates per 10,000 persons.
  
  # Extract unique combinations.
  if ("county_fips" %in% colnames(dt) && level == "county") {
    unique_combos <- dt %>%
      distinct(decennial_census, state_fips, county_fips)
  } else {
    unique_combos <- dt %>%
      distinct(decennial_census, state_fips) %>%
      mutate(county_fips = NA_character_)
  }
  
  # Fetch population data
  pop_data <- fetch_population_data(unique_combos, geography, use_acs, level, allow_alternative)
  
  # Merge population data back to the main dataset
  dt <- if (level == "county") {
    dt %>% left_join(pop_data, by = c("decennial_census", "state_fips", "county_fips"))
  } else {
    dt %>% left_join(pop_data %>% select(-county_fips), by = c("decennial_census", "state_fips"))
  }
  
  # Closure rate calculation function
  closure_rate_fn <- function(closure_count, population) {
    if (is.na(population) || population == 0) return(NA)
    rate <- (closure_count / population) * 10000
    return(rate)
  }
  
  # Helper function to wrap closure_rate_fn for mutate_with_progress
  closure_rate_wrapper <- function(df, cols_to_convert) {
    df %>%
      rowwise() %>%
      mutate(across(
        all_of(cols_to_convert), 
        ~ closure_rate_fn(.x, population),
        .names = "{gsub('closure_count_', 'closure_rate_per_10000_', .col)}"
      )) %>%
      ungroup() %>% as.data.frame()
  }
  
  # Initialize a progress bar
  pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
  
  # Determine the grouping columns based on the level
  grouping_cols <- if (level == "county") c("state_fips", "county_fips") else "state_fips"
  
  # Identify columns for closure counts
  closure_cols <- grep("closure_count_", names(dt), value = TRUE)
  
  # Apply the transformation with progress tracking
  dt <- mutate_with_progress(dt, closure_cols, grouping_cols, closure_rate_wrapper, pb) %>%
    # Remove the none-rate columns.
    select(-starts_with(c("closure_count_", "reopening_count_", "persistence_")))
  
  # Close the progress bar
  close(pb)
  
  return(dt)
}




fips_combination_exists_sf <- function(state_fips, county_fips, year, level) {
  #' @description
  #' This function verifies the existence of land area data for a state and 
  #' county FIPS combination for a given decennial year. Depending on the 
  #' specified level ("state" or "county"), it handles both state and 
  #' county-level data. Errors during API calls are suppressed, and a logical 
  #' value is returned indicating whether the land area data exists.
  #'
  #' @param state_fips A character string specifying the state FIPS code.
  #' @param county_fips A character string specifying the county FIPS code. 
  #'                    Can be NULL for state-level data.
  #'                    
  #' @param year An integer specifying the decennial census year (e.g., 2000, 
  #'             2010, 2020).
  #'             
  #' @param level A character string specifying the data level ("county" or "state").
  #'
  #' @return A logical value indicating whether the specified land area data 
  #'         exists for the given year. Returns TRUE if exists, FALSE otherwise.
  
  tryCatch({
    if (level == "state") {
      # Fetch land area data for state level.
      if(year == 2020) {
        land_sf <- suppressWarnings(suppressMessages(
          states(cb = TRUE, year = year) %>% filter(STATEFP == state_fips)
        ))
      } else {
        land_sf <- suppressWarnings(suppressMessages(
          states(cb = TRUE, year = year) %>% filter(STATE == state_fips)
        ))
      }
    } else {
      # Fetch land area data for county level.
      land_sf <- suppressWarnings(suppressMessages(
        counties(state = state_fips, cb = TRUE, year = year) %>% filter(COUNTYFP == county_fips)
      ))
    }
    # Check if data exists.
    return(nrow(land_sf) > 0)
  }, error = function(e) {
    # Suppress API error messages and return FALSE.
    FALSE
  })
}




fetch_sf_data <- function(unique_combos, geography, use_acs, level, allow_alternative = FALSE) {
  #' @description
  #' This function fetches unique land area data for given combinations of 
  #' census year, state FIPS, and county FIPS. If data doesn't exist for the 
  #' given year, it fetches from the nearest available decennial year. It 
  #' handles both state and county levels, ensuring no invalid NA values are 
  #' passed when not required.
  #'
  #' @param unique_combos A data frame with unique combinations of 
  #'                      decennial_census, state_fips, and county_fips. For 
  #'                      state-level data, county_fips can be NA.
  #'                      
  #' @param geography A character string specifying the geography level 
  #'                  ("county" or "state").
  #'                  
  #' @param use_acs A logical value indicating whether to use ACS data (TRUE) 
  #'                or decennial Census data (FALSE).
  #'                
  #' @param level A character string specifying the data level ("county" or "state").
  #' @param allow_alternative A logical value indicating whether to fetch populations 
  #'                          from alternative decennial years if missing (TRUE) or 
  #'                          not (FALSE).
  #'
  #' @return A data frame with retrieved population and land area data for each 
  #'         unique combination. The data frame includes columns: state_fips, 
  #'         county_fips (if applicable), decennial_census, land_area, and a
  #'         column indicating if an alternative year was used.
  
  message(sprintf("Fetching the population and land area values. ACS set to: '%s'", use_acs))
  
  # Define the alternative decennial years within the function
  alternatives <- data.frame(
    decennial_year = c(2000, 2010, 2020),
    alternative_1 = c(1990, 2000, 2010),
    alternative_2 = c(2010, 2020, 2000)
  )
  
  # Function to fetch ACS or Decennial Census data for a given year, state, and county/state
  fetch_data <- function(year, state_fips, county_fips, level) {
    
    # Validate FIPS combination before API call
    if (!fips_combination_exists_sf(state_fips, county_fips, year, level)) {
      message(paste("The FIPS combination", state_fips, county_fips, "does not exist for the year", year))
      return(list(land_area = NA, alternative_used = FALSE))
    }
    
    result <- tryCatch({
      if (is.na(county_fips) || level == "state") county_fips <- NULL  # Set county_fips to NULL for state level
      
      if (level == "state") {
        if(year == 2020) {
          land_sf <- suppressWarnings(suppressMessages(
            states(cb = TRUE, year = year) %>% filter(STATEFP == state_fips)
          ))
        } else {
          land_sf <- suppressWarnings(suppressMessages(
            states(cb = TRUE, year = year) %>% filter(STATE == state_fips)
          ))
        }
      } else {
        land_sf <- suppressWarnings(suppressMessages(
          counties(state = state_fips, cb = TRUE, year = year) %>% filter(COUNTYFP == county_fips)
        ))
      }
      
      if (nrow(land_sf) == 0) stop("No land area data found")
      
      land_area <- st_area(land_sf) / 2.58999 # Convert square meters to square miles
      land_area <- as.numeric(land_area)
      
      return(list(land_area = land_area, alternative_used = FALSE))
    }, error = function(e) {
      return(list(land_area = NA, alternative_used = FALSE))
    })
  }
  
  # Initialize the progress bar
  pb <- txtProgressBar(min = 0, max = nrow(unique_combos), style = 3)
  
  
  # Fetch population and land area data for each combination, with progress tracking
  sq_mile_data <- unique_combos %>%
    mutate(temp = pmap(list(decennial_census, state_fips, county_fips), function(decennial_census, state_fips, county_fips) {
      result <- fetch_data(decennial_census, state_fips, county_fips, level)
      
      # Handle missing land_area data by using alternative years
      if (allow_alternative == TRUE && is.na(result$land_area)) {
        alternative_years <- alternatives %>%
          filter(decennial_year == decennial_census) %>%
          select(-decennial_year) %>%
          unlist()
        
        # Attempt to fetch population data from alternative years
        for (alternative_year in alternative_years) {
          if (!is.na(alternative_year)) {
            result <- fetch_data(alternative_year, state_fips, county_fips)
            if (!is.na(temp$land_area)) {
              message(paste("Applied alternative year", alternative_year, "for decennial year", decennial_census, "state", state_fips, "county", county_fips))
              result$alternative_used <- TRUE
              break
            }
          }
        }
        
        # If all alternatives fail, set land_area to NA and print a warning
        if (is.na(result$land_area)) {
          warning(paste("Land area data not found for any alternative years for decennial year", decennial_census, "state", state_fips, "county", county_fips))
        }
      }
      
      setTxtProgressBar(pb, getTxtProgressBar(pb) + 1)
      return(result)
    }))
  
  
  if (allow_alternative) {
    sq_mile_data <- sq_mile_data %>%
      mutate(
        land_area = map_dbl(temp, "land_area"),
        alternative_used = map_lgl(temp, "alternative_used")
      ) %>%
      select(-temp)
  } else {
    sq_mile_data <- sq_mile_data %>%
      mutate(
        land_area = map_dbl(temp, "land_area"),
        alternative_used = FALSE
      ) %>%
      select(-temp)
  }
  
  close(pb)
  return(sq_mile_data)
}




calculate_closure_rates_per_sq_mile <- function(dt, geography, use_acs = TRUE, level = "county", allow_alternative = FALSE) {
  #' @description
  #' This function calculates closure rates per square mile using pre-fetched 
  #' population and land area data, handling both county and state levels dynamically, 
  #' incorporating progress tracking. The function can handle missing populations 
  #' and land areas by setting them to NA or using alternative data from the nearest 
  #' decennial year.
  #'
  #' @param dt A data frame containing the closure counts, reopening counts, 
  #'           and persistence counts.
  #'
  #' @param geography A character string specifying the geography level 
  #'                  ("county" or "state").
  #'
  #' @param use_acs A logical value indicating whether to use ACS data (TRUE) 
  #'                or decennial Census data (FALSE).
  #'
  #' @param level A character string specifying data level ("county" or "state").
  #'
  #' @param allow_alternative A logical value indicating whether to allow missing 
  #'                          populations and land areas to be set to NA or use 
  #'                          alternative data from the nearest decennial year 
  #'                          (default = FALSE).
  #'
  #' @return A data frame with closure counts and calculated closure rates per square mile.
  
  # Extract unique combinations
  if ("county_fips" %in% colnames(dt) && level == "county") {
    unique_combos <- dt %>%
      distinct(decennial_census, state_fips, county_fips)
  } else {
    unique_combos <- dt %>%
      distinct(decennial_census, state_fips) %>%
      mutate(county_fips = NA_character_)
  }
  
  
  # Fetch land area data
  land_data <- fetch_sf_data(unique_combos, geography, use_acs, level, allow_alternative)
  
  # Merge land area data back to the main dataset
  if (level == "county") {
    dt <- dt %>% left_join(land_data, by = c("decennial_census", "state_fips", "county_fips"))
  } else {
    dt <- dt %>% left_join(land_data %>% select(-county_fips), by = c("decennial_census", "state_fips"))
  }
  
  # Closure rate calculation function
  closure_rate_fn <- function(closure_count, land_area) {
    if (is.na(land_area) || land_area == 0) return(NA)
    rate <- closure_count / land_area
    return(rate)
  }
  
  # Helper function to wrap closure_rate_fn for mutate_with_progress
  closure_rate_wrapper <- function(df, cols_to_convert) {
    df %>%
      rowwise() %>%
      mutate(across(
        all_of(cols_to_convert), 
        ~ closure_rate_fn(.x, land_area),
        .names = "{gsub('closure_count_', 'closure_rate_per_sq_mile_', .col)}"
      )) %>%
      ungroup() %>% as.data.frame()
  }
  
  # Initialize a progress bar
  pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
  
  # Determine the grouping columns based on the level
  grouping_cols <- if (level == "county") c("state_fips", "county_fips") else "state_fips"
  
  # Identify columns for closure counts using a specific pattern
  closure_cols <- grep("closure_count_", names(dt), value = TRUE)
  
  # Apply the transformation with progress tracking
  dt <- mutate_with_progress(dt, closure_cols, grouping_cols, closure_rate_wrapper, pb) %>%
    # Remove the none-rate columns.
    select(-starts_with(c("closure_count_", "reopening_count_", "persistence_")))
  
  # Close the progress bar
  close(pb)
  
  return(dt)
}




reorder_columns <- function(df, metrics_vector, dates_table) {
  #' @description
  #' Reorders the columns of a given data frame based on specified metric types 
  #' and date ranges. The columns are first ordered by the provided metric types 
  #' and then by date ranges within each metric type.
  #'
  #' @param df A data frame whose columns need to be reordered.
  #' @param metrics_vector A character vector specifying the order of metric types.
  #' @param dates_table A data frame containing start and end dates along with 
  #'                    their combined string representation.
  #'
  #' @return A data frame with columns reordered first by metric types and then 
  #'         by date ranges.
  
  
  columns <- colnames(df) %>% .[. %!in% c("state_fips", "county_fips", "decennial_census")]
  column_lead <- colnames(df) %>% .[. %in% c("state_fips", "county_fips", "decennial_census")]
  
  # Regular expression to extract metric type and time period.
  pattern <- "(\\w+)_(\\d{4}-\\d{4})"
  extracted_info <- str_match(columns, pattern)
  metric_types <- extracted_info[, 2]
  date_ranges <- extracted_info[, 3]
  
  # Create a lookup table for proper ordering of date ranges.
  if (!"Combined" %in% colnames(dates_table)) {
    dates_table <- dates_table %>% mutate(Combined = as.character(Combined))
  }
  
  # Ensuring the extracted metrics and dates match the specified vectors.
  if (all(metric_types %in% metrics_vector) && all(date_ranges %in% dates_table$Combined)) {
    # Order columns by specified metrics_vector and then dates_table.
    sorted_columns <- columns[order(match(date_ranges, dates_table$Combined), match(metric_types, metrics_vector))]
    return(df[, c(column_lead, sorted_columns)])
  } else {
    stop("Columns do not match the specified vectors.")
  }
}




combine_geocoding <- function(data) {
  #' @description
  #' This function combines geocoding information with decennial census data for 
  #' both state and county levels. It iteratively processes each unique 
  #' decennial period present in the input data and merges the corresponding 
  #' geographic shape files with the data based on FIPS codes.
  #'
  #' @param data A data frame containing the decennial census data. It must 
  #'             include columns `decennial_census` and `state_fips`. For 
  #'             county-level data, it should also include the column 
  #'             `county_fips`.
  #'
  #' @return A data frame with the combined geocoding information for each 
  #'         decennial period.
  
  # Function to get the shape files and merge for a specific decennial period.
  get_shape_and_merge <- function(year, data_sub) {
    message("Processing decennial census year: ", year)
    
    # Retrieve shape files for the specific decennial period.
    states <- tigris::states(cb = TRUE, year = year)
    counties <- tigris::counties(cb = TRUE, year = year)
    
    # Convert FIPS codes in data_sub to character type.
    data_sub <- data_sub %>%
      mutate(
        state_fips = as.character(state_fips)
      )
    
    # Check if data has county_fips column.
    has_county_fips <- "county_fips" %in% colnames(data_sub)
    
    if (has_county_fips) {
      data_sub <- data_sub %>%
        mutate(
          county_fips = ifelse(is.na(county_fips), "", as.character(county_fips))
        )
    }
    
    # Determine the correct state FIPS column name based on the year.
    state_fips_column <- if (year == 2020) {
      "STATEFP"
    } else if (year %in% c(2000, 2010)) {
      "STATE"
    } else {
      stop(paste("Unsupported decennial period:", year))
    }
    
    # Determine the correct county FIPS column name based on the year.
    if (has_county_fips) {
      county_fips_column <- if (year == 2020) {
        "COUNTYFP"
      } else if (year %in% c(2000, 2010)) {
        "COUNTY"
      } else {
        stop(paste("Unsupported decennial period:", year))
      }
      
      # Merge for county-level data.
      merged_data <- left_join(data_sub, counties, by = c("state_fips" = state_fips_column, "county_fips" = county_fips_column))
    } else {
      # Merge for state-level data.
      merged_data <- left_join(data_sub, states, by = c("state_fips" = state_fips_column))
    }
    
    # Print the column names to identify differences in the shape files output
    # between the decennial periods.
    print(paste("Year:", year))
    print(colnames(states))
    print(colnames(counties))
    
    return(merged_data)
  }
  
  # Get a list of unique decennial census years.
  unique_years <- unique(data$decennial_census)
  
  # Apply the get_shape_and_merge function to each decennial period.
  combined_data <- map_dfr(unique_years, function(year) {
    data_sub <- filter(data, decennial_census == year)
    get_shape_and_merge(year, data_sub)
  })
  
  return(combined_data)
}




