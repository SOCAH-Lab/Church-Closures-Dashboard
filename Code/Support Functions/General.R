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
##   1. check_all_counts_0_or_1: This function summarizes the selected year 
##       columns (if the columns are preceded by a "20") by summing them, and 
##       checks if all counts in these columns are either 0 or 1.
##
##   2. process_with_progress: This function processes a group within a data 
##       frame, updates the progress bar, and applies a specified function to 
##       the group.
##
##   3. process_with_progress_txt: This function processes a group within a data 
##      frame, updates the progress bar, and applies a specified function to 
##      the group. Utilized in the `mutate_with_progress()` function.
##
##   4. mutate_with_progress: This function processes each group within a data 
##      frame by converting specified columns while updating the progress bar 
##      to track completion. It ensures all values above zero are converted to 
##      1, and zeroes remain unchanged.
##
##   5. capture_warnings: This function evaluates an expression and captures 
##      any warnings generated during the evaluation. 

## ----------------------------------------------------------------
## FUNCTIONS

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



