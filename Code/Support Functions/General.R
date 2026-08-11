## ----------------------------------------------------------------
## Define general-purpose functions referenced in subsequent scripts.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: March 5th, 2026
## 
## Description: General-purpose functions used across the data cleaning,
##              harmonization, and metric calculation processes, some of
##              which are nested within other functions and others used
##              independently.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions:
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
## 
##   6. find_first_one: Finds the date column name where the first 1 occurs. 
##      Used for arranging the rows associated with one ABI in descending 
##      order: i.e. older address to recent address.
## 
##   7. import_church_db: Import church-closures DuckDB tables (data + QC) with 
##       minimal dependencies. Reads tables from a DuckDB database file located 
##       at `db_path`. Designed to avoid `dplyr`/`dbplyr` and return a simple R 
##       list:
##          - optionally the main compiled `data` table
##          - QC tables grouped by prefix (e.g., `import_qc_18`, `import_qc_20`)
## 
##       QC tables are expected to follow the naming pattern: <prefix>__<qc_name>
##       For example: `import_qc_20__abi_check` becomes accessible as
##       `res$import_qc_20$abi_check`.
## 
##   8. read_list_from_duckdb: Read tables from a DuckDB database file into a 
##       named list. Companion to write_list_to_duckdb(): each DuckDB table is 
##       returned as one list element (analogous to reading sheets from an XLSX 
##       workbook).

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




import_church_db <- function(db_path,
                             import_data = c("all", "data", "qc"),
                             data_table  = "data",
                             qc_prefixes = c("import_qc_18", "import_qc_20"),
                             read_only   = TRUE) {
  #' Import church-closures DuckDB tables (data + QC) with minimal dependencies.
  #' Reads tables from a DuckDB database file located at `db_path`.
  #' Designed to avoid `dplyr`/`dbplyr` and return a simple R list:
  #' - optionally the main compiled `data` table
  #' - QC tables grouped by prefix (e.g., `import_qc_18`, `import_qc_20`)
  #'
  #' QC tables are expected to follow the naming pattern:
  #'   <prefix>__<qc_name>
  #' For example: `import_qc_20__abi_check` becomes accessible as
  #' `res$import_qc_20$abi_check`.
  #'
  #' @param db_path Character scalar. Path to the DuckDB database directory/file.
  #' @param import_data Character. What to import:
  #'   \itemize{
  #'     \item `"all"`: import both the main data table and QC tables
  #'     \item `"data"`: import only the main data table
  #'     \item `"qc"`: import only QC tables (grouped by `qc_prefixes`)
  #'   }
  #' @param data_table Character scalar. Name of the main data table (default `"data"`).
  #' @param qc_prefixes Character vector. QC table prefixes to import (defaults to
  #'   `c("import_qc_18","import_qc_20")`).
  #' @param read_only Logical. Passed to DuckDB connection; should generally stay `TRUE`.
  #'
  #' @return A named list.
  #' - If `import_data` includes `"data"`, the list contains element `$data` (a data.frame).
  #' - If `import_data` includes `"qc"`, the list contains one element per prefix in
  #'   `qc_prefixes` (e.g., `$import_qc_18`, `$import_qc_20`), each a named list of
  #'   QC data.frames keyed by the suffix after `__`.
  #'
  #' @examples
  #' \dontrun{
  #' # QC only
  #' res_qc <- import_church_db(out_db, import_data = "qc")
  #' names(res_qc$import_qc_20)
  #' res_qc$import_qc_20$abi_check
  #'
  #' # Data only
  #' res_data <- import_church_db(out_db, import_data = "data")
  #' head(res_data$data)
  #'
  #' # Everything
  #' res_all <- import_church_db(out_db, import_data = "all")
  #' }
  
  # Validate/standardize `import_data`
  import_data <- match.arg(import_data)
  
  # Hard dependencies (kept minimal on purpose)
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required.")
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("Package 'duckdb' is required.")
  }
  
  # Open DuckDB connection to the database at `db_path`
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = read_only)
  
  # Always clean up connection, even if an error occurs mid-import.
  # This helps avoid: "Connection already working on another query"
  on.exit({
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    try(duckdb::duckdb_shutdown(), silent = TRUE)
  }, add = TRUE)
  
  # Discover available tables (we use this to find QC tables by prefix)
  tabs <- DBI::dbListTables(con)
  
  # The result container we will populate and return
  res <- list()
  
  # ---- Import main compiled data table (optional) ----
  if (import_data %in% c("all", "data")) {
    if (!data_table %in% tabs) {
      stop(sprintf("Data table '%s' not found in DB.", data_table))
    }
    res$data <- DBI::dbReadTable(con, data_table)
  }
  
  # ---- Import QC tables (optional) ----
  # For each prefix (e.g., "import_qc_20"), import all tables matching:
  #   ^import_qc_20__
  # Then strip the prefix from the list element names so you can do:
  #   res$import_qc_20$abi_check
  if (import_data %in% c("all", "qc")) {
    for (px in qc_prefixes) {
      # All QC tables for this prefix
      qc_tabs <- grep(paste0("^", px, "__"), tabs, value = TRUE)
      
      # Pre-allocate list for speed and set clean names (suffix after "__")
      out <- vector("list", length(qc_tabs))
      names(out) <- sub(paste0("^", px, "__"), "", qc_tabs)
      
      # Read each QC table into memory
      for (i in seq_along(qc_tabs)) {
        out[[i]] <- DBI::dbReadTable(con, qc_tabs[[i]])
      }
      
      # Store per-prefix QC list at res[[px]] (e.g., res$import_qc_20)
      res[[px]] <- out
    }
  }
  
  res
}




read_list_from_duckdb <- function(path,
                                  tables = NULL,            # NULL = read all tables
                                  check_home_writable = TRUE,
                                  use_home_cache_if_writable = TRUE) {
  #' Read tables from a DuckDB database file into a named list.
  #' Companion to write_list_to_duckdb(): each DuckDB table is returned as one
  #' list element (analogous to reading sheets from an XLSX workbook).
  #'
  #' @param path File path to the DuckDB database file, e.g. "./results/qc_geo.duckdb".
  #' @param tables Character vector of table names to read. If NULL, reads all tables.
  #' @param check_home_writable Logical; same intent as in write_list_to_duckdb().
  #' @param use_home_cache_if_writable Logical; same intent as in write_list_to_duckdb().
  #'
  #' @return A named list of data.frames (one per table).
  
  # Optional: confirm HOME is writable (useful on HPC) -----------------------
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
  
  # Connect read-only and pull tables ---------------------------------------
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  available <- DBI::dbListTables(con)
  
  if (is.null(tables)) {
    tables <- available
  } else {
    missing <- setdiff(tables, available)
    if (length(missing) > 0) {
      stop("These tables were not found in the DuckDB file: ",
           paste(missing, collapse = ", "))
    }
  }
  
  out <- setNames(vector("list", length(tables)), tables)
  for (nm in tables) {
    out[[nm]] <- DBI::dbReadTable(con, nm)
  }
  out
}



