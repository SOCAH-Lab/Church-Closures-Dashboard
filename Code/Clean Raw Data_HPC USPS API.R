## ----------------------------------------------------------------
## 
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 1st, 2026
## Date Modified: May 4th, 2026
## 
## Description: This script validates addresses using the USPS API. It was
##              executed both locally and on Yale's High Performance Computing
##              (HPC) cluster, using the parsed indices defined below. Results
##              were processed in sequential sections and compiled in the Step 2 
##              main script.
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: VALIDATE ADDRESSES USING THE USPS API
##        * SUBSECTION A1: Utilizing the HPC and Local Device
##        * SUBSECTION A2: Index Queue
##        * SUBSECTION A3: Script to Validate Addresses

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Install missing packages
pkgs <- c("readr", "tidyr", "dplyr", "stringr", "tibble", "purrr", "httr", "jsonlite", "progress")

to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
if (length(to_install)) {install.packages(to_install, ask = FALSE)}

# Load packages to the environment
suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests for APIs (GET/POST, headers, auth)
  library("jsonlite")         # Parse/write JSON (fromJSON/toJSON)
  library("progress")         # Progress bars
})

# Load in the functions
source("./General.R")
source("./For Step 2.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Load in the pre-produced test results for evaluation.
step_1 <- read_csv("./Step 01_HPC Subset_05.01.2026.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

uscities_df <- read_csv("./simplemaps_uscities_basicv1.90/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)




## ----------------------------------------------------------------
## PART A: VALIDATE ADDRESSES USING THE USPS API

## --------------------
## SUBSECTION A1: Utilizing the HPC and Local Device

# The USPS API requires a user account and API key to submit requests. These
# credentials are strictly private and must not be shared. To protect them,
# the API keys were stored in a .Renviron file, which is automatically loaded
# at runtime. On the HPC, however, this approach is complicated by the fact
# that compute nodes (which are not private) do not have access to the login
# node (which is private to users and HPC administrators).
#
# To address this, the Yale Center for Research Computing (YCRC) recommended
# running the script directly on the login node or using an active session. Any 
# opportunities for parallelization would depend on the computational resource 
# demands of the script itself, and whether those exceed user limitations.
# 
# The following Bash script was executed on the HPC to assess compute resource
# demands. Results indicated that the login node could not support
# parallelization; however, sequential live sessions were able to complete
# within their respective 6-hour session time limits.
# 
# Bash code:
# module load R/4.4.2-gfbf-2024a
# /usr/bin/time -v Rscript "Clean Raw Data_HPC USPS API.R" > logs/run.out 2> logs/run.err
# tail -n 50 logs/run.err


# As a result, the script was executed in multiple live sessions, both on the
# HPC and locally. The output from each session was saved and then compiled
# together once all sections were completed.


## --------------------
## SUBSECTION A2: Index Queue

# The algorithm was timed locally, where approximately 875 entries were
# processed per 5 minutes (~42,000 in four hours). Based on this, the data
# was partitioned into 42,000-entry indices (listed below) to fit within
# the HPC's 6-hour session limit.
#
# Each index was processed in a separate session and compiled in
# "Clean Raw Data_Step 2.R".

# 1:42000 <-- completed HPC
# 42001:84000 <-- completed HPC
# 84001:126000 <-- completed local
# 126001:168000 <-- completed local
# 168001:210000 <-- completed local
# 210001:252000 <-- completed HPC
# 252001:294000 <-- completed HPC
# 294001:336000 <-- completed local
# 336001:378000 <-- completed local
# 378001:420000 <-- completed local
# 420001:462000 <-- completed local
# 462001:504000 <-- completed local
# 504001:546000 <-- completed local
# 546001:588000 <-- completed local
# 588001:630000 <-- completed local
# 630001:672000 <-- completed local
# 672001:714000 <-- completed HPC
# 714001:756000 <-- completed local
# 756001:798000 <-- completed local
# 798001:840000 <-- in progress
# 840001:882000 <-- in progress
# 882001:924000
# 924001:966000
# 966001:1008000
# 1008001:1050000
# 1050001:1092000
# 1092001:1134000
# 1134001:1176000
# 1176001:1210975


## --------------------
## SUBSECTION A3: Script to Validate Addresses




# Load the USPS API Keys.
Sys.getenv("R_ENVIRON_USER")
consumer_key <- Sys.getenv("USPS_CONSUMER_KEY", unset = "<UNSET>")
consumer_secret <- Sys.getenv("USPS_CONSUMER_SECRET", unset = "<UNSET>")

# Set index
index = 672001:714000

# Add a progress bar to show where the function is in the for loop.
pb = txtProgressBar(min = min(index), max = max(index), style = 3)

for (i in min(index):max(index)) {
  # 1) Pull the i-th row into variables used by validate_usps_address()
  address1 <- step_1$address_line_1[i]
  address2 <- ""
  city     <- step_1$city[i]
  state    <- step_1$state[i]
  
  # Split ZIP into ZIP5 and ZIP+4 (blank if missing)
  zip5 <- str_extract(step_1$zipcode[i], "^[0-9]+") %>% ifelse(is.na(.) || . == "", "", .)
  zip4 <- str_extract(step_1$zipcode[i], "(?<=-)[0-9]+") %>% ifelse(is.na(.) || . == "", "", .)
  
  # 2) Attempt #1: Validate using the original inputs
  suppressWarnings({
    usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "")
  })
  
  # 3) Attempt #2: If no match, assess/correct city (try ZIP5 orientations), then retry
  if (all(dim(usps_validated) == 0)) {
    
    zip5_raw <- zip5 %>% ifelse(is.na(.) || . == "", "", .)
    zip5_raw <- ifelse(nzchar(zip5_raw), str_pad(zip5_raw, width = 5, side = "left", pad = "0"), "")
    
    # Leading/trailing zeros were stripped prior to receiving the raw data.
    # Some ZIP-to-city sources treat those edge zeros differently, so we test 
    # multiple orientations by "sliding" the same count of edge zeros between 
    # the front and back of the ZIP (still 5 digits).
    zip5_candidates <- make_zip5_candidates(zip5_raw) %>% .[. %!in% zip5_raw]
    
    # Try candidates until one returns a city (then stop); otherwise do nothing
    for (z in zip5_candidates) {
      query_result <- get_city_info(z, zip_city_lookup)
      
      if (!str_detect(query_result, "No Matches")) {
        city <- query_result
        zip5 <- z
        
        suppressWarnings({
          usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = address2, city, state, zip5, zip4 = zip4
          )
        })
        
        # Stop after the first candidate that yields a city
        break
      }
    }
  }
  
  # 4) Attempt #3: if still no match, swap address lines, then retry
  if (all(dim(usps_validated) == 0)) {
    # Move address_line_1 into address2 and leave address1 blank
    address1 <- ""
    address2 <- step_1$address_line_1[i]
    
    suppressWarnings({
      usps_validated <- validate_usps_address(consumer_key, consumer_secret, address1, address2 = "", city, state, zip5, zip4 = "")
    })
  }
  
  # 5) Save results back into step_1 (single write block)
  if (all(dim(usps_validated) == 0)) {
    
    # Nothing matched after all attempts
    step_1$compiled_address[i] <- "No address match found"
    step_1$address_verified[i] <- FALSE
    
  } else {
    
    # Overwrite fields with USPS-preferred formatting
    step_1$address_line_1[i] <- usps_validated[, "address_line_1"]
    step_1$address_line_2[i] <- usps_validated[, "address_line_2"]
    step_1$city[i]           <- usps_validated[, "city"]
    step_1$state[i]          <- usps_validated[, "state"]
    step_1$zipcode[i]        <- usps_validated[, "zipcode"]
    step_1$zipcode_ext[i]    <- usps_validated[, "zipcode_ext"]
    
    # Mark as verified
    step_1$address_verified[i] <- TRUE
    
    # Build a single printable address string: "line1, line2, city, state ZIP-EXT"
    step_1$compiled_address[i] <- str_c(
      str_flatten(na.omit(unlist(usps_validated[1:4])), collapse = ", "),
      str_flatten(na.omit(unlist(usps_validated[5:6])), collapse = "-"),
      sep = " "
    ) %>%
      str_trim() %>%
      str_remove("-$") %>%
      str_remove("\\s*$")
  }
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}

# Convert list-format columns to character type
step_1_out <- step_1[min(index):max(index), ] |>
  dplyr::mutate(dplyr::across(
    where(is.list),
    ~ vapply(.x, function(el) {
      if (is.null(el) || (length(el) == 0)) return(NA_character_)
      paste(as.character(unlist(el, recursive = TRUE, use.names = FALSE)), collapse = "; ")
    }, character(1))
  ))

# Commit results
write.csv(as.data.frame(step_1_out), 
          str_c("./Results/Step 2_USPS Output_", index[1], " to ", index[length(index)], ".csv"), 
          row.names = FALSE)


