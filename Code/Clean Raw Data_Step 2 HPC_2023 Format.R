## ----------------------------------------------------------------
## Run address validation using the USPS API.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 1st, 2026
## Date Modified: May 26th, 2026
## 
## Description: This script validates addresses using the USPS API. It is designed
##              to run both locally and on Yale's High Performance Computing (HPC)
##              cluster, leveraging the parsed indices defined below.
##
##              When running on the HPC, this script supports two execution modes:
##              a single index at a time via a live session, or as a job array
##              using the provided batch script (see SUBSECTION A1: Utilizing the
##              HPC for details).
##
##              When running locally, ensure that all code sections marked
##              "... on the HPC" are commented out and their corresponding
##              alternatives marked "... locally" are active. The HPC version
##              is given first, followed by the local version.
##
##              Results are processed in sequential sections and compiled in
##              the "Clean Raw Data_2023 Format_Step 2.R" main script.
## 
## NOTE: The USPS API requires a user account and API key to submit requests.
##       These credentials are strictly private and must not be shared. To
##       protect them, API keys are stored in a ".Renviron" file, which is
##       automatically loaded at runtime, preventing them from being hard-coded
##       into the script. Instructions for creating your own API client key and
##       secret are provided in the "Clean Raw Data_2023 Format_Step 2.R" file.
## 
##       If you are running this script locally and experience issues loading 
##       your environment variables, try running the following code to 
##       explicitly set the ".Renviron" file location using rprojroot:
## 
##       rprojroot::find_rstudio_root_file()
##       readRenviron(rprojroot::find_rstudio_root_file(".Renviron"))
## 
##       The HPC batch script includes a command that points to the ".Renviron"
##       file. However, you may still encounter issues setting this location. 
##       To resolve this, open the "Shell Access" application and run the 
##       following code, updating the R module version as needed.
## 
##       module avail R/
##       module reset
##       module load R/4.4.2-gfbf-2024a
##       normalizePath("~/FILE-PATH/.Renviron", mustWork = FALSE)
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: VALIDATE ADDRESSES USING THE USPS API
##        * SUBSECTION A1: Utilizing the HPC
##        * SUBSECTION A2: Index Queue
##        * SUBSECTION A3: Validate Addresses
##        * SUBSECTION A4: Save Result

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# # Initiate the package environment using an HPC array job
# renv::activate() 

# Initiate the package environment locally or in an HPC live session
renv::restore()

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

# Load in the functions in the HPC
source("./General.R")
source("./For Step 2_2023 Format.R")

# # Load in the functions locally
# source("./Code/Support Functions/General.R")
# source("./Code/Support Functions/For Step 2_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))


## ----------------------------------------------------------------
## PROCESS PARAMETERS FROM BATCH SCRIPT

# # Process the defined output directory and current SLURM array index for HPC array job
# args   <- commandArgs(trailingOnly = TRUE)
# outdir <- args[1]
# idx    <- as.integer(args[2])

# Set output directory HPC live session
outdir <- "USPS_Validation_Results"
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# # Set output directory locally
# outdir <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format"


## ----------------------------------------------------------------
## LOAD IN THE DATA

# Load in the previous step in the HPC
step_1 <- read_csv("./Step 1_2023 Format_HPC Subset_05.01.2026.csv",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

uscities_df <- read_csv("./simplemaps_uscities_basicv1.90/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)

# # Load in the previous step locally
# step_1 <- read_csv("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format/Step 1_2023 Format_HPC Subset_05.01.2026.csv",
#                    col_types = cols(...1 = col_skip())) %>% as.data.frame()
# 
# uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.90/uscities.csv") %>% as.data.frame()
# zip_city_lookup <- build_zip_city_lookup(uscities_df)
# 
# # While verifying the addresses, we want to add the address line 2, zip code
# # 4-digit extension, and a boolean to verify that the address has been verified.
# step_1 <- step_1 %>%
#   mutate(address_line_2 = "", address_verified = NA, zipcode_ext = "") %>%
#   relocate(address_line_2, .after = address_line_1) %>%
#   relocate(zipcode_ext, .after = zipcode) %>%
#   relocate(address_verified, .after = compiled_address) %>%
#   `rownames<-`(NULL)
# 
# step_1 <- rownames_to_column(step_1, var = "rowname")




## ----------------------------------------------------------------
## PART A: VALIDATE ADDRESSES USING THE USPS API

## --------------------
## SUBSECTION A1: Utilizing the HPC

# As described in the header section, this script can be run in two ways on
# the HPC: through a live session or as a job array. Both methods operate on
# a subset of the data to maximize compliance with the DUA. This subset is
# prepared under "SUBSECTION A1: Prepare Subset for HPC" in the
# "Clean Raw Data_Step 2.R" file.
#
# Regardless of which option is used, the first step is to upload all
# required documents and configure the environment. These steps are
# identical for both the live session and batch job approaches.
#
#   1. Create a dedicated project directory within your private user portal.
#
#   2. Upload the following files and directories:
#       - "church-closures.Rproj"
#       - "Clean Raw Data_Step 2 HPC_2023 Format.R"
#       - "USPS SLURM_2023 Format.sh"
#       - Both associated function scripts: "General.R" and 
#         "For Step 2_2023 Format.R"
#       - ".Renviron"
#       - "Step 01_2023 Format_HPC Subset_05.01.2026.csv"
#       - simplemaps_uscities_basicv1.90/
#       - renv/
#       - renv.lock
# 
#   3. Now we need to configure the ".Renviron" file and activate the project's 
#      package library. Click "<HPC name> Shell Access" to open the command-line 
#      interface.
#
#   4. Navigate to the project directory.
# 
#      cd "project_pi_bm895/sg2736/church-closures"
# 
#   5. Clear any loaded modules and load a bare version of R, updating the
#      version as needed. NOTE: Not all R versions are compatible with all
#      package versions stored by renv. This script was developed using R 4.5.2.
# 
#      module --force purge
#      module load R/4.4.2-gfbf-2024a-bare
# 
#   6. Request time on a compute node.
# 
#      salloc -t 0:20:00 --mem=8G
# 
#   7. Load a bare version of R.
# 
#      R --vanilla
# 
#   8. Set the paths to the ".Renviron" and "renv.lock" files. Verify that
#      the resulting file path output is correct.
# 
#      normalizePath(".Renviron", mustWork = FALSE)
#      normalizePath("renv.lock", mustWork = FALSE)
# 
#   9. Restore all packages and their dependencies from the lockfile. Use 
#      "Selection: 1" to activate the project using the provided library.
# 
#      renv::restore()
# 
#  10. Exit R to refresh the session. Now you are ready to proceed with either
#      the live session of job array method.
# 
#      quit()
# 
# The live session operates in a manner similar to running the script locally. 
# After completing the steps outlined above, simply:
#
#   1. Start a live session by selecting "Interactive Apps" --> "RStudio Server".
#      NOTE: Only one live session can be run at a time.
#
#   2. Use the settings below, ensuring the R version matches the one
#      configured in the previous step.
#       - RStudio Server version: RStudio-Server/2024.12.1-563
#       - R version: R/4.4.2-gfbf2024a
#       - 6 hours, 2 CPU, 10 GiB per CPU
# 
#   3. When the session is ready, click "Connect to RStudio Server" to open
#      the environment.
# 
#   4. In the top-right click the "Project" button and open 
#      "church-closures.Rproj".
# 
#   5. Open "Clean Raw Data_2023 Format_HPC USPS API.R" and run all the code 
#      until you reach "SUBSECTION A2: Index Queue".
# 
#   6. In SUBSECTION A2: Index Queue", set the current index range to run 
#      (x = 1 through 29).
# 
#     processed_indices[x]
# 
#   7. Run the code under "SUBSECTION A3: Validate Addresses". A progress bar
#      will appear indicating the current progress of the function.
#
#   8. After the function completes, save the results in
#      "SUBSECTION A4: Save Result".
# 
#   9. Once all index ranges have been processed, save the results locally to:
#
#      ~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format
#
#  10. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2023 Format.R" to compile all results together.
# 
# The array job is run entirely through the command-line interface. After 
# completing the steps outlined above, simply:
#
#   1. Click "<HPC name> Shell Access" to open the command-line interface.
#
#   2. Navigate to the project directory.
# 
#      cd "project_pi_bm895/sg2736/church-closures"
# 
#   3. Request time on a compute node.
# 
#      salloc -p day -t 7:00:00 --cpus-per-task=8 --mem=16G
# 
#   4. After the job allocation has been approved and is ready for use, execute
#      the SLURM batch script:
#
#      chmod +x "USPS SLURM_2023 Format.sh"
#      sbatch "USPS SLURM_2023 Format.sh"
#
#   5. OPTIONAL: Inspect any errors that arise from running the script:
#
#      tail -n 50 Logs/<RUN NAME>.err
# 
#   6. Once all index ranges have been processed, save the results locally to:
#
#      ~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format
#
#   7. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2023 Format.R" to compile all results together.


## --------------------
## SUBSECTION A2: Index Queue

# The algorithm was timed locally, where approximately 875 entries were
# processed per 5 minutes (~42,000 in four hours). Based on this, the data
# was partitioned into 42,000-entry indices (listed below) to fit within
# the HPC's 6-hour session limit.
#
# Each index was processed in a separate session and compiled in
# "Clean Raw Data_Step 2_2023 Format.R".

processed_indices <- c(
  "1 to 42000", "42001 to 84000", "84001 to 126000", "126001 to 168000",
  "168001 to 210000", "210001 to 252000", "252001 to 294000", "294001 to 336000",
  "336001 to 378000", "378001 to 420000", "420001 to 462000", "462001 to 504000",
  "504001 to 546000", "546001 to 588000", "588001 to 630000", "630001 to 672000",
  "672001 to 714000", "714001 to 756000", "756001 to 798000", "798001 to 840000",
  "840001 to 882000", "882001 to 924000", "924001 to 966000", "966001 to 1008000",
  "1008001 to 1050000", "1050001 to 1092000", "1092001 to 1134000", 
  "1134001 to 1176000", "1176001 to 1210975"
)

# Set index using an HPC array job
current_array_index <- processed_indices[idx]

# # Set index locally or in an HPC live session (1 through 29)
# idx <- 1
# current_array_index <- processed_indices[idx]

nums <- as.integer(unlist(regmatches(current_array_index, gregexpr("\\d+", current_array_index))))


## --------------------
## SUBSECTION A3: Validate Addresses

# Load the USPS API Keys
Sys.getenv("R_ENVIRON_USER")
consumer_key <- Sys.getenv("USPS_CONSUMER_KEY", unset = "<UNSET>")
consumer_secret <- Sys.getenv("USPS_CONSUMER_SECRET", unset = "<UNSET>")

# Parse index
index = seq(nums[1], nums[2])

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
        
        # Stop after the first candidate that yields a result
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


## --------------------
## SUBSECTION A4: Save Result

# # Commit results in the HPC
outfile <- file.path(outdir, sprintf(str_c("Step 2_2023 Format_USPS Output_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
write.csv(as.data.frame(step_1_out), outfile, row.names = FALSE)

cat("Wrote:", outfile, "\n")

# # Commit results locally
# outfile <- file.path(outdir, sprintf(str_c("Step 2_2023 Format_USPS Output_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
# write.csv(as.data.frame(step_1_out), outfile, row.names = FALSE)


