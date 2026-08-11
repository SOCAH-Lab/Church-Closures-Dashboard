## ----------------------------------------------------------------
## Run Address Validation and Annotate with Census Boundaries (Version 2)
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: July 3rd, 2026
## Date Modified: July 18th, 2026
## 
## Description: This script conducts all data cleaning and validation steps: 
##              validating addresses via the USPS API, associating unverified 
##              addresses with existing similar verified addresses, verifying 
##              provided geolocations using the Census Bureau Geocoder API, and 
##              annotating unique ABI and address entries with relevant census 
##              boundaries (block, tract, CBSA, CSA, etc.).
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
##              the "Clean Raw Data_Step 2_2026 Format.R" main script.
## 
## UPDATE: As of July 12, 2026, the USPS API charges users per address query.
##         The process outlined in version 1 of this script
##         ("Clean Raw Data_Step 2 HPC v1_2026 Format") assumed unrestricted
##         access to the API. To accommodate this change, a second version
##         ("Clean Raw Data_Step 2 HPC v2_2026 Format.R") was created that
##         includes the following modifications:
##
##           1. Users can choose whether or not to use the USPS API in their
##              workflow. If the user chooses not to use the API, or if none of
##              the addresses under a given ABI are successfully verified via
##              the API, only an agnostic string-matching step is used to help
##              mitigate redundancy caused by typographical errors.
##
##           2. Users who choose to use the API can set limits on how many
##              addresses are verified, allowing finer control over the costs
##              of running the algorithm.
##
##         More details can be found in "SUBSECTION B3: Optional USPS Address 
##         Verification (API Setup and Quota Controls)" below.
## 
## NOTE: The USPS API requires a user account and API key to submit requests.
##       These credentials are strictly private and must never be hard-coded
##       into scripts or shared. API keys are stored in a project-level
##       ".Renviron" file that is loaded automatically at runtime. Instructions
##       for creating your own API client key and secret are provided in the
##       script header of "Clean Raw Data_Step 2_2026 Format.R".
##
##       LOCAL USERS — if environment variables fail to load, explicitly point
##       R to the ".Renviron" file using rprojroot:
##
##          rprojroot::find_rstudio_root_file()
##          readRenviron(rprojroot::find_rstudio_root_file(".Renviron"))
##
##       HPC USERS — the batch submission script includes a command that sets
##       the ".Renviron" path automatically. If environment variables still
##       fail to resolve in that context, follow the full environment
##       configuration steps documented in "PART A: UTILIZING THE HPC".
## 
## NOTE: This script requires GDAL to run. Verify that GDAL is installed on
##       your local device using the following commands:
##
##       Check for the TIGER driver (returns one row if installed)
##          sf::st_drivers() |> subset(name == "TIGER")
##
##       Confirm the GDAL version (this script was developed using v3.5.3)
##          sf::sf_extSoftVersion()["GDAL"]
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - PROCESS PARAMETERS FROM BATCH SCRIPT
##    - LOAD IN THE DATA
## 
##    - PART A: UTILIZING THE HPC
## 
##    - PART B: ENVIRONMENT SETUP (GENERAL + ARRAY-SPECIFIC)
##        * SUBSECTION B1: Index Queue
##        * SUBSECTION B2: Set Geocoder Search Priorities
##        * SUBSECTION B3: Optional USPS Address Verification (API Setup and Quota Controls)
##        * SUBSECTION B4: Load Relevant Block-Level GeoPackages by State
## 
##    - PART C: CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA
##        * SUBSECTION C1: Algorithm
##        * SUBSECTION C2: Save Result

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment
source("renv/activate.R")
renv::activate()

# Load packages to the environment
suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("openxlsx")         # Read/write Excel workbooks (.xlsx) with multiple sheets
  library("DBI")              # Standard database interface for R (dbConnect, dbWriteTable, dbGetQuery)
  library("duckdb")           # DuckDB database engine + DBI backend (local .duckdb files, in-memory DBs)
  library("arrow")            # Parquet/Feather & fast I/O (Arrow)
  library("tidyr")            # Tidies/reshapes data (pivot, separate/unnest)
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("tibble")           # Manipulate data frames in tidyverse
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests for APIs (GET/POST, headers, auth)
  library("jsonlite")         # Parse/write JSON (fromJSON/toJSON)
  library("future.apply")     # Parallel processing
  library("stringdist")       # Measuring string distances
  library("progress")         # Progress bars
  library("data.table")       # High-performance data manipulation
  library("sf")               # Simple Features for spatial data (geometry + CRS operations)
  library("tigris")           # Download/read US Census TIGER/Line shapefiles
})

# # Load in the functions in the HPC
# source("./Support Functions/General.R")
# source("./Support Functions/For Step 2_2026 Format.R")
# source("./Support Functions/For Step 2_Compile Decennial Data.R")

# Load in the functions locally
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 2_2026 Format.R")
source("./Code/Support Functions/For Step 2_Compile Decennial Data.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))

# Define the "if else" for null options operation
"%||%" <- function(a, b) if (!is.null(a)) a else b

# Cache TIGRIS shapefiles locally to avoid re-downloading each session
options(tigris_use_cache = TRUE)

# Use S2 spherical geometry engine for correct lon/lat distance calculations
sf::sf_use_s2(TRUE)

# Set up the plan for parallel processing
plan(multisession, workers = 4)




## ----------------------------------------------------------------
## PROCESS PARAMETERS FROM BATCH SCRIPT

# # Process the defined output directory and current SLURM array index for HPC 
# # array job
# args   <- commandArgs(trailingOnly = TRUE)
# outdir <- args[1]
# idx    <- as.integer(args[2])
# 
# # Set output directory HPC live session
# outdir <- "Results"

# Set output directory locally
outdir <- file.path("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format", 
                    paste("batch_local", "01", sep = "_"))

# The code provided below creates the results directories for a live or local
# session. Note that the shell script for the batch array creates a separate
# folder for each job's output, which differs from this directory structure.

dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(outdir, "Verified Result"), showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(outdir, "Address QC"), showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(outdir, "Geo QC"), showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(outdir, "Census QC"), showWarnings = FALSE, recursive = TRUE)



## ----------------------------------------------------------------
## LOAD IN THE DATA

# # Load in the previous step and other datasets in the HPC
# # Load standardized and converted data
# church_2026_form <- read_parquet("./church_2026_form_standardized_06.10.2026.parquet")
# 
# # Toggle based on whether the zip code field was originally imported as a
# # character type; if not, leading and trailing zeros may have been lost.
# zip_codes_character <- TRUE
# 
# # Load core field handling/rename spec.
# core_fields <- read_csv("./Handling Raw Variables_05.12.2026.csv")
# 
# # Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
# uscities_df <- read_csv("./simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
# zip_city_lookup <- build_zip_city_lookup(uscities_df)
# 
# # Load the combined national-level Metropolitan/Micropolitan Statistical Area 
# # (CBSA), Combined Statistical Area (CSA), and ZIP Code Tabulation Areas (ZCTA)
# # GeoPackage file. If nothing loads, skip to "SUBSECTION A3: Build Precompiled 
# # TIGER/Line GeoPackages" first before coming back to this line.
# # 
# # State block or block group, tract, county, and state geometries are loaded 
# # later in the script, as they depend on which states are present in the 
# # current subset.
# core_areas_layers <- sf::st_layers("./Census Bureau TIGER Line Shapefiles/core_areas.gpkg")$name
# 
# core_areas <- setNames(
#   lapply(core_areas_layers, function(lyr) st_read("./Census Bureau TIGER Line Shapefiles/core_areas.gpkg", layer = lyr, quiet = TRUE)),
#   core_areas_layers
# )


# Load in the previous step and other datasets locally
# Load standardized and converted data
church_2026_form <- read_parquet("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")

# Toggle based on whether the zip code field was originally imported as a
# character type; if not, leading and trailing zeros may have been lost.
zip_codes_character <- TRUE

# Load core field handling/rename spec.
core_fields <- read_csv("Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")

# Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)

# Load the combined national-level Metropolitan/Micropolitan Statistical Area
# (CBSA), Combined Statistical Area (CSA), and ZIP Code Tabulation Areas (ZCTA)
# GeoPackage file. If nothing loads, skip to "SUBSECTION A3: Build Precompiled
# TIGER/Line GeoPackages" first before coming back to this line.
#
# State block or block group, tract, county, and state geometries are loaded
# later in the script, as they depend on which states are present in the
# current subset.
core_areas_layers <- sf::st_layers("./Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg")$name

core_areas <- setNames(
  lapply(core_areas_layers, function(lyr) st_read("./Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg", layer = lyr, quiet = TRUE)),
  core_areas_layers
)




## ----------------------------------------------------------------
## PART A: UTILIZING THE HPC

# As described in the header section, this script can be run in two ways on
# the HPC: through a live session or as a job array. Regardless of which option 
# is used, the first step is to upload all required documents and configure the 
# environment. These steps are identical for both the live session and batch 
# job approaches.
#
#   1. Create a dedicated project directory within your private user portal.
#      For example:
# 
#         mkdir "church_closures"    # in "project_pi_bm895/sg2736/"
# 
#   2. Upload the following files and directories:
#       - "church-closures.Rproj"
#       - ".Renviron" (if using the USPS API)
#       - renv/
#       - "renv.lock"
#       - Two scripts: 
#             1. "Clean Raw Data_Step 2 HPC v2_2026 Format.R"
#             2. "Validation SLURM_2026 Format.sh"
#       - Three associated function scripts. Place these in 
#         "~/church-closures/Support Functions/": 
#             1. "General.R"
#             2. "For Step 2_2026 Format.R"
#             3. "For Step 2_Compile Decennial Data.R".
#       - Four datasets or directories containing data: 
#             1. "church_2026_form_standardized_06.10.2026.parquet"
#             2. "Handling Raw Variables_05.12.2026.csv" (from ./Data/Results/From Process Data Update)
#             3. simplemaps_uscities_basicv1.93/ (from ./Data/Raw/)
#             4. Census Bureau TIGER Line Shapefiles/ (from ./Data/Results/)
# 
#   3. Now we need to configure the ".Renviron" file (if using) and activate the 
#      project's package library. Click "<HPC name> Shell Access" to open the 
#      command-line interface.
#
#   4. Navigate to the project directory.
# 
#         cd "project_pi_bm895/sg2736/church_closures"
#
#   5. Request time on a compute node.
# 
#         salloc --mem=16G -t 0-4
# 
#   6. Reset the renv project environment by removing the local library,
#      staging directory, and lockfile:
#  
#         rm -rf renv/library
#         rm -rf renv/staging
# 
#   7. Reset the module environment and load the required R module. NOTE: Not 
#      all R versions are compatible with all package versions stored by renv. 
#      This script was developed using R 4.5.2.
#
#         module reset
#
#      This resets all loaded modules to the cluster's default state while
#      preserving StdEnv, which maintains essential environment variables
#      required for proper cluster functioning.
#
#         module load R/4.4.2-gfbf-2024a
#
#      The non-bare version of R is specified here intentionally, as it
#      includes additional system libraries that are necessary when
#      building packages from source.
# 
#   8. Reload the ICU module environment so stringi will link against an
#      available one.
# 
#         module spider icu                       # find the ICU for the installed stringi.so
#         module load ICU/75.1-GCCcore-13.3.0     # load one of the modules
# 
#   9. Load R.
# 
#         R
# 
#  10. Disable the system requirements check. The default checker only scans 
#      standard system locations and is unaware of custom paths used on Bouchet.
#
#         options(renv.config.sysreqs.check = FALSE)
# 
#  11. Set the paths to the ".Renviron" and "renv.lock" files. Verify that
#      the resulting file path output is correct.
# 
#         normalizePath(".Renviron", mustWork = FALSE)    # if using
#         normalizePath("renv.lock", mustWork = FALSE)
#
#  12. Read the lockfile to extract exact package versions. 
#      NOTE: renv::install() does not respect lockfile versions automatically.
#
#         lock <- renv::lockfile_read("renv.lock")
#
#         ks_ver  <- paste0("KernSmooth@", lock$Packages$KernSmooth$Version)
#         s2_ver  <- paste0("s2@",         lock$Packages$s2$Version)
#         sf_ver  <- paste0("sf@",         lock$Packages$sf$Version)
#         stringi <- paste0("stringi@",    lock$Packages$stringi$Version)
#
#  13. Install required packages from source:
#
#         renv::install(ks_ver, type = "source", prompt = FALSE, rebuild = TRUE)
#         renv::install(s2_ver, type = "source", prompt = FALSE, rebuild = TRUE)
#         renv::install(sf_ver, type = "source", prompt = FALSE, rebuild = TRUE)
#         renv::install(stringi, type = "source", prompt = FALSE, rebuild = TRUE)
#
#      The rebuild = TRUE flag bypasses the global cache entirely, ensuring
#      a clean build is performed even if a cached — but potentially
#      incorrectly built — version already exists. Note: installation of
#      s2 may take approximately 5 minutes.
# 
#  14. Restore all packages and their dependencies from the lockfile. Use 
#      "Selection: 1" to activate the project using the provided library.
# 
#         renv::restore()
# 
#  15. Exit R to refresh the session without saving the workspace image. Now 
#      you are ready to proceed with either the live session of job array method.
# 
#         quit()
# 
#  16. If you experience issues loading installed packages from the cache,
#      try deleting the renv directory and lockfile, then re-upload them
#      before repeating the setup steps. Note that it may sometimes take a
#      few minutes for the system to reflect installations or other updates.
# 
# 
# The live session operates in a manner similar to running the script locally. 
# After completing the steps outlined above, simply:
#
#   1. Start a live session by selecting "Interactive Apps" --> "RStudio Server".
#      NOTE: Only one live session can be run at a time.
#
#   2. Use the settings below, ensuring the R version matches the one
#      configured in the previous step.
#       - RStudio Server version: RStudio-Server/2024.12.1-563-renvfix
#       - R version: R/4.4.2-gfbf-2024a
#       - 6 hours, 2 CPU, 10 GiB per CPU
# 
#   3. When the session is ready, click "Connect to RStudio Server" to open
#      the environment.
# 
#   4. In the top-right click the "Project" button and open "church-closures.Rproj".
# 
#   5. Open "Clean Raw Data_Step 2 HPC v2_2026 Format.R" and run all the code
#      until you reach "PART B: ENVIRONMENT SETUP (GENERAL + ARRAY-SPECIFIC)".
#      This part contains several user-configurable sections that must be
#      carefully reviewed and adjusted depending on your current run or
#      preferred verification approach.
#
#      WARNING: These sections are critical and must be paid close attention to.
#
#   6. In "SUBSECTION B1: Index Queue", set the current index range to run.
#      For example:
#
#         idx <- 1
#         current_array_index <- processed_indices[idx]
#
#   7. In "SUBSECTION B2: Set Geocoder Search Priorities", confirm that the
#      appropriate benchmarks and vintages are configured. If you are unsure
#      which options are available, refer to "SUBSECTION A2: Set Geocoder
#      Search Priorities" in "Clean Raw Data_Step 2_2026 Format.R".
#
#   8. In "SUBSECTION B3: Optional USPS Address Verification (API Setup and
#      Quota Controls)", use the algorithm toggles to enable or disable address
#      validation via the USPS API (incurs a charge), and if enabled, specify
#      how many addresses to verify.
# 
#      A message will be generated summarizing how the algorithm has interpreted
#      your settings, and indicating how many total addresses will be submitted
#      to the API, if applicable.
# 
#   9. In "SUBSECTION B4: Load Relevant Block-Level GeoPackages by State",
#      select the smallest geographic unit to associate with each record: block
#      or block group. The block includes block group data, but is
#      computationally intensive. The algorithm is designed to generate results
#      for both options; toggle which one you wish to use.
# 
#      NOTE: This section assumes the required GeoPackage (*.gpkg) files have
#      already been created. If these have not been generated, refer to 
#      "SUBSECTION A3: Build Precompiled TIGER/Line GeoPackages Search Priorities" 
#      in "Clean Raw Data_Step 2_2026 Format.R" for further directions.
# 
#  10. Run the code under "SUBSECTION C1: Algorithm" of "PART C: CLEAN,
#      VALIDATE, AND ANNOTATE ADDRESS DATA" to start the processing algorithm.
#      A progress bar will appear indicating the current progress of the
#      function.
# 
#  11. After the function completes, save the results in
#      "SUBSECTION C2: Save Result".
# 
#  12. Once all index ranges have been processed, save results locally to their
#      respective directories specified in "PROCESS PARAMETERS FROM BATCH SCRIPT":
#
#      "~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"
#
#  13. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2026 Format.R" to compile all results together.
# 
# 
# The array job is run entirely through the command-line interface. After 
# completing the steps outlined above, simply:
#
#   1. Click "<HPC name> Shell Access" to open the command-line interface.
#
#   2. Navigate to the project directory.
# 
#         cd "project_pi_bm895/sg2736/church_closures"
# 
#   3. After the job allocation has been approved and is ready for use, execute
#      the SLURM batch script:
#
#         chmod +x "Validation SLURM_2026 Format.sh"
#         sbatch "Validation SLURM_2026 Format.sh"
# 
#      This process will generate two new directories: Results and Logs. The
#      Logs directory contains the output and error files produced by each
#      array job. Successfully written results will be saved to their
#      respective subdirectory within Results.
# 
#   4. OPTIONAL: Check the status of the script by running the following command.
#      Replace the batch job number with the one returned by the previous step.
# 
#         squeue -j 12924166
#
#   5. OPTIONAL: Inspect any errors that arise from running the script:
#
#         tail -n 50 Logs/<RUN NAME>.err
# 
#   6. Once all index ranges have been processed, save results locally to their
#      respective directories specified in "PROCESS PARAMETERS FROM BATCH SCRIPT":
#
#      "~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"
#
#   7. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2026 Format.R" to compile all results together.




## ----------------------------------------------------------------
## PART B: ENVIRONMENT SETUP (GENERAL + ARRAY-SPECIFIC)

## --------------------
## SUBSECTION B1: Index Queue

# The algorithm was timed locally, where approximately 14 entries were
# processed per 5 minutes (~1,000 abi in six hours). Based on this, the data
# will need to be partitioned into 1,000-entry indices (listed below) to fit 
# within the HPC's 6-hour session limit.
# 
# Each index was processed in a separate session and compiled in
# "PART C: Recompile Results from the HPC" of "Clean Raw Data_Step 2_2026 
# Format.R".

# Prepare the dataframes used in the script
church_2026_form_dt <- as.data.table(church_2026_form)  # Convert for efficient data manipulation
setorder(church_2026_form_dt, state, abi)  # Organize the table by state to increase census boundary efficiency then abi

## Generate the parsed indices based on unique ABI positions (labels are just "start to end")
# processed_indices <- make_ranges(church_2026_form_dt, abi_col = "abi", chunk_size = 5000L) %>%
#  .$label

# Initial indices for batch array
processed_indices <- sprintf(
  "%d to %d",
  seq(1, 1080001, by = 5000),
  c(seq(5000, 1080000, by = 5000), 1080764)
)

# Prior batches may not have completed the full set; adjust the indices to run 
# only those missed previously.
# 
# Save the array indices as numeric values for reference in "SUBSECTION B1: 
# Index Queue" of "Clean Raw Data_Step 2 HPC v2_2026 Format.R".
array_num <- c(
  1:19, 21:24, 29:38, 40:45, 48:55, 57, 59:91, 93:105, 107:121, 123:168, 
  171:182, 186, 189:190, 193, 195, 199:201, 203, 205:206, 208:217
)

# Isolate the indices that were not successfully run and extract the
# dimensions of their corresponding arrays.
miss_chr <- processed_indices[-array_num]

mat <- stringr::str_match(miss_chr, "^\\s*(\\d+)\\s*to\\s*(\\d+)\\s*$")
miss_df <- data.frame(
  from = as.integer(mat[,2]),
  to   = as.integer(mat[,3])
)

# Define the new span of indices to be run. Reducing the index range aims to
# help the arrays that exceeded the time limit complete within the allotted
# time period.
by <- 1000

missing_subranges_1000 <- unlist(Map(function(a, b) {
  starts <- seq(a, b, by = by)
  ends   <- pmin(starts + by - 1L, b)
  sprintf("%d to %d", starts, ends)
}, miss_df$from, miss_df$to))

# Update the primary indices to include only the missing indices.
processed_indices <- missing_subranges_1000


# # Set index using an HPC array job
# current_array_index <- processed_indices[idx]

# Set index locally or in an HPC live session
idx <- 56
current_array_index <- processed_indices[idx]

nums <- as.integer(unlist(regmatches(current_array_index, gregexpr("\\d+", current_array_index))))

# Parse index over UNIQUE-ABI POSITIONS
index <- seq.int(nums[1], nums[2])


# Define the search space as the ABI values for this chunk (mutually exclusive).
# Exclude any ABI that appears in any non‑U.S. state/territory (keep only the 
# 50 states + DC).
us_states <- c(state.abb, "DC")

# 1) ABI subset first
search_space <- unique(church_2026_form_dt[, abi])[index]

# 2) Keep only ABIs whose rows are ALL in allowed states
abi_ok <- church_2026_form_dt[
  abi %in% search_space,
  .(ok = all(state %in% us_states)),
  by = abi
][ok == TRUE, abi]

search_space <- abi_ok


## --------------------
## SUBSECTION B2: Set Geocoder Search Priorities

# The U.S. Census Geocoder API supports multiple benchmarks and vintages for
# geolocation searches. Refer to "SUBSECTION A2: Set Geocoder Search Priorities"
# in "Clean Raw Data_Step 2_2026 Format.R" to see the available options and
# construct a prioritized search sequence (copied below). 


# Construct a prioritized table of benchmark/vintage search criteria,
# where earlier rows take precedence.
spec <- tibble::tibble(
  benchmark_name = c("Public_AR_Census2020", "Public_AR_Census2020",
                     "Public_AR_Current",  "Public_AR_ACS2025"),
  vintage_name   = c("Census2020_Census2020", "Census2010_Census2020",
                     "Current_Current",       "Current_ACS2025")
)

# Construct the "tries" input for `validate_geolocation()`
geocoder_census_tries <- census_geo_make_tries(spec)


## --------------------
## SUBSECTION B3: Optional USPS Address Verification (API Setup and Quota Controls)

# Ideally, the addresses used to confirm geolocation accuracy would also be
# validated. This workflow previously relied on the USPS API for that purpose;
# however, as of July 12th, 2026, the USPS API now charges a fee per address
# queried.
# 
# In response, address verification has been made an optional step in the
# workflow. Users may also limit API usage by capping the number of addresses
# submitted per query.
# 
# If address verification is enabled, users must create their own USPS account
# and assume responsibility for any applicable license agreements and costs.
# Refer to the API key setup guide in the "Clean Raw Data_Step 2_2026 Format.R" 
# header, then use the settings below to configure whether and to what extent 
# the USPS API is used.
# 
# NOTE: The USPS API verification functions and algorithm were designed and
#       optimized for version 3.2.3 and have not been tested against the
#       July 2026 update (version 3.3.1).


# Toggle on or off the address verification workflow using the USPS API.
verify_addresses <- FALSE

# USPS API costs are incurred per verified address. Users can control the
# number of addresses submitted for verification using the quota setting below.
# 
# To preserve downstream behavior — where unverified addresses are reconciled
# to a verified version via similarity matching — the algorithm selects either
# all or no addresses for a given ABI. If no addresses are marked for
# verification (whether due to quota limitations or verify_addresses == FALSE),
# an agnostic similarity clustering method will be used to reduce unnecessary
# redundancy.
# 
# Setting `set_quota = Inf` instructs the function to select all available
# addresses in the subset. Equivalently, any quota value high enough to
# accommodate all addresses in the subset will produce the same result.
# 
# NOTE: The USPS API may itself set quota limits that will impact how many
#       addresses are able to be verified.
set_quota = 0

# If the quota is less than the number of available ABIs in the subset, the
# selection is randomized. Users can set a seed to make this reproducible or
# use set_seed = NULL to allow results to vary across runs.
set_seed = 1


# The following conditionals apply the quota and validate that all necessary
# components are present: verify_addresses, set_quota, set_seed, search_space
# (defined in the previous section), and the user's consumer_key and
# consumer_secret from ".Renviron". If all components are present, a do_api
# Boolean column is either omitted entirely or added with the selected entries
# marked.
# 
# A message is also returned clarifying whether randomization was applied and
# how many addresses across how many businesses (ABI) will be processed.

if (exists("verify_addresses", inherits = FALSE) == FALSE) {
  
  stop("You must clarify if addresses will be verified using the USPS API.", call. = FALSE)
  
} else if (isTRUE(verify_addresses)) {
  
  # -- Require a search_space when running USPS verification -------------------
  # Case 1: search_space not provided/present -> stop
  if (exists("search_space", inherits = FALSE) == FALSE) {
    stop("search_space not set", call. = FALSE)
    
    # Case 2: search_space present -> must be a non-empty numeric/integer vector
  } else {
    if (is.null(search_space) || length(search_space) < 1L || anyNA(search_space) ||
        !(is.numeric(search_space) || is.integer(search_space))) {
      stop("search_space must be a non-empty vector of numbers.", call. = FALSE)
    }
  }
  
  
  # -- Require a seed when running USPS verification ---------------------------
  # Case 1: set_seed not provided/present -> stop
  if (exists("set_seed", inherits = FALSE) == FALSE) {
    stop("Seed not set", call. = FALSE)
    
    # Case 2: set_seed present -> must be NULL or a single number/integer
  } else if (!is.null(set_seed)) {
    
    if (length(set_seed) != 1L || is.na(set_seed) || !(is.numeric(set_seed) || is.integer(set_seed))) {
      stop("Seed must be NULL or a single number/integer.", call. = FALSE)
    }
  }
  
  
  # -- Require a quota when running USPS verification --------------------------
  # Case 1: set_quota not provided/present -> stop
  if (exists("set_quota", inherits = FALSE) == FALSE) {
    stop("quota not set", call. = FALSE)
    
    # Case 2a: set_quota = Inf -> process all addresses
  } else if (length(set_quota) == 1L && isTRUE(is.infinite(set_quota)) && set_quota > 0) {
    # ok; message will be emitted after selection so we can report counts
    
    # Case 2b: otherwise must be a single non-negative numeric (finite)
  } else if (is.null(set_quota) || length(set_quota) != 1L || is.na(set_quota) ||
             !is.numeric(set_quota) || set_quota < 0 || isTRUE(is.infinite(set_quota))) {
    stop("Addresses will be verified using the USPS API, but a valid numeric `quota` (single non-negative value) was not provided.", call. = FALSE)
  }
  
  
  # -- Load the USPS keys --------------------------------------------
  Sys.getenv("R_ENVIRON_USER")
  consumer_key    <- Sys.getenv("USPS_CONSUMER_KEY", unset = "<UNSET>")
  consumer_secret <- Sys.getenv("USPS_CONSUMER_SECRET", unset = "<UNSET>")
  
  
  # Require consumer_key and consumer_secret when running USPS verification
  # Case 1: either not provided/present -> stop
  if (exists("consumer_key", inherits = FALSE) == FALSE ||
      exists("consumer_secret", inherits = FALSE) == FALSE) {
    stop('To use the USPS API the consumer key and secret must be provided in the ".Renviron" file. Refer to the header of "Clean Raw Data_Step 2_2026 Format.R" for additional guidance about how to do this.',
         call. = FALSE)
    
    # Case 2: both present -> must be non-missing, non-empty strings
  } else {
    
    key_ok <- !(is.null(consumer_key) || is.na(consumer_key) ||
                  !is.character(consumer_key) || length(consumer_key) != 1L ||
                  !nzchar(trimws(consumer_key)))
    
    secret_ok <- !(is.null(consumer_secret) || is.na(consumer_secret) ||
                     !is.character(consumer_secret) || length(consumer_secret) != 1L ||
                     !nzchar(trimws(consumer_secret)))
    
    if (!key_ok || !secret_ok) {
      missing_which <- paste(
        c(if (!key_ok) "consumer key", if (!secret_ok) "consumer secret"),
        collapse = " and "
      )
      
      stop(sprintf(
        'To use the USPS API, %s must be provided in the ".Renviron" file. Refer to the header of "Clean Raw Data_Step 2_2026 Format.R" for additional guidance about how to do this.',
        missing_which
      ), call. = FALSE)
    }
  }
  
  
  # -- Generate optimized selection --------------------------------------------
  res <- usps_quota_mark_api(
    church_2026_form_dt[abi %in% search_space],
    quota = set_quota,
    seed  = set_seed
  )
  
  process_df <- res$data
  
  # -- Let users know what was selected ----------------------------------------
  all_abi_selected <- length(res$picked_groups) == length(unique(process_df$abi))
  
  repro_msg <- if (all_abi_selected) {
    "All available ABI are being processed; randomization has no effect."
  } else if (is.null(set_seed)) {
    "Seed is NULL: quota forces a subset to be chosen, so the selected ABI may vary between runs."
  } else {
    sprintf(
      "Seed set (%s): quota forces a subset to be chosen, so the selected ABI will be reproducible.",
      format(as.integer(set_seed), big.mark = ",")
    )
  }
  
  if (length(set_quota) == 1L && isTRUE(is.infinite(set_quota)) && set_quota > 0) {
    message(sprintf(
      "Quota is Inf: all eligible addresses will be processed (n = %s unique (abi, combined_address) units across %s of %s ABI). %s",
      format(res$picked_unit_n, big.mark = ","),
      format(length(res$picked_groups), big.mark = ","),
      format(length(unique(process_df$abi)), big.mark = ","),
      repro_msg
    ))
  } else {
    message(sprintf(
      "Addresses to be processed: %s unique (abi, combined_address) units across %s of %s ABI. %s",
      format(res$picked_unit_n, big.mark = ","),
      format(length(res$picked_groups), big.mark = ","),
      format(length(unique(process_df$abi)), big.mark = ","),
      repro_msg
    ))
  }
  
} else if (identical(verify_addresses, FALSE)) {
  
  message("Addresses will not be verified using the USPS API.")
  
  consumer_key    <- NA_character_
  consumer_secret <- NA_character_
  
  process_df <- church_2026_form_dt[abi %in% search_space]
  
}


## --------------------
## SUBSECTION B4: Load Relevant Block-Level GeoPackages by State

# State block-level shapefiles were precompiled into GeoPackage (*.gpkg)
# files containing all desired metadata (block, tract, county, and state).
# Loading all files simultaneously is computationally prohibitive, so data
# is first sorted by state and then subset for processing. The following
# function loads only the GeoPackage required for the current batch.
# 
# This step is required before moving the analysis to the HPC and must be run
# locally (it prepares large spatial inputs for downstream steps).
#
# Confirm completion by checking "SUBSECTION A3: Build Precompiled TIGER/Line
# GeoPackages" in the script "Clean Raw Data_Step 2_2026 Format.R".
# 
# You should verify that all required Census geography GeoPackage (*.gpkg)
# files have been created. The block and/or block group layers, along with the 
# core Census reference GeoPackages used throughout the pipeline.
# 
# After confirming these files exist, upload the full set to the HPC from:
# "./Data/Results/Census Bureau TIGER Line Shapefiles/"

# Define the geography level = c("blocks", "block group")
block_geography <- "block groups"

# Identify the unique states represented in the data.
states_present <- church_2026_form %>%
  filter(abi %in% search_space) %>%
  pull(state) %>%
  unique()

# # For in the HPC:
# # Load the relevant GeoPackages as a nested list: states at the first level,
# # decennial years at the second.
# blocks_by_state <- read_state_gpkgs_for_data(
#   states_present, "Census Bureau TIGER Line Shapefiles/",
#   geography = block_geography
# )

# For locally:
# Load the relevant GeoPackages as a nested list: states at the first level,
# decennial years at the second.
blocks_by_state <- read_state_gpkgs_for_data(
  states_present, "./Data/Results/Census Bureau TIGER Line Shapefiles/", 
  geography = block_geography
)


## ----------------------------------------------------------------
## PART C: CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA

## --------------------
## SUBSECTION C1: Algorithm

# Initialize the empty lists
finish_build <- vector("list", length(search_space))
qc_address   <- list(qc1 = list(), qc2 = list(), qc3 = list())
qc_geo       <- list(qc1 = list(), qc2 = list(), qc3 = list())
qc_census    <- list(qc1 = list(), qc2 = list())

# Initialize progress bar
pb = txtProgressBar(min = 0, max = length(search_space), style = 3)

for (i in 1:length(search_space)) {
  # Subset to only entries associated with one business ABI.
  subset <- process_df[abi %in% search_space[i]]
  
  # ---- reset per-ABI verification state ----
  na_addresses <- FALSE
  dont_match   <- NULL
  
  reduced_candidate_addresses <- setNames(
    as.data.frame(replicate(14, logical(0), simplify = FALSE)),
    c("best_address","state","n_address","enough_geo","lat_avg","lon_avg",
      "geolocation_verified","n_attempts","query_statuses","geo_matched_address",
      "benchmark","vintage_input","latitude_ver","longitude_ver")
  )
  
  # combined table + checks
  candidate_addresses         <- data.table()
  reduced_candidate_addresses <- data.table()
  induced_duplicates          <- FALSE
  combined                    <- data.table()
  qc_geo_df                   <- data.table()
  qc_census_df                <- data.table()
  
  
  # --------------------
  # LOOP PART A: Isolate Unique Candidate Addresses
  
  # For this script, it is not necessary to process each individually listed
  # address. Instead, they will be compressed down to the exact unique addresses
  # until the validation is completed, then rejoined with the remaining, whole
  # dataset.
  # 
  # To ensure this information relates back to subset an anchored min/max year 
  # is added to show the span of years that address was observed. Additionally,
  # the geolocation (longitude/latitude) is averaged for each exact address.
  
  candidate_addresses <- subset %>%
    # Keep only the address fields plus a numeric year we can aggregate (and lat/lon for averaging)
    transmute(
      address_line_1, city, state, zipcode, zip4, combined_address,
      year = as.integer(archive_version_year),  # convert year to integer for min/max
      latitude, longitude,
      # Only carry do_api forward if it exists in the subset
      ..do_api = if ("do_api" %in% names(.)) do_api else NA,
    ) %>%
    # Collapse multiple yearly rows down to one row per unique combined_address
    group_by(combined_address) %>%
    summarise(
      # Representative address fields to carry forward for this combined_address
      # (they may vary slightly over time; we keep the first observed)
      address_line_1 = dplyr::first(address_line_1),
      city           = dplyr::first(city),
      state          = dplyr::first(state),
      zipcode        = dplyr::first(zipcode),
      zip4           = dplyr::first(zip4),
      
      # Earliest year this combined_address appears in the chronological subset
      anchor_year_min = min(year, na.rm = TRUE),
      
      # Latest year this combined_address appears in the chronological subset
      anchor_year_max = max(year, na.rm = TRUE),
      
      # Mean lat/long per combined_address (and count of non-missing coordinate rows)
      n_geo = sum(!is.na(latitude) & !is.na(longitude)),
      latitude_avg  = ifelse(n_geo > 0, mean(latitude,  na.rm = TRUE), NA_real_),
      longitude_avg = ifelse(n_geo > 0, mean(longitude, na.rm = TRUE), NA_real_),
      
      # Include USPS API boolean if present, otherwise NA
      do_api        = dplyr::first(..do_api),
      
      # Return an ungrouped data frame
      .groups = "drop"
    )
  
  # Ensure the rows are ordered by ascending year of observation
  setorder(candidate_addresses, anchor_year_min)
  setDT(candidate_addresses)
  
  # Some ABIs include records where address_line_1 is missing. These records 
  # cannot be used in the downstream analyses. We still separate them here 
  # (rather than dropping silently) so we can quantify coverage of the
  # issue—e.g., whether missingness is concentrated in specific regions and
  # evaluate lon/lat consistency.
  
  # If this ABI has any missing address_line_1 values, split out those records.
  if (any(is.na(subset$address_line_1))) {
    
    # Records missing address_line_1 cannot be matched/geocoded; store separately.
    dont_match <- candidate_addresses %>% filter(is.na(address_line_1))
    
    # Keep only records with a usable address_line_1 for downstream matching/geocoding.
    candidate_addresses <- candidate_addresses %>% filter(!is.na(address_line_1))
    
    # Flag that this ABI contained missing address_line_1 values (for QC/reporting).
    na_addresses <- TRUE
    
    # dont_match is later appended to reduced_candidate_addresses. If there are no
    # non-missing addresses to process, dont_match would otherwise be carried forward
    # and incorrectly appended under the prior ABI. To prevent this, we initialize
    # an empty placeholder (with the expected columns) here.
    
    reduced_candidate_addresses <- setNames(
      as.data.frame(replicate(14, logical(0), simplify = FALSE)),
      c("best_address","state","n_address","enough_geo","lat_avg","lon_avg",
        "geolocation_verified","n_attempts","query_statuses","geo_matched_address",
        "benchmark","vintage_input","latitude_ver","longitude_ver")
    )
    
    if (nrow(candidate_addresses) == 0) {
      # If there are no non-missing addresses to process, reduced_candidate_addresses
      # would otherwise be carried forward and incorrectly appended under the 
      # prior ABI. To prevent this, we initialize an empty placeholder (with the 
      # expected columns) here.
      
      candidate_addresses <- data.frame(
        combined_address        = character(),
        address_line_1          = character(),
        city                    = character(),
        state                   = character(),
        zipcode                 = character(),
        zip4                    = character(),
        anchor_year_min         = integer(),
        anchor_year_max         = integer(),
        n_geo                   = integer(),
        latitude_avg            = numeric(),
        longitude_avg           = numeric(),
        do_api                  = logical(),
        address_verified        = character(),
        usps_status             = character(),
        usps_status_detail      = character(),
        attempt_succeeded       = character(),
        ver_geolocation_test    = character(),
        verified_address        = character(),
        address_matched         = character(),
        match_geolocation_test  = character(),
        matched_address         = character(),
        best_address            = character(),
        row_idx                 = integer(),
        stringsAsFactors        = FALSE
      )
      
    }
    
    
    
  } else {
    
    # Flag that this ABI had complete address_line_1 coverage.
    na_addresses <- FALSE
  }
  
  
  # --------------------
  # LOOP PART B: Consolidate and Verify the Addresses
  
  # Address heterogeneity was detected in the 2023 Format: duplicate entries
  # arise from minor typographical variation, and some records sharing the same
  # address_line_1 are associated with different cities, producing inconsistent
  # coordinates.
  #
  # This workflow resolves heterogeneity in two stages:
  #   1) USPS API verification: standardises addresses and reconciles unverified
  #      records to their closest verified match by string similarity.
  #   2) Agnostic clustering (fallback when verification is unavailable or fails):
  #      groups similar addresses by string similarity and resolves conflicts
  #      within each cluster, reducing redundant entries.
  #
  # In the 2023 data, applying this workflow halved the number of unique
  # address-level rows (unique by ABI and address) in the wide format.
  
  # Proceed with matching only for records with a valid address_line_1 (not NA).
  if (!nrow(candidate_addresses) == 0) {
    
    if ( verify_addresses == TRUE && !is.null(candidate_addresses$do_api) && all(candidate_addresses$do_api == TRUE) ) {
      
      # ---- reset per-ABI verification + triage state ----
      match_to   <- data.table()
      unmatched  <- data.table()
      cand       <- data.table()
      best       <- data.table()
      
      # fuzzy/exact triage working vars
      target_line_1      <- NA_character_
      comparisons_line_1 <- character(0)
      match_line_1       <- list()
      target_cluster     <- character(0)
      threshold_line1    <- NA_real_
      match_check        <- NA_integer_
      
      # USPS per-row holders (initialized here for clarity; also reset inside the loop)
      usps_validated     <- list(ok = FALSE, status = NA_character_, status_detail = NA_character_)
      attempt_succeeded  <- NA_character_
      attempt_log        <- list()
      
      
      # Prepare candidate_addresses for QC output:
      candidate_addresses <- candidate_addresses %>%
        #   - Initialize verification status and verified address storage fields.
        mutate(address_verified = NA, verified_address = NA) %>%
        #   - Initialize attempt_succeeded to track which retry attempt validated
        #     each address and to capture non-hard-stop API errors
        #     (e.g., HTTP 400 "Address Not Found") for audit purposes.
        mutate(attempt_succeeded = NA_character_) %>%
        #   - Initialize standardized USPS status fields (from usps_status_group())
        mutate(usps_status = NA_integer_, usps_status_detail = NA_character_) %>%
        #   - Initialize ver_geolocation_test as NA for downstream geocoding checks.
        mutate(ver_geolocation_test = NA_character_)
      
      
      # --------------------
      # LOOP PART B.i.: Validate Addresses with USPS Database
      
      usps_status_group <- function(status) {
        s_raw <- as.character(status)
        
        if (is.na(s_raw)) {
          return(list(usps_status = NA_integer_, usps_status_detail = "Other unanticipated errors"))
        }
        
        if (s_raw == "ok") {
          return(list(usps_status = 200L, usps_status_detail = "Verified"))
        }
        
        detail <- dplyr::case_when(
          s_raw == "token_error" ~ "Token error",
          s_raw == "parse_error" ~ "Response parse error",
          s_raw == "no_address_in_response" ~ "No address found in USPS response",
          s_raw == "invalid_zip4_format" ~ "Invalid ZIP+4 format (programmer/config error)",
          TRUE ~ NA_character_
        )
        
        code_chr <- stringr::str_extract(s_raw, "(?<!\\d)\\d{3}(?!\\d)")
        code_int <- suppressWarnings(as.integer(code_chr))
        
        if (is.na(detail)) {
          detail <- dplyr::case_when(
            is.na(code_int) ~ "Other unanticipated errors",
            code_int == 200 ~ "200 Successful operation",
            code_int == 400 ~ "400 Bad request",
            code_int == 401 ~ "401 Unauthorized",
            code_int == 403 ~ "403 Access denied",
            code_int == 429 ~ "429 Rate limit reached",
            code_int == 503 ~ "503 Service unavailable",
            TRUE ~ paste0(code_int, " HTTP error")
          )
        }
        
        list(usps_status = code_int, usps_status_detail = detail)
      }
      
      for (addr_idx in seq_len(nrow(candidate_addresses))) {
        
        # ---- reset per-row USPS state ----
        address1 <- address2 <- city <- state <- zip5 <- zip4 <- NA_character_
        zip5_raw <- NA_character_
        zip5_candidates <- character(0)
        query_result <- NA_character_
        
        usps_validated    <- list(ok = FALSE, status = NA_character_, status_detail = NA_character_)
        grp               <- list()
        attempt_succeeded <- NA_character_
        attempt_log       <- list()
        last_log          <- NULL
        
        # Pull the addr_idx-th row into variables used by validate_usps_address()
        address1 <- candidate_addresses$address_line_1[addr_idx]
        address2 <- ""
        city     <- candidate_addresses$city[addr_idx]
        state    <- candidate_addresses$state[addr_idx]
        zip5     <- candidate_addresses$zipcode[addr_idx]
        zip4     <- ifelse(is.na(candidate_addresses$zip4[addr_idx]), "", candidate_addresses$zip4[addr_idx])
        
        # -- Attempt 1: validate using the original inputs as-is -------------------
        
        usps_validated <- suppressWarnings(
          validate_usps_address(
            consumer_key, consumer_secret,
            address1, address2, city, state, zip5, zip4
          )
        )
        
        # -- Derive standardized USPS status + detail from final status ----
        grp <- usps_status_group(usps_validated$status)
        
        if (usps_validated$ok) {
          attempt_succeeded <- "First try"
        } else {
          attempt_log[[1]] <- list(attempt = 1L, status = usps_validated$status,
                                   detail  = usps_validated$status_detail)
        }
        
        
        # -- Attempt 2: if no match, assess/correct city via ZIP lookup, then retry
        
        # Handles the case where ZIP codes were imported as numeric, stripping leading
        # and trailing zeros. Only try if the rate limit has not been reached.
        
        if (!usps_validated$ok && (is.na(grp$usps_status) || grp$usps_status != 429L)) {
          
          if (zip_codes_character) {
            
            # Look up the city in the SimpleMaps U.S. Cities dataset
            query_result <- zip5 %>%
              ifelse(is.na(.) || . == "", "", .) %>%
              (\(z) get_city_info(z, zip_city_lookup))()
            
            # If a match is found, re-query the USPS database to confirm
            if (!str_detect(query_result, "No Matches")) {
              city <- query_result
              
              suppressWarnings({
                usps_validated <- validate_usps_address(
                  consumer_key, consumer_secret,
                  address1, address2, city, state, zip5, zip4
                )
              })
              
              if (usps_validated$ok) {
                attempt_succeeded <- "With city correction by zip5"
              } else {
                attempt_log[[2]] <- list(attempt = 2L, status = usps_validated$status,
                                         detail  = usps_validated$status_detail)
              }
            }
            
          } else if (!zip_codes_character) {
            
            # Leading/trailing zeros may have been stripped prior to importing the 
            # raw data. Some ZIP-to-city sources treat those edge zeros differently, 
            # so we test multiple orientations by "sliding" the same count of edge 
            # zeros between the front and back of the ZIP (still 5 digits).
            
            zip5_raw <- zip5 %>% ifelse(is.na(.) || . == "", "", .)
            zip5_raw <- ifelse(nzchar(zip5_raw), str_pad(zip5_raw, width = 5, side = "left", pad = "0"), "")
            
            # Generate different possible combinations
            zip5_candidates <- make_zip5_candidates(zip5_raw) %>% .[. %!in% zip5]
            
            # Try candidates until one returns a city (then stop); otherwise do nothing.
            if (length(zip5_candidates) > 0) {
              for (z in zip5_candidates) {
                query_result <- get_city_info(z, zip_city_lookup)
                
                if (!str_detect(query_result, "No Matches")) {
                  city <- query_result
                  zip5 <- z
                  
                  suppressWarnings({
                    usps_validated <- validate_usps_address(
                      consumer_key, consumer_secret,
                      address1, address2, city, state, zip5, zip4
                    )
                  })
                  
                  if (usps_validated$ok) {
                    attempt_succeeded <- "With city correction by zip5"
                  } else {
                    attempt_log[[2]] <- list(attempt = 2L, status = usps_validated$status,
                                             detail  = usps_validated$status_detail)
                  }
                  
                  # Stop after the first candidate that yields a city, regardless of
                  # whether the USPS call succeeded — avoid burning more candidates.
                  break
                }
              }
            }
          }
        }
        
        # -- Derive standardized USPS status + detail from final status ----
        grp <- usps_status_group(usps_validated$status) # Update if needed
        candidate_addresses$usps_status[addr_idx]        <- grp$usps_status
        candidate_addresses$usps_status_detail[addr_idx] <- grp$usps_status_detail
        
        # -- Save results back into candidate_addresses (single write block) -------
        
        if (!usps_validated$ok) {
          
          # Nothing matched after all attempts — record failure reason from the last
          # non-empty attempt log entry for the most actionable status message.
          last_log <- Filter(Negate(is.null), attempt_log)
          last_log <- if (length(last_log) > 0) last_log[[length(last_log)]] else NULL
          
          candidate_addresses$address_verified[addr_idx]  <- FALSE
          candidate_addresses$verified_address[addr_idx]  <- "No address match found"
          candidate_addresses$attempt_succeeded[addr_idx] <- "None"
          
        } else {
          
          # Build a formatted address string ("line1, line2, city, state ZIP-EXT"),
          # treating blank/whitespace-only fields as NA to omit them from the output.
          candidate_addresses$address_verified[addr_idx]  <- TRUE
          candidate_addresses$attempt_succeeded[addr_idx] <- attempt_succeeded
          
          candidate_addresses$verified_address[addr_idx] <-
            paste(
              stringr::str_c(
                stats::na.omit(
                  dplyr::na_if(
                    stringr::str_trim(as.character(unlist(usps_validated[1:4]))),
                    ""
                  )
                ),
                collapse = ", "
              ),
              stringr::str_c(
                stats::na.omit(
                  dplyr::na_if(
                    stringr::str_trim(as.character(unlist(usps_validated[5:6]))),
                    ""
                  )
                ),
                collapse = "-"
              )
            ) |>
            stringr::str_trim() |>
            stringr::str_remove("-$") |>
            stringr::str_remove("\\s*$")
        }
      }
      
      # Re-cast candidate_addresses as a data.table for downstream processing.
      setDT(candidate_addresses)
      
      
      # --------------------
      # LOOP PART B.ii.: Resolve Records with No Address Match Found
      
      # Prepare candidate_addresses for QC output:
      candidate_addresses <- candidate_addresses %>%
        #   - Normalize address_verified to a "TRUE"/"FALSE" string.
        mutate(address_verified = as.numeric(address_verified)) %>%
        mutate(address_verified = as.character(address_verified)) %>%
        mutate(address_verified = ifelse(as.numeric(address_verified) == 1, "TRUE", "FALSE"))
      
      
      # For addresses that could not be verified, route them through a triage process
      # that attempts to associate each record with a verified address:
      # 
      #   a. Exact match on address_line_1 with differing metadata:
      #         - First prefer a verified match whose anchor-year interval overlaps 
      #           (or touches) the unmatched anchor-year window.
      #         - If no overlap exists, link to the verified match whose anchor-year 
      #           interval is closest to the unmatched window, whether it falls 
      #           before or after.
      #         - “Closest” is based on the smallest non-overlap gap between the two 
      #           year ranges; for non-overlapping ranges, use the candidate’s end 
      #           year if it is before the window, or its start year if it is after 
      #           the window.
      #         - Bypass the geolocation validation step.
      # 
      #   b. Fuzzy match on address_line_1:
      #         - Link to the verified match whose anchor-year interval overlaps (or 
      #           touches) the unmatched anchor-year window; if none overlap, choose 
      #           the verified match with the smallest gap to the window 
      #           (before/after treated equally). Similar to exact matching.
      #         - Apply the geolocation validation step.
      #         - If validation fails, retain the record as a separate unverified 
      #           entry.
      
      # ---- reset per-batch triage state (before splitting) ----
      match_to   <- data.table()
      unmatched  <- data.table()
      cand       <- data.table()
      best       <- data.table()
      
      target_line_1      <- NA_character_
      comparisons_line_1 <- character(0)
      match_line_1       <- list()
      target_cluster     <- character(0)
      threshold_line1    <- NA_real_
      match_check        <- NA_integer_
      
      # Only proceed if (1) there are still unverified addresses to resolve and
      # (2) there is at least one verified address available to test against.
      if( any(candidate_addresses$verified_address %in% "No address match found") &&
          any(candidate_addresses$verified_address %!in% "No address match found") ) {
        
        # Separate addresses into two groups: records to be matched and potential 
        # match candidates.
        match_to  <- candidate_addresses[address_verified == TRUE, ]
        unmatched <- candidate_addresses[address_verified == FALSE, ]
        
        # Assign a unique row index
        unmatched[, u_id := .I]
        
        for (u_idx in seq_len(nrow(unmatched))) {
          
          # ---- reset per-unmatched-record triage state ----
          target_line_1      <- NA_character_
          comparisons_line_1 <- character(0)
          
          match_line_1      <- list()
          target_cluster    <- character(0)
          threshold_line1   <- 0.2
          match_check       <- NA_integer_
          
          cand <- data.table()
          best <- data.table()
          
          # then set the target and comparisons
          target_line_1      <- unmatched[u_idx, address_line_1]
          comparisons_line_1 <- c(match_to$address_line_1, target_line_1)
          
          
          # Isolate the addresses for exact test comparison
          target_line_1 <- unmatched[u_idx, address_line_1]
          comparisons_line_1 <- c(match_to$address_line_1, target_line_1)
          
          # Exact match tests for the address_line_1 and whole address
          match_line_1 <- find_similar_addresses(comparisons_line_1, threshold = 0)
          
          
          # -- a. Exact Match on address_line_1 with Conflicting Metadata ----------
          
          if( any(match_to$address_line_1 == target_line_1) ) {
            
            # Join verified candidates to unmatched records on address_line_1.
            # `allow.cartesian = TRUE` handles the many-to-many relationship.
            cand <- merge(
              match_to,
              unmatched[u_idx, .(u_id, address_line_1, u_min = anchor_year_min, u_max = anchor_year_max)],
              by = "address_line_1",
              allow.cartesian = TRUE
            )
            
            # Classify relative position of candidate interval vs unmatched window
            cand[, dir := fifelse(
              anchor_year_max < u_min, "before",
              fifelse(anchor_year_min > u_max, "after", "overlap")
            )]
            
            # Distance from unmatched window [u_min, u_max] to candidate anchor interval
            #  - candidate entirely before: distance from its END to u_min
            #  - candidate entirely after : distance from its START to u_max
            #  - overlaps/touches         : 0
            cand[, dist := fifelse(
              dir == "before", u_min - anchor_year_max,
              fifelse(dir == "after", anchor_year_min - u_max, 0)
            )]
            
            # Pick best match:
            #   1) overlaps/touches always win (dir == "overlap")
            #   2) otherwise choose smallest dist, regardless of before/after
            #   3) tie-break within direction by the closest boundary:
            #        - before: larger anchor_year_max (closest end)
            #        - after : smaller anchor_year_min (closest start)
            best <- cand[
              order(
                u_id,
                dir != "overlap",                      # overlaps/touches first
                dist,                                  # then closest gap (before/after treated equally)
                fifelse(dir == "before", -anchor_year_max, 0), # tie-break: closest end if before
                fifelse(dir == "after",   anchor_year_min, 0)  # tie-break: closest start if after
              ),
              .SD[1],
              by = u_id
            ]
            
            
            # Update unmatched records with the associated verified address
            unmatched[u_idx, "verified_address"]     <- best[1, verified_address]
            unmatched[u_idx, "address_verified"]     <- "Exact match"
            unmatched[u_idx, "ver_geolocation_test"] <- "Override"
            unmatched[u_idx, "latitude_avg"]  <- best[1, latitude_avg]
            unmatched[u_idx, "longitude_avg"] <- best[1, longitude_avg]
            
            
            # -- b. Fuzzy Match on address_line_1 -----------------------------------
            
          } else {
            
            # -- i. Identify Candidate Fuzzy Matches -------------------------------
            
            # Fuzzy matching is run iteratively with decreasing similarity thresholds
            # until each unmatched address resolves to a single best candidate.
            # If no match is found at any threshold, the record falls skips to
            # section iii.
            
            # Starting string similarity comparator (line_1 only)
            threshold_line1 <- 0.2
            
            repeat {
              match_line_1 <- find_similar_addresses(comparisons_line_1, threshold = threshold_line1)
              
              # Identify which cluster(s) contain the target
              target_in_cluster <- vapply(
                match_line_1,
                function(cluster) any(cluster %in% target_line_1),
                logical(1)
              )
              
              # Pull out the target cluster (if present)
              if (!any(target_in_cluster)) {
                target_cluster <- character(0)
              } else {
                target_cluster <- unlist(match_line_1[target_in_cluster], use.names = FALSE)
              }
              
              # "Too many" means target cluster still has > 2 addresses
              too_many_line1 <- length(target_cluster) > 2
              
              # Stop when: target cluster is small enough, or threshold bottomed out,
              # or (after tightening) everything is singleton clusters.
              if (!too_many_line1 ||
                  threshold_line1 <= 0 ||
                  (threshold_line1 < 0.2 && all(vapply(match_line_1, length, integer(1)) == 1))) {
                break
              }
              
              threshold_line1 <- max(0, threshold_line1 - 0.01)
            }
            
            # How many addresses ended up in the target cluster?
            match_check <- length(target_cluster)
            
            
            # -- ii.  Associate Candidates and Perform Geolocation Test ------------
            
            if ( match_check == 2 ) {
              
              # Bind the fuzzy-matched address_line_1 candidates to the unmatched 
              # record, retaining only the fields needed for geolocation testing.
              cand <- bind_cols(
                match_to[match_to$address_line_1 %in% 
                           match_line_1[vapply(match_line_1, length, integer(1)) > 1][[1]], ],
                unmatched[u_idx, .(
                  u_id,
                  u_address_line_1 = address_line_1,      # ← renamed: unmatched record's street address
                  u_min            = anchor_year_min,
                  u_max            = anchor_year_max,
                  u_lat            = latitude_avg,
                  u_lon            = longitude_avg
                )]
              )
              
              setDT(cand)
              
              # Apply preference rules for match selection:
              #     - First prefer a verified match whose anchor-year interval
              #       overlaps (or touches) the unmatched anchor-year window.
              #     - If no overlap exists, select the verified match whose 
              #       anchor-year interval is closest to the unmatched window, 
              #       regardless of whether it falls before or after.
              #     - "Closest" is based on the smallest non-overlap gap between the 
              #       two year ranges (overlap/touch counts as zero); for 
              #       non-overlapping ranges, use the candidate's end year if it is 
              #       before the window, or its start year if it is after the window.
              cand[, `:=`(
                dir = fifelse(anchor_year_max < u_min, "before",
                              fifelse(anchor_year_min > u_max, "after", "overlap")),
                dist = fifelse(anchor_year_max < u_min, u_min - anchor_year_max,
                               fifelse(anchor_year_min > u_max, anchor_year_min - u_max, 0))
              )]
              
              # Test how similar the longitude and latitude are - 
              # change in degrees (~222 meters or 728 feet)
              cand[, `:=`(
                lat_diff = abs(latitude_avg  - u_lat),
                lon_diff = abs(longitude_avg - u_lon)
              )]
              
              cand[, ver_geolocation_test := (lat_diff < 0.002) & (lon_diff < 0.002)]
              
              # Select the best verified match per unmatched record using the 
              # preference rules (ONLY among coordinate-close candidates).
              best <- cand[ver_geolocation_test == TRUE][
                order(
                  u_id,
                  dir != "overlap",                      # overlaps/touches first
                  dist,                                  # then closest gap (before/after treated equally)
                  fifelse(dir == "before", -anchor_year_max, 0), # tie-break: closest end if before
                  fifelse(dir == "after",   anchor_year_min, 0)  # tie-break: closest start if after
                ),
                .SD[1],
                by = u_id
              ]
              
              if (nrow(best) == 0L) {
                
                # No candidate survived the geolocation test — flag as failed
                unmatched[u_idx, "ver_geolocation_test"] <- "FALSE"
                
              } else {
                
                # Geolocation test passed — assign the top candidate as the verified address.
                # u_address_line_1 confirms which unmatched street was resolved.
                unmatched[u_idx, "verified_address"]     <- best[1, verified_address]
                unmatched[u_idx, "address_verified"]     <- "Fuzzy match"
                unmatched[u_idx, "ver_geolocation_test"] <- "TRUE"
                unmatched[u_idx, "latitude_avg"]     <- best[1, latitude_avg]
                unmatched[u_idx, "longitude_avg"]    <- best[1, longitude_avg]
                
              }
              
              
              # -- iii. No Fuzzy Match Found ---------------------------------------
              
            } else if ( threshold_line1 < 0.2 && match_check > 2 ) {
              
              # Threshold reached 0 before candidates could be separated, or the
              # step-wise threshold failed to resolve to a single candidate —
              # no single best match could be isolated.
              unmatched[u_idx, "address_verified"] <- "No Separable Match"
              
            } else if ( match_check == 1 ) {
              
              # No verified match exists at any threshold
              unmatched[u_idx, "address_verified"] <- "FALSE"
              
            } else {
              
              # Fallback: condition was unanticipated — flag for manual review
              unmatched[u_idx, "address_verified"] <- "Unanticipated Error"
              
            }
          }
          
        }
        
        # Recombine all separated dataset partitions into a single unified dataset
        common <- Reduce(intersect, list(names(match_to), names(unmatched)))
        candidate_addresses <- rbindlist(list(match_to[, ..common], unmatched[, ..common]), use.names = TRUE)
        
        # ---- optional: clear triage temporaries after recombination ----
        rm(match_to, unmatched, cand, best, match_line_1, target_cluster)
      }
      
      
      # Organize columns
      candidate_addresses <- candidate_addresses %>%
        relocate(attempt_succeeded, .after = address_verified) %>%
        relocate(usps_status, .after = attempt_succeeded) %>%
        relocate(usps_status_detail, .after = usps_status) %>%
        relocate(ver_geolocation_test, .after = address_verified)
      setDT(candidate_addresses)
      
      # Ensure the rows are ordered by ascending year of observation.
      setorder(candidate_addresses, anchor_year_min)
      
    } 

    
    # Separate addresses into those that were verified and those that were not
    
    # ---- reset per-ABI verification state ----
    data.table::setDT(candidate_addresses)
    candidate_addresses <- data.table::copy(candidate_addresses)  # defensive: avoid shallow-copy warnings
    
    ok_vals <- c("TRUE", "Exact match", "Fuzzy match")
    
    if ("address_verified" %in% names(candidate_addresses)) {
      
      # Split into verified vs unverified (fresh copies so := is safe by-reference)
      ver_candidates <- data.table::copy(
        candidate_addresses[address_verified %in% ok_vals]
      )
      
      unver_candidates <- data.table::copy(
        candidate_addresses[!(address_verified %in% ok_vals)]
      )
      
    } else {
      
      # If address_verified doesn't exist, treat all as unverified
      unver_candidates <- data.table::copy(candidate_addresses)
      
      # Keep ver_candidates defined (0-row placeholder with same columns)
      ver_candidates <- candidate_addresses[0]
    }
    
    # Add row indices scoped to each subset
    ver_candidates[,   row_idx := .I]
    unver_candidates[, row_idx := .I]
    
    
    if ( verify_addresses == FALSE ||
         (verify_addresses == TRUE && !is.null(unver_candidates$do_api) && all(unver_candidates$do_api == FALSE)) ||
         (verify_addresses == TRUE && all(unver_candidates$verified_address == "No address match found", na.rm = TRUE)) ) {
      
      # Prepare unver_candidates for QC output:
      unver_candidates <- unver_candidates %>%
        #   - Initialize verification status and verified address storage fields.
        mutate(address_matched = NA_character_, matched_address = NA_character_) %>%
        #   - Initialize match_geolocation_test as NA for downstream geocoding checks.
        mutate(match_geolocation_test = NA_character_)
      
      # --------------------
      # LOOP PART B.iii.: Agnostically Resolve Record Heterogeneity
      
      # For addresses that could not be verified, route them through triage to
      # associate each record with an anchored address agnostic of validity:
      #
      #   a) Exact duplicate address_line_1 (metadata conflict):
      #      - Work within each exact address_line_1 group, and only match within 
      #        the same pred_city.
      #      - Partition rows into trusted anchors (city_match == TRUE) vs conflicts
      #        (city_match == FALSE/NA). If no trusted anchor exists, promote the 
      #        best conflict row to act as a surrogate anchor.
      #      - Choose the best anchor for each row by prioritizing:
      #          1) anchor-year interval overlap (or smallest year-gap),
      #          2) presence of geolocation,
      #          3) smallest lon/lat distance,
      #          4) stable tie-break (e.g., alphabetical).
      #      - Geolocation guardrail: test is skipped and denoted as "override"
      #      - Records whose pred_city has no anchor counterpart are left unmatched
      #        for the fuzzy pass.
      #      - Provenance fields: address_matched = "Exact match"; 
      #        match_geolocation_test - "Override".
      #
      #   b) Fuzzy match on address_line_1 (similar strings):
      #      - Form similarity clusters and select one anchor per pred_city within
      #        each cluster.
      #      - Define anchor identity as matched_address when present; otherwise use
      #        combined_address.
      #      - Anchor selection prefers: non-blank matched_address (especially prior
      #        "Exact*"), then earliest anchor_year_min, then stable tie-break; if no
      #        prior match exists, fall back to a “best” anchor based on geo-guardrail
      #        pass rate, mean year gap, geo coverage, then stable key.
      #      - Geolocation guardrail: if both sides have coords and either
      #        $$|\Delta lon| > 0.02$$ or $$|\Delta lat| > 0.02$$, reject the pair;
      #        if coords are missing on either side, keep the pair eligible.
      #      - Only apply the fuzzy match when geo_valid == TRUE; if geo_valid == FALSE,
      #        leave the record as unverified/unmatched.
      #      - Provenance fields: address_matched = "Fuzzy match"; match_geolocation_test
      #        indicates whether the geo check was evaluated/passed.
      #
      #   Both passes:
      #   - First-write wins: only fill blank/NA fields; never overwrite earlier, higher-
      #     confidence results.
      #   - At most one match per record
      
      # ---- reset per-ABI verification state ----
      check_city <- data.table()
      comparisons_line_1 <- character(0)
      
      # ---- Verify city vs ZIP (prep for downstream matching) ----
      # Derive pred_city from zipcode using zip_city_lookup, then flag whether the
      # observed city matches the ZIP-predicted city (case/whitespace-insensitive).
      
      check_city <- copy(unver_candidates)
      
      check_city <- check_city %>%
        mutate(
          pred_city = ifelse(
            is.na(zipcode) | zipcode == "",
            NA_character_,
            vapply(
              zipcode, zip_to_pred_city,
              FUN.VALUE = character(1),
              zip_city_lookup = zip_city_lookup
            )
          )
        ) %>%
        mutate(
          city_match = case_when(
            # leave as NA when either side is missing/blank
            is.na(pred_city) | pred_city == "" | is.na(city) | city == "" ~ NA,
            # otherwise compare normalized strings
            TRUE ~ trimws(tolower(pred_city)) == trimws(tolower(city))
          )
        )
      
      # Convert to data.table for fast keyed joins/updates downstream
      setDT(check_city)
      
      # ---- String-similarity grouping on address_line_1 ----
      # Vector of street-line strings used for similarity comparisons
      comparisons_line_1 <- check_city$address_line_1
      
      if (length(comparisons_line_1) > 1) {
        
        # ---- reset per-ABI verification state ----
        match_line_1       <- list()
        similar_line_1     <- list()
        which_similar      <- list()
        n_matches          <- integer(0)
        
        # threshold = 0   : exact matches only
        # threshold = 0.2 : "similar enough" matches (fuzzy groups)
        match_line_1   <- find_similar_addresses(comparisons_line_1, threshold = 0)
        similar_line_1 <- find_similar_addresses(comparisons_line_1, threshold = 0.2)
        
        # For each exact-match representative, count how many times it appears in 
        # the input
        n_matches <- vapply(
          unlist(match_line_1, use.names = FALSE),
          function(v) sum(comparisons_line_1 == v, na.rm = TRUE),
          integer(1)
        )
        
        # Keep only fuzzy groups that actually contain >1 address (i.e., worth matching)
        which_similar <- similar_line_1[
          vapply(similar_line_1, function(x) length(x) > 1, logical(1))
        ]
        
        
        # -- a. Exact duplicate address_line_1 --------------------------------------
        # Goal:
        #   For each address_line_1 that appears more than once, use within-line_1 
        #   evidence to map records to a “best” anchor so later fuzzy steps face 
        #   fewer conflicts.
        #
        # Grouping:
        #   exact_matches = address_line_1 values with frequency > 1 in check_city.
        # 
        # Within each exact address_line_1 group:
        #   dtT (trusted)   = city_match %in% TRUE   (eligible anchors)
        #   dtF (untrusted) = everything else        (rows to resolve)
        #
        # Anchor selection:
        #   - If dtT exists: choose ONE anchor per join_city (typically pred_city) 
        #     from dtT via pick_best(), scored against the full group (dtT ∪ dtF).
        #   - If dtT empty: choose a single anchor from dtF, scored within dtF.
        #
        # Candidate generation:
        #   Join unresolved rows to anchor(s) on join_city (pred_city if present
        #   and city if not); exclude self-matches.
        #
        # Geo:
        #   Permissive: compute geo diffs if available for ordering/diagnostics, 
        #   but do NOT drop pairs based on geo thresholds in this exact-duplicate 
        #   pass.
        #
        # Write-back (first-write wins):
        #   Only fill blank fields; never overwrite existing matches.
        #   Tags:
        #     address_matched  = "Exact match"
        #     match_geolocation_test = "Override" (exact linkage accepted; geo not 
        #     enforced here)
        
        
        if (any(n_matches > 1L)) {
          
          # ---- reset per-ABI verification state ----
          exact_matches <- character(0)
          result        <- data.table()
          
          # address_line_1 values that occur more than once (exact duplicates on line_1)
          exact_matches <- names(n_matches[n_matches > 1])
          
          choose_match <- vector("list", length(exact_matches))
          
          for (j in seq_along(exact_matches)) {
            
            # ---- reset per-ABI verification state ----
            options       <- data.table()
            dtT           <- data.table()
            dtF           <- data.table()
            match_pool    <- data.table()
            best_anchor   <- data.table()
            dtF_to_match  <- data.table()
            cand          <- data.table()
            
            # All rows for this street address_line_1 value
            options <- check_city[address_line_1 %in% exact_matches[j]]
            
            # dtT = city/ZIP consistent (trusted anchors)
            # dtF = city/ZIP conflict or unverified (rows to resolve)
            dtT <- options[!is.na(city_match) & city_match == TRUE]
            dtF <- options[is.na(city_match) | city_match == FALSE]
            
            # Both empty → nothing to do
            if (nrow(dtT) == 0L && nrow(dtF) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # --- Choose anchor(s) PER pred_city and define the rows-to-annotate --------
            if (nrow(dtT) == 0L) {
              # No trusted anchors → anchors come from dtF, per pred_city, scored against dtF
              match_pool   <- dtF
              best_anchor  <- pick_best_by_city(dtF, match_pool, geo_tol = 0.02)
              
              # Rows to annotate = all non-anchor rows from dtF
              dtF_to_match <- dtF[!best_anchor, on = .(combined_address)]
              match_tag    <- "no_trusted_anchor"
              
            } else if (nrow(dtF) == 0L) {
              # All rows trusted → anchors from dtT, per pred_city, scored against dtT
              match_pool   <- dtT
              best_anchor  <- pick_best_by_city(dtT, match_pool, geo_tol = 0.02)
              
              # Rows to annotate = all non-anchor rows from dtT
              dtF_to_match <- dtT[!best_anchor, on = .(combined_address)]
              match_tag    <- "trusted_only"
              
            } else {
              # Both sides present → anchors from dtT, per pred_city, scored against dtT ∪ dtF
              match_pool   <- rbindlist(list(dtT, dtF), use.names = TRUE, fill = TRUE)
              best_anchor  <- pick_best_by_city(dtT, match_pool, geo_tol = 0.02)
              
              # Rows to annotate = everything in match_pool except the chosen anchors
              dtF_to_match <- match_pool[!best_anchor, on = .(combined_address)]
              match_tag    <- "conflict_resolved"
            }
            
            # If excluding the anchor(s) left nothing to match, nothing left to annotate
            if (nrow(dtF_to_match) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # Stable row ID belongs on the rows-to-annotate, not on the anchor(s)
            dtF_to_match[, row_id := .I]
            
            # helper condition: treat "No Matches" (and optionally NA/"") as unusable pred_city
            setDT(best_anchor)
            setDT(dtF_to_match)
            
            dtF_to_match[, join_city := fifelse(
              !is.na(pred_city) & pred_city != "No Matches" & pred_city != "",
              pred_city,
              city
            )]
            
            best_anchor[, join_city := fifelse(
              !is.na(pred_city) & pred_city != "No Matches" & pred_city != "",
              pred_city,
              city
            )]
            
            # --- Pair each row-to-annotate with its city's best anchor -----------------
            # IMPORTANT: because best_anchor is one row per pred_city, this is a 1:1 join
            # on pred_city for typical data (allow.cartesian is defensive).
            cand <- best_anchor[
              dtF_to_match,
              on = .(join_city = join_city),
              allow.cartesian = TRUE,
              nomatch = NA,
              .(
                row_id,
                combined_address_i       = i.combined_address,
                address_line_1_i         = i.address_line_1,
                city_i                   = i.city,
                state_i                  = i.state,
                zipcode_i                = i.zipcode,
                anchor_min_i             = i.anchor_year_min,
                anchor_max_i             = i.anchor_year_max,
                latitude_i               = i.latitude_avg,
                longitude_i              = i.longitude_avg,
                pred_city                = i.pred_city,
                
                matched_combined_address = combined_address,
                matched_anchor_min       = anchor_year_min,
                matched_anchor_max       = anchor_year_max,
                matched_city             = city,
                matched_zipcode          = zipcode,
                matched_latitude         = latitude_avg,
                matched_longitude        = longitude_avg
              )
            ]
            
            if (nrow(cand) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # Drop rows with no join_city match in the pool — left for the fuzzy step
            cand <- cand[!is.na(matched_combined_address)]
            if (nrow(cand) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # Drop self-matches (can happen if best_anchor was chosen from match_pool and
            # that same row is in dtF_to_match due to non-unique combined_address, etc.)
            cand <- cand[combined_address_i != matched_combined_address]
            if (nrow(cand) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # --- Score each candidate pair (for later ordering/selection) --------------
            cand[, `:=`(
              overlap  = (anchor_min_i <= matched_anchor_max) & (matched_anchor_min <= anchor_max_i),
              year_gap = fifelse(
                (anchor_min_i <= matched_anchor_max) & (matched_anchor_min <= anchor_max_i),
                0,
                pmin(abs(anchor_min_i - matched_anchor_max),
                     abs(matched_anchor_min - anchor_max_i))
              ),
              has_geo  = !is.na(longitude_i) & !is.na(latitude_i) &
                !is.na(matched_longitude) & !is.na(matched_latitude),
              lon_diff = abs(longitude_i - matched_longitude),
              lat_diff = abs(latitude_i  - matched_latitude)
            )]
            
            # Tag how this candidate linkage was produced (for auditing/debugging)
            cand[, `:=`(
              match_type = match_tag,
              geo_valid  = has_geo == TRUE
            )]
            
            choose_match[[j]] <- cand
          }
          
          # Flatten all exact address_line_1 groups into one table
          result <- rbindlist(Filter(nrow, choose_match), use.names = TRUE, fill = TRUE)
          
          if (nrow(result) > 0L) {
            
            # Provenance tags for this pass:
            # - "Exact match": sourced from exact duplicate address_line_1 linkage
            # - "Override": geo is not used as a hard guardrail in this pass
            result[, `:=`(
              address_matched  = "Exact match",
              match_geolocation_test = "Override"
            )]
            
            # Make row_id globally unique before deduplication (it resets per group)
            result[, row_id := .I]
            
            # One mapping per input record:
            # Keep the “best” candidate per combined_address_i (smallest year_gap,
            # then closest geo when available, then stable tie-break).
            setorder(result, combined_address_i, year_gap, lon_diff, lat_diff, matched_combined_address)
            result <- result[!duplicated(combined_address_i)]
            
            setDT(check_city)
            
            # Ensure all target columns exist before the update join
            for (col in c("matched_address", "address_matched", "match_geolocation_test", "match_type")) {
              if (!col %in% names(check_city)) {
                check_city[, (col) := NA_character_]
              }
            }
            
            # Write match results back onto check_city (first-write wins).
            # Only fill blank/NA fields so earlier, higher-confidence passes are not overwritten.
            check_city[
              result,
              on = .(combined_address = combined_address_i),
              `:=`(
                matched_address = fifelse(
                  is.na(matched_address) | matched_address == "",
                  i.matched_combined_address,
                  matched_address
                ),
                address_matched = fifelse(
                  is.na(address_matched) | trimws(address_matched) == "",
                  i.address_matched,
                  address_matched
                ),
                match_geolocation_test = fifelse(
                  is.na(address_matched) | trimws(address_matched) == "",
                  i.match_geolocation_test,
                  match_geolocation_test
                ),
                match_type = fifelse(
                  is.na(address_matched) | trimws(address_matched) == "",
                  i.match_type,
                  match_type
                )
              )
            ]
          }
        }
        
        
        # -- b. Fuzzy match on address_line_1 ---------------------------------------
        # Goal:
        #   Within each cluster of similar address_line_1 values, choose ONE 
        #   anchor per join_city and map currently-unmatched rows in that 
        #   join_city to it.
        #
        # Anchors (resolved BEFORE joining):
        #   Tier 1 (reuse prior confirmed match):
        #     If any row in join_city has non-blank matched_address, promote 
        #     ONE best target: prefer Exact/Override-like, then earliest 
        #     anchor_year_min, then alphabetic.
        #   Tier 2 (no prior match):
        #     Eligible rows: join_city present AND city_match %in% TRUE AND 
        #     still unmatched. pick_best() selects one anchor per join_city 
        #     using the full cluster as context.
        #
        # Matching:
        #   Only attempt matches for currently-unmatched rows (first-write wins).
        #   Exclude self-matches.
        #
        # Geo guardrail (per-coordinate):
        #   Test lon only when both lon values exist; test lat only when both l
        #   at values exist. A pair FAILS if any tested dimension exceeds 
        #   tolerance (0.02°) and is dropped. If a dimension can’t be tested 
        #   (missing on either side), the pair is retained and the non-NA 
        #   coordinate is carried forward to populate missing geo.
        #
        # Selection + tagging:
        #   One match per record: prefer full geo PASS, then partial PASS, then 
        #   closest diffs.
        # 
        #   address_matched  = "Fuzzy match"
        #   match_geolocation_test = "PASS" / "PASS; no Lon" / "PASS; no Lat" / "PASS; no Lon/Lat"
        #   Geo failures are recorded as "FAIL" and stamped only if the record remains unmatched.
        
        if (length(which_similar) > 0L) {
          
          choose_match    <- vector("list", length(which_similar))
          # NEW: one geo-fail collector per similarity cluster (must match which_similar length)
          choose_geo_fail <- vector("list", length(which_similar))
          
          for (j in seq_along(which_similar)) {
            
            # ---- reset per-ABI verification state ----
            options          <- data.table()
            full_options     <- data.table()
            all_join_cities  <- character(0)
            prior_match_pool <- data.table()
            prior_best       <- data.table()
            valid_anchors    <- data.table()
            tier2_cities     <- character(0)
            tier1_anchors    <- data.table()
            tier2_anchors    <- data.table()
            anchors          <- data.table()
            geo_lookup       <- data.table()
            options_to_join  <- data.table()
            m_best           <- data.table()
            m_fail           <- data.table()
            fail_result      <- data.table()
            # scalars / per-group scratch values (initialize so stale values can't be reused)
            target_addr      <- NA_character_
            meta             <- data.table()
            
            # ── 0. Subset for this similarity cluster ───────────────────────────────
            # options = every record in the current “similar address_line_1” cluster.
            options <- check_city[address_line_1 %in% unlist(which_similar[j])]
            
            if (nrow(options) == 0L) {
              choose_match[[j]]    <- data.table()
              choose_geo_fail[[j]] <- data.table()
              next
            }
            
            # match_target is the canonical “self identifier” for Step 3 and beyond.
            # It is set ONCE to combined_address and used consistently for self-match checks.
            options[, match_target := combined_address]
            
            # full_options = frozen copy of the full cluster used as the reference_pool
            # for scoring Tier 2 anchors (so ranking sees the entire cluster context).
            full_options <- copy(options)
            
            # ── 0b. Build join key (pred_city unless unusable, else city) ───────────
            options[, join_city := fifelse(
              !is.na(pred_city) & pred_city != "No Matches" & trimws(pred_city) != "",
              pred_city,
              city
            )]
            
            all_join_cities <- unique(options$join_city)
            all_join_cities <- all_join_cities[!is.na(all_join_cities) & trimws(all_join_cities) != ""]
            
            if (length(all_join_cities) == 0L) {
              choose_match[[j]]    <- data.table()
              choose_geo_fail[[j]] <- data.table()
              next
            }
            
            # ── 0c. Tier 1 — one pre-resolved matched_address per join_city ─────────
            prior_match_pool <- options[
              !is.na(join_city) & trimws(join_city) != "" &
                !is.na(matched_address) & trimws(matched_address) != ""
            ]
            
            if (nrow(prior_match_pool) > 0L) {
              prior_match_pool[, exactish := grepl("^Exact", address_matched, ignore.case = TRUE)]
              setorder(prior_match_pool, join_city, -exactish, anchor_year_min, matched_address)
              
              prior_best <- prior_match_pool[
                , .SD[1L],
                by = join_city
              ][
                , .(join_city, prior_matched_address = matched_address)
              ]
            } else {
              prior_best <- data.table(
                join_city             = character(),
                prior_matched_address = character()
              )
            }
            
            # ── 0d. Tier 2 — valid anchor pool for join_city groups with no prior match ───
            valid_anchors <- options[
              !is.na(join_city) & trimws(join_city) != "" &
                city_match %in% TRUE &
                (is.na(matched_address) | trimws(matched_address) == "")
            ]
            
            tier2_cities  <- setdiff(all_join_cities, prior_best$join_city)
            valid_anchors <- valid_anchors[join_city %chin% tier2_cities]
            
            if (nrow(prior_best) == 0L & nrow(valid_anchors) == 0L) {
              choose_match[[j]]    <- data.table()
              choose_geo_fail[[j]] <- data.table()
              next
            }
            
            # ── 1a. Tier 1 anchors — directly from pre-resolved prior_best ──────────
            if (nrow(prior_best) > 0L) {
              
              tier1_anchors <- prior_best[, {
                target_addr <- prior_matched_address[1L]
                
                meta <- options[combined_address == target_addr]
                if (nrow(meta) == 0L) meta <- options[join_city == .BY$join_city]
                meta <- meta[1L]
                
                .(
                  match_target    = target_addr,
                  anchor_year_min = meta$anchor_year_min,
                  anchor_year_max = meta$anchor_year_max
                )
              }, by = join_city]
              
            } else {
              tier1_anchors <- data.table(
                join_city      = character(),
                match_target    = character(),
                anchor_year_min = integer(),
                anchor_year_max = integer()
              )
            }
            
            # ── 1b. Tier 2 anchors — pick_best() for join_city groups with no prior match ─
            if (nrow(valid_anchors) > 0L) {
              
              tier2_anchors <- valid_anchors[
                ,
                {
                  dt <- copy(.SD)
                  a  <- pick_best(copy(dt), geo_tol = 0.02, reference_pool = copy(full_options))
                  .(
                    match_target    = a$match_target,
                    anchor_year_min = a$anchor_year_min,
                    anchor_year_max = a$anchor_year_max
                  )
                },
                by = join_city
              ]
              
            } else {
              tier2_anchors <- data.table(
                join_city      = character(),
                match_target    = character(),
                anchor_year_min = integer(),
                anchor_year_max = integer()
              )
            }
            
            # ── 1c. Combine — Tier 1 takes precedence; Tier 2 fills remainder ───────
            anchors <- unique(
              rbindlist(list(tier1_anchors, tier2_anchors), use.names = TRUE, fill = TRUE),
              by = "join_city"
            )
            
            # ── 2. Attach anchor coordinates ────────────────────────────────────────
            # match_target is a combined_address string → lookup by combined_address
            geo_lookup <- options[
              !is.na(longitude_avg) & !is.na(latitude_avg),
              .(anchor_lon = longitude_avg[1L], anchor_lat = latitude_avg[1L]),
              by = combined_address
            ]
            setnames(geo_lookup, "combined_address", "match_target")
            
            anchors <- geo_lookup[anchors, on = "match_target"]
            
            # ── 2b. Slim anchors to join-essential columns only ─────────────────────
            anchors <- anchors[, .(
              join_city,
              match_target,
              anchor_year_min,
              anchor_year_max,
              anchor_lon,
              anchor_lat
            )]
            
            # ── 3. Join anchor onto candidate rows (ONLY if currently unmatched) ────
            options_to_join <- options[is.na(matched_address) | trimws(matched_address) == ""]
            
            if (nrow(options_to_join) == 0L) {
              m_best <- data.table()
            } else {
              m_best <- anchors[
                options_to_join,
                on  = .(join_city),
                allow.cartesian = TRUE,
                nomatch = 0L,
                .(
                  join_city,
                  
                  combined_address_i = i.combined_address,
                  address_line_1_i   = i.address_line_1,
                  city_i             = i.city,
                  state_i            = i.state,
                  zipcode_i          = i.zipcode,
                  
                  anchor_year_min_i  = i.anchor_year_min,
                  anchor_year_max_i  = i.anchor_year_max,
                  latitude_i         = i.latitude_avg,
                  longitude_i        = i.longitude_avg,
                  
                  matched_combined_address = match_target,
                  matched_year_min         = anchor_year_min,
                  matched_year_max         = anchor_year_max,
                  matched_latitude         = anchor_lat,
                  matched_longitude        = anchor_lon
                )
              ][combined_address_i != matched_combined_address]  # exclude self-matches
            }
            
            if (nrow(m_best) == 0L) {
              choose_match[[j]]    <- data.table()
              choose_geo_fail[[j]] <- data.table()
              next
            }
            
            # ── 4. Geo guardrail — per-coordinate diffs + nuanced messages ──────────
            tol_lon <- 0.02
            tol_lat <- 0.02
            
            m_best[, `:=`(
              lon_testable = !is.na(longitude_i) & !is.na(matched_longitude),
              lat_testable = !is.na(latitude_i)  & !is.na(matched_latitude),
              
              lon_diff = abs(longitude_i - matched_longitude),
              lat_diff = abs(latitude_i  - matched_latitude)
            )]
            
            m_best[, `:=`(
              lon_ok = (!lon_testable) | (lon_diff <= tol_lon),
              lat_ok = (!lat_testable) | (lat_diff <= tol_lat)
            )]
            
            m_best[, geo_valid := lon_ok & lat_ok]
            
            m_best[, match_geolocation_test := fcase(
              geo_valid == FALSE, "FAIL",
              
              geo_valid == TRUE & lon_testable == TRUE & lat_testable == TRUE,  "PASS",
              geo_valid == TRUE & lon_testable == FALSE & lat_testable == TRUE, "PASS; no Lon",
              geo_valid == TRUE & lon_testable == TRUE  & lat_testable == FALSE,"PASS; no Lat",
              geo_valid == TRUE & lon_testable == FALSE & lat_testable == FALSE,"PASS; no Lon/Lat",
              
              default = NA_character_
            )]
            
            # record failures only when at least one dimension was tested
            m_fail <- m_best[(lon_testable | lat_testable) & geo_valid == FALSE]
            
            if (nrow(m_fail) > 0L) {
              choose_geo_fail[[j]] <- m_fail[, .(
                combined_address_i,
                match_geolocation_test,                 # "FAIL"
                address_matched  = "Fuzzy match",
                match_type       = "geo_guardrail_failed"
              )]
            } else {
              choose_geo_fail[[j]] <- data.table()
            }
            
            # keep only passing pairs
            m_best <- m_best[geo_valid == TRUE]
            
            # fill missing coordinates using the non-NA value (candidate takes precedence)
            m_best[, `:=`(
              final_longitude = fifelse(!is.na(longitude_i), longitude_i, matched_longitude),
              final_latitude  = fifelse(!is.na(latitude_i),  latitude_i,  matched_latitude)
            )]
            
            if (nrow(m_best) == 0L) {
              choose_match[[j]] <- data.table()
              next
            }
            
            # ── 5. Select one best match per input row ──────────────────────────────
            m_best[, `:=`(
              geo_full_pass    = (lon_testable == TRUE & lat_testable == TRUE), # geo_valid already TRUE
              geo_partial_pass = (lon_testable == FALSE | lat_testable == FALSE)
            )]
            
            setorder(
              m_best,
              combined_address_i,
              -geo_full_pass,
              -geo_partial_pass,
              lon_diff, lat_diff,
              matched_combined_address
            )
            
            m_best <- m_best[, .SD[1L], by = combined_address_i]
            choose_match[[j]] <- m_best
          }
          
          # ── 6. Write matches back to check_city (first-write wins) ───────────────
          result <- rbindlist(Filter(nrow, choose_match), use.names = TRUE, fill = TRUE)
          
          if (nrow(result) > 0L) {
            
            # tag provenance
            result[, address_matched := "Fuzzy match"]
            if (!"match_type" %in% names(result)) result[, match_type := NA_character_]
            
            # ensure uniqueness (Step 5 already does this, but keep safe)
            setorder(
              result,
              combined_address_i,
              -geo_full_pass, -geo_partial_pass,
              lon_diff, lat_diff,
              matched_combined_address
            )
            result <- result[!duplicated(combined_address_i)]
            
            setDT(check_city)
            
            # ensure target columns exist (keep lat/lon numeric)
            if (!"matched_address" %in% names(check_city))  check_city[, matched_address := NA_character_]
            if (!"address_matched" %in% names(check_city))  check_city[, address_matched := NA_character_]
            if (!"match_geolocation_test" %in% names(check_city)) check_city[, match_geolocation_test := NA_character_]
            if (!"match_type" %in% names(check_city))       check_city[, match_type := NA_character_]
            if (!"longitude_avg" %in% names(check_city))    check_city[, longitude_avg := as.numeric(NA)]
            if (!"latitude_avg" %in% names(check_city))     check_city[, latitude_avg := as.numeric(NA)]
            
            # PASS predicate (PASS / PASS; no Lon / etc.)
            result[, geo_pass := !is.na(match_geolocation_test) & substr(match_geolocation_test, 1L, 4L) == "PASS"]
            
            check_city[
              result,
              on = .(combined_address = combined_address_i),
              `:=`(
                matched_address = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") & i.geo_pass,
                  i.matched_combined_address,
                  matched_address
                ),
                
                address_matched = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(address_matched) | trimws(address_matched) == ""),
                  i.address_matched,
                  address_matched
                ),
                
                match_geolocation_test = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(match_geolocation_test) | trimws(match_geolocation_test) == ""),
                  i.match_geolocation_test,
                  match_geolocation_test
                ),
                
                match_type = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(match_type) | trimws(match_type) == ""),
                  i.match_type,
                  match_type
                ),
                
                # populate missing geo (numeric-safe)
                longitude_avg = fifelse(
                  is.na(longitude_avg) & !is.na(i.final_longitude),
                  i.final_longitude,
                  longitude_avg
                ),
                latitude_avg = fifelse(
                  is.na(latitude_avg) & !is.na(i.final_latitude),
                  i.final_latitude,
                  latitude_avg
                )
              )
            ]
          }
          
          # ── 7. Mark geo failures (no match written) ─────────────────────────────
          # These are rows where at least one coordinate dimension was testable and
          # the pair failed tolerance. We stamp match_geolocation_test = "FAIL" only if the
          # record is still unmatched (first-write wins) and match_geolocation_test is blank.
          
          fail_result <- rbindlist(Filter(nrow, choose_geo_fail), use.names = TRUE, fill = TRUE)
          
          if (nrow(fail_result) > 0L) {
            
            setDT(check_city)
            
            # Ensure columns exist
            for (col in c("match_geolocation_test", "address_matched", "match_type")) {
              if (!col %in% names(check_city)) {
                check_city[, (col) := NA_character_]
              }
            }
            
            # One flag per input record (if multiple failures, just keep the first)
            setorder(fail_result, combined_address_i)
            fail_result <- fail_result[!duplicated(combined_address_i)]
            
            check_city[
              fail_result,
              on = .(combined_address = combined_address_i),
              `:=`(
                match_geolocation_test = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(match_geolocation_test) | trimws(match_geolocation_test) == ""),
                  i.match_geolocation_test,   # should be "FAIL"
                  match_geolocation_test
                ),
                address_matched = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(address_matched) | trimws(address_matched) == ""),
                  i.address_matched,    # "Fuzzy match"
                  address_matched
                ),
                match_type = fifelse(
                  (is.na(matched_address) | trimws(matched_address) == "") &
                    (is.na(match_type) | trimws(match_type) == ""),
                  i.match_type,         # "geo_guardrail_failed"
                  match_type
                )
              )
            ]
          }
        }
        
        # ---- Update unver_candidates from check_city (by combined_address) ----
        # Only fill unver_candidates fields when they are blank/NA (first-write wins).
        setDT(check_city)
        setDT(unver_candidates)
        
        unver_candidates[
          check_city,
          on = .(combined_address),
          `:=`(
            matched_address = fifelse(
              is.na(matched_address) | trimws(matched_address) == "",
              i.matched_address,
              matched_address
            ),
            address_matched = fifelse(
              is.na(address_matched) | trimws(address_matched) == "",
              i.address_matched,
              address_matched
            ),
            match_geolocation_test = fifelse(
              is.na(match_geolocation_test) | trimws(match_geolocation_test) == "",
              i.match_geolocation_test,
              match_geolocation_test
            )
          )
        ]
      } else if (length(comparisons_line_1) == 1) {
        unver_candidates$address_matched <- "Only one address"
      }
      
      # Ensure the rows are ordered by ascending year of observation.
      setorder(unver_candidates, anchor_year_min)
      
    }
    
    
    # Combine, filling any columns unique to one subset with NA
    candidate_addresses <- rbindlist(
      list(ver_candidates, unver_candidates),
      use.names = TRUE,
      fill      = TRUE
    )
    
    # Verify no duplicate combined_address records were introduced by the
    # address matching and write-back steps.
    induced_duplicates <- candidate_addresses[, c("combined_address", "anchor_year_min", "anchor_year_max")] %>%
      duplicated() %>%
      any()
    
    
    # Going forward, the intention is to use the verified or matched address
    # as the unique entry per ABI. This means the geolocation needs to be averaged
    # over the newly prepared matches (through verification or string similarity).
    
    # --- Add best match key for rejoining in PART E ---
    candidate_addresses <- candidate_addresses %>%
      mutate(
        # Create a single “best available” address string to use for downstream matching
        # Priority:
        # 1) verified_address (trimmed), unless it literally equals "No address match found"
        # 2) matched_address
        # 3) combined_address (fallback)
        best_address = (\(x) {
          # Use verified_address only if the column exists; otherwise treat as missing
          v <- if ("verified_address" %in% names(x)) x$verified_address else NA_character_
          
          # Use matched_address only if the column exists; otherwise treat as missing
          m <- if ("matched_address"  %in% names(x)) x$matched_address  else NA_character_
          
          coalesce(
            # Trim whitespace and convert the sentinel string to NA so it won’t “win”
            na_if(trimws(v), "No address match found"),
            # Otherwise use matched address if present
            m,
            # Finally fall back to the reported/combined address
            x$combined_address
          )
        })(pick(everything()))  # pass the current row’s columns into the function
      )
    
    # Work on a copy so we don’t accidentally modify candidate_addresses further
    reduced_candidate_addresses <- copy(candidate_addresses)
    
    reduced_candidate_addresses <- reduced_candidate_addresses %>%
      # Keep only what we need for collapsing to one row per unique address
      transmute(best_address, longitude_avg, latitude_avg) %>%
      # Aggregate to one row per best_address
      group_by(best_address) %>%
      summarise(
        # Extract 2-letter state from the end of the address:
        # matches "... , ST 12345" and "... , ST, 12345-6789"
        # Ensure exactly ONE value per group
        state = str_extract(
          str_to_upper(first(best_address)),
          "(?<=,\\s)[A-Z]{2}(?=,?\\s\\d{5}(?:-\\d{4})?$)"
        ),
        
        # Count rows that have BOTH lat and lon (usable coordinate pairs)
        n_address = sum(!is.na(latitude_avg) & !is.na(longitude_avg)),
        
        # Whether we have enough coordinates to do any spatial operation for this address
        enough_geo = n_address > 0,
        
        # Average coordinates across all usable rows for this address
        # (computed only when at least one usable coordinate pair exists)
        lat_avg = if (n_address > 0) mean(latitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
        lon_avg = if (n_address > 0) mean(longitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
        
        .groups = "drop"  # return an ungrouped data frame
      ) %>%
      # Convert to data.table (by reference); the anonymous function returns the same object
      (\(x) { setDT(x) })()
    
    
    # --------------------
    # LOOP PART C: Verify Geolocation with the US Census Bureau’s Geocoder Database
    
    # Helper to safely pull a field from each attempt
    pluck_or_na <- function(x, name) if (!is.null(x[[name]])) as.character(x[[name]]) else NA_character_
    
    out_row <- list()
    for (j in 1:nrow(reduced_candidate_addresses)) {
      address_to_check <- reduced_candidate_addresses[j, ] %>% pull(best_address)
      
      # -- i. Parse the Address into Its Components -----------------------------
      
      # Split the address into its components (assuming comma-separated format)
      parts <- trimws(strsplit(address_to_check, ",")[[1]])
      
      if (length(parts) < 3) {
        warning("Invalid address format. Please provide a full address.")
        return(NULL)
      }
      
      if (length(parts) == 3) {
        # street, city, "ST ZIP"
        street <- parts[1]
        city   <- parts[2]
        
        state_zip <- strsplit(parts[3], "\\s+")[[1]]
        if (length(state_zip) < 2) {
          warning("Invalid state and ZIP code format. Please provide a full address.")
          return(NULL)
        }
        state <- state_zip[1]
        zip   <- state_zip[2]
        
      } else {
        # The street may contain commas (address line 2), and state/zip may be 
        # split by commas. Take the last tokens as state + zip, and the one before 
        # them as city.
        last <- parts[length(parts)]
        
        # Case A: ... , ST ZIP
        # Case B: ... , ST, ZIP
        if (grepl("\\b[A-Z]{2}\\b\\s+\\d", last)) {
          # last is "ST ZIP"
          state_zip <- strsplit(last, "\\s+")[[1]]
          state <- state_zip[1]
          zip   <- state_zip[2]
          city  <- parts[length(parts) - 1]
          street <- paste(parts[1:(length(parts) - 2)], collapse = ", ")
        } else {
          # assume last is ZIP and previous is state
          zip   <- last
          state <- parts[length(parts) - 1]
          city  <- parts[length(parts) - 2]
          street <- paste(parts[1:(length(parts) - 3)], collapse = ", ")
        }
      }
      
      
      # -- ii. Run the Query ----------------------------------------------------
      
      res <- validate_geolocation(
        street = street,
        city   = city,
        state  = state,
        zip    = zip,
        tries  = geocoder_census_tries,
        quiet = TRUE
      )
      
      
      # -- iii. Save Result -----------------------------------------------------
      
      if (res$ok) {
        succeeded_i <- which(vapply(res$attempts, function(a) isTRUE(a$ok), logical(1)))[1]
        
        search_res <- tibble::tibble(
          geolocation_verified = "TRUE",
          n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
          query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                               FUN.VALUE = character(1))),
                                                collapse = " | "),
          geo_matched_address  = res$best$matched_address,
          benchmark            = res$best$benchmark,
          vintage_input        = res$best$vintage_input,
          latitude_ver         = res$best$lat,
          longitude_ver        = res$best$lon
        )
        
        out_row[[j]] <- dplyr::bind_cols(reduced_candidate_addresses[j, , drop = FALSE], search_res)
        
      } else {
        search_res <- tibble::tibble(
          geolocation_verified = "FALSE",
          n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
          query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                               FUN.VALUE = character(1))),
                                                collapse = " | "),
          geo_matched_address  = NA_character_,
          benchmark            = NA_real_,
          vintage_input        = NA_character_,
          latitude_ver         = NA_real_,
          longitude_ver        = NA_real_
        )
        
        out_row[[j]] <- dplyr::bind_cols(reduced_candidate_addresses[j, , drop = FALSE], search_res)
      }
    }
    
    # Save results
    reduced_candidate_addresses <- bind_rows(out_row)
  
  }
  
  
  # --------------------
  # LOOP PART D: Point-in-Polygon Spatial Assignment of Census Information
  
  # After handling entries with non-NA address_line_1, we can recombine
  # both components for geolocation annotation.
  
  if (any(is.na(subset$address_line_1))) {
    if (nrow(candidate_addresses) == 0L){
      induced_duplicates <- NA
    }
    
    # Rejoining needs to be handled differently depending if there were any
    # addresses without an NA.
    
    if(nrow(candidate_addresses) != 0L) {
      store_candidate_addresses <- copy(candidate_addresses)
      
      # Add matching column to the addresses not matched (missing address_line_1)
      dont_match <- dont_match %>% 
        mutate(best_address = combined_address)
      
      # Recreate candidate_addresses with all the new information
      candidate_addresses <- candidate_addresses[
        reduced_candidate_addresses,
        on = .(best_address),
        nomatch = NA
      ] %>%
        select(-i.state)
      
      # Add the missing entries, dont_match, with all their metadata
      candidate_addresses <- rbindlist(
        list(candidate_addresses, dont_match),
        use.names = TRUE,
        fill      = TRUE
      )
      candidate_addresses[, row_idx := .I]
      
      # Recreate the reduced_candidate_addresses structure for point-in-polygon 
      # matching using the values missing an address_line_1
      dont_match_intermediate <- candidate_addresses %>%
        # Keep only what we need for collapsing to one row per unique address
        transmute(best_address, longitude_avg, latitude_avg) %>%
        # Aggregate to one row per best_address
        group_by(best_address) %>%
        summarise(
          # Extract 2-letter state from the end of the address:
          # matches "... , ST 12345" and "... , ST, 12345-6789"
          # Ensure exactly ONE value per group
          state = str_extract(
            str_to_upper(first(best_address)),
            "(?<=,\\s)[A-Z]{2}(?=,?\\s\\d{5}(?:-\\d{4})?$)"
          ),
          
          # Count rows that have BOTH lat and lon (usable coordinate pairs)
          n_address = sum(!is.na(latitude_avg) & !is.na(longitude_avg)),
          
          # Whether we have enough coordinates to do any spatial operation for this address
          enough_geo = n_address > 0,
          
          # Average coordinates across all usable rows for this address
          # (computed only when at least one usable coordinate pair exists)
          lat_avg = if (n_address > 0) mean(latitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
          lon_avg = if (n_address > 0) mean(longitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
          
          # Clarify there was no address to verify the address_line_1
          geolocation_verified = "No address_line_1",
          
          .groups = "drop"  # return an ungrouped data frame
        ) %>%
        filter(best_address %in% dont_match$best_address)
      
      # Re-add the entries with no address_line_1
      reduced_candidate_addresses <- rbindlist(
        list(reduced_candidate_addresses, dont_match_intermediate),
        use.names = TRUE,
        fill      = TRUE
      )
      reduced_candidate_addresses[, row_id := .I]
      
      # Normalize candidate_addresses to the expected dimensions
      candidate_addresses <- candidate_addresses[, intersect(names(candidate_addresses), names(store_candidate_addresses)), with = FALSE]
    } else {
      
      # Recreate reduced_candidate_addresses for point-in-polygon matching
      reduced_candidate_addresses <- dont_match %>%
        mutate(best_address = combined_address) %>%
        # Keep only what we need for collapsing to one row per unique address
        transmute(best_address, longitude_avg, latitude_avg) %>%
        # Aggregate to one row per best_address
        group_by(best_address) %>%
        summarise(
          # Extract 2-letter state from the end of the address:
          # matches "... , ST 12345" and "... , ST, 12345-6789"
          # Ensure exactly ONE value per group
          state = str_extract(
            str_to_upper(first(best_address)),
            "(?<=,\\s)[A-Z]{2}(?=,?\\s\\d{5}(?:-\\d{4})?$)"
          ),
          
          # Count rows that have BOTH lat and lon (usable coordinate pairs)
          n_address = sum(!is.na(latitude_avg) & !is.na(longitude_avg)),
          
          # Whether we have enough coordinates to do any spatial operation for this address
          enough_geo = n_address > 0,
          
          # Average coordinates across all usable rows for this address
          # (computed only when at least one usable coordinate pair exists)
          lat_avg = if (n_address > 0) mean(latitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
          lon_avg = if (n_address > 0) mean(longitude_avg[!is.na(latitude_avg) & !is.na(longitude_avg)]) else NA_real_,
          
          # Clarify there was no address to verify the address_line_1
          geolocation_verified = "No address_line_1",
          
          .groups = "drop"  # return an ungrouped data frame
        )
      
      setDT(reduced_candidate_addresses)
      reduced_candidate_addresses[, row_id := .I]
      
      # Reinitialize candidate_addresses, which was made empty to prevent
      # string matching
      candidate_addresses <- dont_match %>%
        # Add matching column to the addresses not matched (missing address_line_1)
        mutate(best_address = combined_address)
      
    }
    
  } else {
    
    reduced_candidate_addresses <- reduced_candidate_addresses %>%
      mutate(
        # Create a stable integer key for joining results back later
        row_id = row_number(),
        
        # Build a single lon/lat per row, preferring verified coordinates then averaged coordinates
        lon = as.numeric(coalesce(longitude_ver, lon_avg)),
        lat = as.numeric(coalesce(latitude_ver,  lat_avg)),
        
        # Final flag: do we have both usable coordinates (after aggregation) AND a state code
        enough_info = enough_geo & !is.na(state)
      ) %>%
      # Put row_id first for readability and easier joins
      relocate(row_id, .before = best_address) %>%
      # Convert to data.table (by reference); the anonymous function returns the same object
      (\(x) { setDT(x) })
  }
  
  
  # ---- reset per-batch geo matching state (sf + joins) ----
  cand_pts            <- NULL
  census_results      <- data.table()
  core_areas_list     <- list()
  core_areas_results  <- data.table()
  vintages            <- list()
  cols_to_add         <- character(0)
  dont_match_processed <- data.table()
  
  # Define the columns you expect to add back
  geo_cols  <- c("row_id", "geoid_2000", "geoid_2010", "geoid_2020")
  
  core_cols <- c(
    "row_id",
    "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007",
    "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010",
    "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020",
    "zcta_2000", "zcta_2010", "zcta_2020"
  )
  
  # --- Prepare points for geo matching ---
  # Build an sf points table (only rows with usable lon/lat/state)
  cand_pts <- reduced_candidate_addresses %>%
    dplyr::as_tibble() %>%
    dplyr::transmute(
      row_id,
      state,
      lon = lon_avg,
      lat = lat_avg
    ) %>%
    dplyr::mutate(state = toupper(trimws(as.character(state)))) %>%
    dplyr::filter(!is.na(lon), !is.na(lat), !is.na(state), state != "") %>%
    {
      if (nrow(.) == 0) {
        # Return a 0-row sf object with the expected columns + empty geometry
        empty <- tibble::tibble(
          row_id = integer(),
          state  = character(),
          lon    = numeric(),
          lat    = numeric()
        )
        
        sf::st_sf(empty, geometry = sf::st_sfc(crs = 4326))
      } else {
        # Normal path: build POINT geometry from lon/lat
        sf::st_as_sf(., coords = c("lon", "lat"), crs = 4326, remove = FALSE)
      }
    }
  
  # Proceed with join only when valid points are available
  if (nrow(cand_pts) != 0) {
    
    # --- Add DECENNIAL census geography (Census blocks) for each vintage ---
    census_results <- add_decennial_geoid_block(
      cand_pts,
      blocks_by_state,
      geography = block_geography
    )
    data.table::setDT(census_results)
    
    # --- CBSA/CSA + ZCTA decoding for each vintage ---
    vintages <- list(
      list(cbsa_csa_year = 2007, cbsa_csa = core_areas$cbsa_csa_2007, zcta_year = "zcta_2000", zcta = core_areas$zcta_2000),
      list(cbsa_csa_year = 2010, cbsa_csa = core_areas$cbsa_csa_2010, zcta_year = "zcta_2010", zcta = core_areas$zcta_2010),
      list(cbsa_csa_year = 2020, cbsa_csa = core_areas$cbsa_csa_2020, zcta_year = "zcta_2020", zcta = core_areas$zcta_2020)
    )
    
    core_areas_list <- lapply(vintages, function(v) {
      out <- list()
      
      if (!is.null(v$cbsa_csa)) {
        out$cbsa_csa <- decode_cbsa_csa(
          cand_sf     = cand_pts,
          cbsa_csa_sf = v$cbsa_csa,
          year        = v$cbsa_csa_year,
          state_col   = "state"
        )
      }
      
      if (!is.null(v$zcta)) {
        out$zcta <- decode_zcta(
          cand_sf      = cand_pts,
          zcta_sf      = v$zcta,
          zcta_colname = v$zcta_year,
          state_col    = "state"
        )
      }
      
      out
    })
    
    # Start with cand_pts attributes (no geometry) and left-join decoded outputs
    core_areas_results <- sf::st_drop_geometry(cand_pts)
    
    for (v in core_areas_list) {
      if (!is.null(v$cbsa_csa)) {
        core_areas_results <- dplyr::left_join(core_areas_results, v$cbsa_csa, by = "row_id")
      }
      if (!is.null(v$zcta)) {
        core_areas_results <- dplyr::left_join(core_areas_results, v$zcta, by = "row_id")
      }
    }
    
    data.table::setDT(core_areas_results)
    
  } else {
    
    # -------------------------------------------------------------
    # No valid comparator points:
    # create correctly-shaped EMPTY results so joins/updates are safe.
    # -------------------------------------------------------------
    census_results <- data.table::as.data.table(setNames(
      replicate(length(geo_cols), character(0), simplify = FALSE),
      geo_cols
    ))
    
    core_areas_results <- data.table::as.data.table(setNames(
      replicate(length(core_cols), character(0), simplify = FALSE),
      core_cols
    ))
    
    # Optional: make row_id type consistent (helps with joins)
    census_results[, row_id := integer()]
    core_areas_results[, row_id := integer()]
  }
  
  # ---- Update reduced_candidate_addresses (safe even if results are empty) ----
  
  reduced_candidate_addresses[
    census_results[, ..geo_cols],
    on = "row_id",
    `:=`(
      geoid_2000 = i.geoid_2000,
      geoid_2010 = i.geoid_2010,
      geoid_2020 = i.geoid_2020
    )
  ]
  
  reduced_candidate_addresses[
    core_areas_results[, ..core_cols],
    on = "row_id",
    `:=`(
      cbsa_code_2007  = i.cbsa_code_2007,
      cbsa_level_2007 = i.cbsa_level_2007,
      csa_code_2007   = i.csa_code_2007,
      cbsa_code_2010  = i.cbsa_code_2010,
      cbsa_level_2010 = i.cbsa_level_2010,
      csa_code_2010   = i.csa_code_2010,
      cbsa_code_2020  = i.cbsa_code_2020,
      cbsa_level_2020 = i.cbsa_level_2020,
      csa_code_2020   = i.csa_code_2020,
      zcta_2000       = i.zcta_2000,
      zcta_2010       = i.zcta_2010,
      zcta_2020       = i.zcta_2020
    )
  ]
  
  # Ensure required columns exist even when no join happened
  needed_cols <- c("geoid_2000", "geoid_2010", "geoid_2020", "enough_info")
  
  for (nm in setdiff(needed_cols, names(reduced_candidate_addresses))) {
    # Default: GEOIDs should be NA_character_; enough_info default NA (or FALSE if you prefer)
    if (nm %chin% c("geoid_2000", "geoid_2010", "geoid_2020")) {
      reduced_candidate_addresses[, (nm) := NA_character_]
    } else if (nm == "enough_info") {
      reduced_candidate_addresses[, (nm) := NA]
    }
  }
  
  # Summarize match completeness across the three decennial block vintages
  reduced_candidate_addresses[, geoid_match :=
                                data.table::fifelse(
                                  !enough_info,
                                  "Not enough info",
                                  data.table::fifelse(
                                    is.na(geoid_2000) & is.na(geoid_2010) & is.na(geoid_2020),
                                    "Matches not found",
                                    data.table::fifelse(
                                      is.na(geoid_2000) | is.na(geoid_2010) | is.na(geoid_2020),
                                      "Some matches not found",
                                      "Matched"
                                    )
                                  )
                                )
  ]
  
  
  # Drop temporary helper columns and rejoin to candidate_addresses
  candidate_addresses <- candidate_addresses %>%
    # keep candidate_addresses’ columns, but drop old avg coords
    select(-any_of(c("n_geo", "anchor_year_min", "anchor_year_max",
                     "latitude_avg", "longitude_avg", "row_idx"))) %>%
    left_join(
      reduced_candidate_addresses %>%
        select(-any_of(c("row_id", "state", "lon", "lat"))) %>%  # drop fields you don’t want to carry over
        rename(longitude_avg = lon_avg, latitude_avg = lat_avg), # replace lon/lat averages by best address
      by = "best_address"                                        # join key
    ) %>% 
    relocate(longitude_avg, .after = zip4) %>%
    relocate(latitude_avg, .after = longitude_avg) %>%
    relocate(geoid_match, .after = enough_info)
  
  
  # --------------------
  # LOOP PART E: Add Back to Main Dataset
  
  # Define column structure
  combined_cols <- c(
    # ---- company / identifiers ----
    "archive_version_year", "company", "abi", "year_established", 
    "primary_naics6_code", "naics6_descriptions",
    "subsidiary_number", "company_holding_status",
    
    # ---- address ----
    "address_line_1", "city", "state", "zipcode", "zip4", "combined_address",
    "do_api", "address_verified", "ver_geolocation_test", "attempt_succeeded",  # validation cols
    "usps_status", "usps_status_detail", "verified_address", "address_matched", # validation cols
    "match_geolocation_test", "matched_address", "best_address",                # validation cols         # validation cols
    
    # ---- industry codes / descriptions ----
    "primary_sic_code", "sic6_descriptions",
    "sic_code", "sic6_descriptions_sic",
    "sic_code_1", "sic6_descriptions_sic1",
    "sic_code_2", "sic6_descriptions_sic2",
    "sic_code_3", "sic6_descriptions_sic3",
    "sic_code_4", "sic6_descriptions_sic4",
    
    # ---- geocoding / verification ----
    "latitude", "longitude", 
    "n_address", "enough_geo", "latitude_avg", "longitude_avg",   # validation cols
    "geolocation_verified", "n_attempts", "query_statuses",       # validation cols
    "geo_matched_address", "benchmark", "vintage_input",          # validation cols
    "latitude_ver", "longitude_ver",                              # validation cols
    
    # ---- census geography (current + vintages) ----
    "census_block", "census_tract", "county_code", "fips_code",
    "enough_info", "geoid_match", "geoid_2000", "geoid_2010", "geoid_2020",      # validation cols
    
    # ---- CBSA/CSA (current + vintages) and ZCTA (new) ----
    "cbsa_level", "cbsa_code", "csa_code",
    "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007", "zcta_2000",  # validation cols
    "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010", "zcta_2010",  # validation cols
    "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020", "zcta_2020",  # validation cols
    
    # ---- other firmographic fields ----
    "area_code", "site_number", "yellow_page_code", "office_size_code",
    "employee_size_location", "location_employee_size_code",
    "sales_volume_location", "location_sales_volume_code",
    "parent_number", "parent_actual_employee_size", "parent_employee_size_code",
    "parent_actual_sales_volume", "parent_sales_volume_code",
    
    # ---- status / misc identifiers ----
    "primary_naics2_code", "address_type_indicator", 
    "industry_specific_first_byte", "business_status_code",
    "population_code", "match_code", "ticker", "idcode"
  )
  
  # Merge the two datasets
  combined <- merge(
    candidate_addresses[, setdiff(names(candidate_addresses), "do_api"), with = FALSE],
    subset,
    by = c("address_line_1", "city", "state", "zipcode", "zip4", "combined_address"),
    all.x = TRUE
  ) %>%
    select(any_of(combined_cols))
  
  setorder(combined, archive_version_year)
  
  
  # --------------------
  # LOOP PART F: Quality Checks — Address Validation and Consolidation Results
  
  # Address verification is adaptable based on whether the user has opted to
  # use the USPS API. Regardless, the secondary goal is to consolidate addresses —
  # either to a USPS-verified form or via agnostic string similarity matching —
  # in order to improve geolocation accuracy and reduce unnecessary redundancy,
  # making downstream processing more efficient.
  #
  # Special cases where the same address_line_1 is associated with different
  # cities are also handled. In these cases, verified addresses (resolved via
  # the USPS API or the simplemaps zip code-to-city dataset) take precedence,
  # and unverified entries are reconciled to the nearest available verified
  # version based on temporal closeness.
  # 
  # Relevant QC tables are generated based on whether the subset was processed
  # via USPS API verification or agnostic string similarity. When USPS
  # verification is used, subsets may produce differently structured results
  # and QC outputs; however, these are expected to merge compatibly.
  # 
  # NOTE: If USPS API verification is enabled, address verification is attempted
  #       first. If no addresses are successfully verified, that subset is
  #       processed agnostically instead.
  
  # Validate that required columns exist in the final table: no loss of information.
  cmp <- compare_tabs(
    combined, subset,
    new_addr_col = "combined_address",
    old_addr_col = "combined_address",
    new_year_col = "archive_version_year",
    old_year_col = "archive_version_year"
  )
  
  qc_address$qc1[[i]] <- data.frame(
    abi                      = unique(combined$abi) %>% first(),
    allow_usps_api           = verify_addresses,
    api_used                 = 
      if (verify_addresses) {
        if ("do_api" %in% names(combined)) paste(unique(combined$do_api), collapse = ", ")
      } else {
        NA
      },
    verification_attempted   = !is.null(combined$verified_address),
    match_attempted          = !is.null(combined$matched_address),
    duplicates_induced       = induced_duplicates %>% first(), 
    any_addresses_line1_na   = na_addresses,
    
    # New vs Old (address+year) comparison
    new_not_in_old_addr_yr   = cmp$n_new_not_old,
    old_not_in_new_addr_yr   = cmp$n_old_not_new,
    new_vs_old_differ        = (cmp$n_new_not_old + cmp$n_old_not_new) > 0,
    
    check.names = FALSE
  )
  
  if ( !is.null(combined$verified_address) ) {
    # Address QC: summarize verification stages
    qc_address$qc2[[i]] <- combined %>%
      mutate(
        # Treat the "no match" sentinel as missing so it doesn't override the fallback
        verified_address = if_else(
          as.character(verified_address) == "No address match found",
          NA_character_,
          as.character(verified_address),
          missing = as.character(verified_address)
        ),
        # Prefer verified/standardized address; fall back to combined_address
        address = str_remove(coalesce(verified_address, combined_address), ",(?=\\s*\\d{5})"),
        reported_address = str_remove(combined_address, ",(?=\\s*\\d{5})")
      ) %>%
      select(
        abi, archive_version_year, address,
        
        # ---- address ----
        reported_address,
        
        # ---- verification ----
        address_verified, ver_geolocation_test, usps_status,
        usps_status_detail, attempt_succeeded, verified_address
      ) %>%
      group_by(across(-archive_version_year)) %>%
      summarise(
        archive_versions_present = format_year_ranges(archive_version_year),
        .groups = "drop"
      ) %>%
      relocate(archive_versions_present, .after = address) %>%
      relocate(reported_address, .after = archive_versions_present) %>%
      (\(x) {setDT(x)})()
    
  } 
  
  if ( !is.null(combined$matched_address) ) {
    # Address QC: summarize agnostic matching results
    qc_address$qc3[[i]] <- combined %>%
      mutate(
        # Prefer verified/standardized address; fall back to combined_address
        address = str_remove(coalesce(matched_address, combined_address), ",(?=\\s*\\d{5})"),
        reported_address = str_remove(combined_address, ",(?=\\s*\\d{5})"),
      ) %>%
      select(
        abi, archive_version_year, address,
        
        # ---- address ----
        reported_address,
        
        # ---- verification ----
        address_matched, match_geolocation_test, matched_address
      ) %>%
      group_by(across(-archive_version_year)) %>%
      summarise(
        archive_versions_present = format_year_ranges(archive_version_year),
        .groups = "drop"
      ) %>%
      relocate(archive_versions_present, .after = address) %>%
      relocate(reported_address, .after = archive_versions_present) %>%
      (\(x) {setDT(x)} )()
  }
  
  
  # --------------------
  # LOOP PART G: Quality Checks — Variation with Geolocation
  
  # Assesses the reliability of reported geolocation values for each address
  # using two approaches:
  #
  #   1) Deviation test: compare validated coordinates to reported/averaged coords.
  #   2) Dispersion test: measure spread of reported coords for the same address
  #      when the address appears multiple times.
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_geo_df <- combined %>%
    rename(address = best_address) %>%
    select(any_of(c(
        "abi", "archive_version_year", "address",
        
        # ---- geocoding ----
        "latitude", "longitude",
        "n_address", "enough_geo",
        "latitude_avg", "longitude_avg",
        "latitude_ver", "longitude_ver",
        
        # ---- verification ----
        "geolocation_verified", "n_attempts", "query_statuses",
        "geo_matched_address", "benchmark", "vintage_input",
        
        # ---- confounders ----
        "address_verified", "ver_geolocation_test",
        "address_matched", "match_geolocation_test"
      )))
  
  
  if ( !all(qc_geo_df$geolocation_verified %in% "No address_line_1") ) {
    # (a) Query QC: number of attempts, API status code, vintage used
    qc_geo$qc1[[i]] <- qc_geo_df %>%
      # Drop lat/lon fields here so QC focuses on request/response + matching metadata.
      # (Those coordinates may be analyzed in a separate QC stage.)
      select(-any_of(c(
        "latitude", "longitude", "latitude_avg", "longitude_avg",
        "latitude_ver", "longitude_ver"
      ))) %>%
      mutate(
        # all_200:
        #   TRUE  = every recorded attempt returned HTTP 200
        #   FALSE = at least one attempt was non-200
        #   NA    = insufficient information to evaluate (missing statuses, or 
        #           fewer parsed statuses than the number of configured tries)
        #
        # query_statuses is assumed to be a single string like "200 | 200 | unverified".
        # geocoder_census_tries is assumed to represent how many attempts were made/allowed.
        qs = as.character(query_statuses),
        all_200 = case_when(
          is.na(qs) ~ NA,
          lengths(strsplit(qs, " \\| ")) < length(geocoder_census_tries) ~ NA,
          TRUE ~ vapply(
            strsplit(qs, " \\| "),
            \(x) all(trimws(x) == "200"),
            logical(1)
          )
        ),
        
        # ---- normalization helpers (computed once) ----
        address_norm = case_when(
          is.na(address) ~ NA_character_,
          TRUE ~ address |>
            stringr::str_squish() |>
            stringr::str_remove("-\\d{4}$") |>
            stringr::str_remove_all(",(?=\\s*\\d{5})") |>
            stringr::str_to_upper()
        ),
        geo_matched_address_norm = case_when(
          is.na(geo_matched_address) ~ NA_character_,
          TRUE ~ geo_matched_address |>
            stringr::str_squish() |>
            stringr::str_remove("-\\d{4}$") |>
            stringr::str_remove_all(",(?=\\s*\\d{5})") |>
            stringr::str_to_upper()
        ),
        
        # ---- exact same (keep your current logic, now using normalized fields) ----
        matched_address_same = case_when(
          is.na(address_norm) | is.na(geo_matched_address_norm) ~ NA,
          TRUE ~ address_norm == geo_matched_address_norm
        ),
        
        # ---- similar (new) ----
        matched_address_similar = dplyr::case_when(
          is.na(address_norm) | is.na(geo_matched_address_norm) ~ NA,
          
          # short-circuit exact equality to avoid fragile similarity internals
          address_norm == geo_matched_address_norm ~ TRUE,
          
          TRUE ~ vapply(seq_len(dplyr::n()), \(i) {
            a <- address_norm[[i]]
            b <- geo_matched_address_norm[[i]]
            
            # extra safety: treat blanks as NA
            if (is.na(a) || is.na(b) || !nzchar(a) || !nzchar(b)) return(NA)
            
            # protect against NA-sensitive `if()` inside your function
            out <- tryCatch(
              find_similar_addresses(c(a, b), threshold = 0.15),
              error = function(e) NULL
            )
            if (is.null(out) || length(out) == 0) return(FALSE)
            
            any(vapply(out, \(grp) all(c(a, b) %in% grp), logical(1)))
          }, logical(1))
        )
      ) %>%
      # Collapse across archive vintages:
      # For each unique record (everything except archive_version_year), collect 
      # the set of archive years observed and format them into compact ranges 
      # (e.g., "2000:2002, 2005").
      select(-qs, -address_norm, -geo_matched_address_norm) %>%
      group_by(across(-archive_version_year)) %>%
      summarise(
        archive_versions_present = format_year_ranges(archive_version_year),
        .groups = "drop"
      ) %>%
      # Reorder columns to make QC outputs easier to scan
      relocate(archive_versions_present, .after = address) %>%
      relocate(all_200, .after = query_statuses) %>%
      relocate(matched_address_same, .after = geo_matched_address) %>%
      relocate(matched_address_similar, .after = matched_address_same) %>%
      # Convert tibble -> data.table (without breaking the pipe)
      (\(x) { setDT(x) })()
    
    
    # (b) Deviation QC: how far validated coords are from averaged coords (rounded)
    qc_geo$qc2[[i]] <- qc_geo_df %>%
      # Only evaluate deviation when validated coordinates exist
      filter(!is.na(latitude_ver), !is.na(longitude_ver)) %>%
      mutate(
        # Absolute differences (rounded to 4 decimals)
        lat_abs_diff = if_else(!is.na(latitude_ver),  round(abs(latitude_avg  - latitude_ver),  4), NA_real_),
        lon_abs_diff = if_else(!is.na(longitude_ver), round(abs(longitude_avg - longitude_ver), 4), NA_real_),
        
        # Flag whether the deviation exceeds the threshold
        lat_gt_002 = if_else(!is.na(lat_abs_diff), lat_abs_diff > 0.002, NA),
        lon_gt_002 = if_else(!is.na(lon_abs_diff), lon_abs_diff > 0.002, NA)
      ) %>%
      # Keep only QC outputs + identifiers
      select(abi, archive_version_year, address,
             lat_abs_diff, lat_gt_002, lon_abs_diff, lon_gt_002) %>%
      group_by(across(-archive_version_year)) %>%
      summarise(
        archive_versions_present = format_year_ranges(archive_version_year),
        
        .groups = "drop"
      ) %>%
      relocate(lat_gt_002, .after = lat_abs_diff) %>%
      relocate(archive_versions_present, .after = address) %>%
      (\(x) {setDT(x)} )()
  }
  
  # (c) Dispersion QC: summarize the spread of reported coords within (abi, address)
  qc_geo$qc3[[i]] <- qc_geo_df %>%
    {
      # ------------------------------------------------------------------
      # Defensive pre-processing to prevent errors when optional QC columns
      # are absent in some runs / sources.
      #
      # Strategy:
      #  1) REQUIRE the grouping/join keys (abi, address). If missing, stop
      #     early with a clear message (better than a cryptic data.table error).
      #  2) SELECT optional columns only if they exist (any_of()).
      #  3) CREATE any missing optional columns and fill with NA so that
      #     downstream mutate()/case_when() can safely reference them.
      # ------------------------------------------------------------------
      
      # Columns we must have (used later in group_by() and commonly in joins)
      req <- c("abi", "address")
      miss_req <- setdiff(req, names(.))
      if (length(miss_req) > 0) {
        stop("qc_geo_df is missing required columns: ", paste(miss_req, collapse = ", "))
      }
      
      # Columns that may or may not exist depending on upstream steps
      opt <- c(
        "latitude", "longitude",
        "address_verified", "ver_geolocation_test",
        "address_matched", "match_geolocation_test"
      )
      
      # Keep required columns + any optional columns that are present
      out <- dplyr::select(., dplyr::all_of(req), dplyr::any_of(opt))
      
      # Add back any missing optional columns as NA so later code never errors
      for (nm in setdiff(opt, names(out))) out[[nm]] <- NA
      
      # Standardize the shape/names we want for the QC pipeline
      dplyr::transmute(
        out,
        abi, address,
        latitude  = latitude,
        longitude = longitude,
        address_verified,
        ver_geolocation_test,
        address_matched,
        match_geolocation_test
      )
    } %>%
    mutate(
      # ------------------------------------------------------------------
      # Convert messy/heterogeneous verification fields into boolean-ish
      # pass/fail flags (TRUE/FALSE/NA).
      # ------------------------------------------------------------------
      
      # Address verified: interpret multiple possible encodings
      address_verified_pass = dplyr::case_when(
        is.na(address_verified) ~ NA,
        as.character(address_verified) %in% c("TRUE", "Exact match", "Fuzzy match") ~ TRUE,
        as.character(address_verified) %in% c("FALSE") ~ FALSE,
        isFALSE(address_verified) ~ FALSE,
        TRUE ~ FALSE
      ),
      
      # Address matched: incorporate match_geolocation_test when match is "Fuzzy"
      address_matched_pass = dplyr::case_when(
        is.na(address_matched) ~ NA,
        
        # "Only one address" means the comparison isn't meaningful
        trimws(as.character(address_matched)) == "Only one address" ~ NA,
        
        # Exact match passes
        trimws(as.character(address_matched)) == "Exact match" ~ TRUE,
        
        # Fuzzy match passes only if the geolocation test also passes
        trimws(as.character(address_matched)) == "Fuzzy match" &
          !is.na(match_geolocation_test) &
          (
            stringr::str_detect(stringr::str_to_upper(as.character(match_geolocation_test)), "PASS") |
              isTRUE(match_geolocation_test) |
              stringr::str_to_upper(trimws(as.character(match_geolocation_test))) == "TRUE"
          ) ~ TRUE,
        
        # Fuzzy match fails if the geolocation test explicitly fails
        trimws(as.character(address_matched)) == "Fuzzy match" &
          !is.na(match_geolocation_test) &
          stringr::str_to_upper(trimws(as.character(match_geolocation_test))) == "FAIL" ~ FALSE,
        
        # Otherwise leave as NA (unknown / not evaluated)
        TRUE ~ NA
      )
    ) %>%
    group_by(abi, address) %>%
    summarise(
      # ------------------------------------------------------------------
      # Summarise geocoding results by (abi, address).
      # ------------------------------------------------------------------
      
      # Count how many complete coordinate pairs exist
      n_geo = sum(!is.na(latitude) & !is.na(longitude)),
      
      # Latitude summaries (guarded so we don't compute min/max on all-NA)
      lat_min    = ifelse(any(!is.na(latitude)), round(min(latitude, na.rm = TRUE), 4), NA_real_),
      lat_q1     = ifelse(any(!is.na(latitude)), round(stats::quantile(latitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), 4), NA_real_),
      lat_median = ifelse(any(!is.na(latitude)), round(stats::median(latitude, na.rm = TRUE), 4), NA_real_),
      lat_mean   = ifelse(any(!is.na(latitude)), round(mean(latitude, na.rm = TRUE), 4), NA_real_),
      lat_q3     = ifelse(any(!is.na(latitude)), round(stats::quantile(latitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), 4), NA_real_),
      lat_max    = ifelse(any(!is.na(latitude)), round(max(latitude, na.rm = TRUE), 4), NA_real_),
      
      # Longitude summaries (same NA-guard pattern)
      lon_min    = ifelse(any(!is.na(longitude)), round(min(longitude, na.rm = TRUE), 4), NA_real_),
      lon_q1     = ifelse(any(!is.na(longitude)), round(stats::quantile(longitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), 4), NA_real_),
      lon_median = ifelse(any(!is.na(longitude)), round(stats::median(longitude, na.rm = TRUE), 4), NA_real_),
      lon_mean   = ifelse(any(!is.na(longitude)), round(mean(longitude, na.rm = TRUE), 4), NA_real_),
      lon_q3     = ifelse(any(!is.na(longitude)), round(stats::quantile(longitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), 4), NA_real_),
      lon_max    = ifelse(any(!is.na(longitude)), round(max(longitude, na.rm = TRUE), 4), NA_real_),
      
      # Spread flags: only meaningful if there are at least 2 non-NA values
      lat_spread_gt_02 = ifelse(
        sum(!is.na(latitude)) >= 2,
        (max(latitude, na.rm = TRUE) - min(latitude, na.rm = TRUE)) > 0.02,
        NA
      ),
      lon_spread_gt_02 = ifelse(
        sum(!is.na(longitude)) >= 2,
        (max(longitude, na.rm = TRUE) - min(longitude, na.rm = TRUE)) > 0.02,
        NA
      ),
      
      # Whether ANY record in the group passed address verification
      any_address_verified = dplyr::case_when(
        any(address_verified_pass %in% TRUE, na.rm = TRUE) ~ TRUE,
        all(is.na(address_verified_pass))                  ~ NA,
        TRUE                                               ~ FALSE
      ),
      
      # Whether ANY record in the group passed address matching (+ geo test if fuzzy)
      any_address_matched = dplyr::case_when(
        any(address_matched_pass %in% TRUE, na.rm = TRUE) ~ TRUE,
        all(is.na(address_matched_pass))                  ~ NA,
        TRUE                                              ~ FALSE
      ),
      
      .groups = "drop"
    ) %>%
    # Keep only cases with >1 geocode point to evaluate “spread”
    filter(n_geo > 1) %>%
    # Convenience: keep the spread flag near the related summary columns
    relocate(lat_spread_gt_002, .after = lat_max) %>%
    # Convert to a data.table (by reference) for downstream data.table workflows
    (\(x) { data.table::setDT(x) })()
  
  
  # --------------------
  # LOOP PART H: Quality Checks — Variation with Census Information
  
  # Preliminary assessments of the raw data indicated that the reported census
  # boundary values do not follow the expected pattern of decennial-only changes.
  # This QC step evaluates the reliability/consistency of census boundary values
  # reported for each address across query attempts and archive vintages.
  
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_census_df <- combined %>%
    rename(address = best_address) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- census geography ----
      census_block, census_tract, county_code, fips_code,   # reported coords (if present)
      n_address, enough_info, geoid_match, geoid_2000, geoid_2010, geoid_2020,   # validated coords
      
      # ---- CBSA/CSA (current + vintages) ----
      cbsa_level, cbsa_code, csa_code,                   # reported coords (if present)
      cbsa_code_2007, cbsa_level_2007, csa_code_2007,    # validated coords
      cbsa_code_2010, cbsa_level_2010, csa_code_2010,    # validated coords
      cbsa_code_2020, cbsa_level_2020, csa_code_2020     # validated coords
    )
  
  # Determined once before the pipeline — drives substring width for all vintages.
  #   "Blocks"       -> substr(geoid, 12, 15)  matches 4-character block code
  #   "Block Groups" -> substr(geoid, 12, 12)  matches 1-character block group code
  block_substr_end <- if (block_geography == "blocks") 15L else 12L
  
  # Define decennial period boundaries
  decennial_map <- data.table(
    vintage    = c(2000L, 2010L, 2020L),
    year_start = c(2000L, 2010L, 2020L),
    year_end   = c(2009L, 2019L, 2029L)
  )
  
  # (a) Census Boundaries QC: compare given and verified census boundaries
  qc_census$qc1[[i]] <- qc_census_df %>%
    # Exclude entries where there was insufficient information
    group_by(abi) %>%
    filter(!any(enough_info %in% FALSE, na.rm = TRUE)) %>%
    ungroup() %>%
    # Focus on entries that matched with a geoid
    dplyr::filter(geoid_match %in% "Matched") %>%
    dplyr::mutate(
      # ---- coerce only (no standardization/padding) ----
      fips_code_chr    = as.character(fips_code),    # expect 5 (state+county)
      county_code_chr  = as.character(county_code),  # expect 3
      census_tract_chr = as.character(census_tract), # expect 6
      census_block_chr = as.character(census_block), # expect 4 (block) or 1 (block group)
      
      # ---- length checks (treat NA as FALSE) ----
      fips_ok   = !is.na(fips_code_chr)    & stringr::str_length(fips_code_chr)    == 5,
      county_ok = !is.na(county_code_chr)  & stringr::str_length(county_code_chr)  == 3,
      tract_ok  = !is.na(census_tract_chr) & stringr::str_length(census_tract_chr) == 6,
      
      # ================ fips_code (positions 1-5 ================
      fips_match_2000 = fips_ok & substr(geoid_2000, 1, 5) == fips_code_chr,
      fips_match_2010 = fips_ok & substr(geoid_2010, 1, 5) == fips_code_chr,
      fips_match_2020 = fips_ok & substr(geoid_2020, 1, 5) == fips_code_chr,
      
      fips_code_any_match = dplyr::case_when(
        is.na(fips_code_chr) ~ "Uncheckable",
        !fips_ok             ~ "Not the expected dimensions",
        (fips_match_2000 | fips_match_2010 | fips_match_2020) ~ "Matched",
        TRUE               ~ "None"
      ),
      
      # Use missing="" so NA logicals never propagate into "NANANA"-style strings
      fips_vintages_raw = paste0(
        dplyr::if_else(fips_match_2000, "2000, ", "", missing = ""),
        dplyr::if_else(fips_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(fips_match_2020, "2020, ", "", missing = "")
      ),
      fips_vintages = dplyr::case_when(
        is.na(fips_code_chr) ~ "Not reported",
        !fips_ok             ~ "Not the expected dimensions",
        fips_code_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(fips_vintages_raw, ", $")
      ),
      
      # ================== county_code (positions 3-5) ==================
      county_match_2000 = county_ok & substr(geoid_2000, 3, 5) == county_code_chr,
      county_match_2010 = county_ok & substr(geoid_2010, 3, 5) == county_code_chr,
      county_match_2020 = county_ok & substr(geoid_2020, 3, 5) == county_code_chr,
      
      county_code_any_match = dplyr::case_when(
        is.na(county_code_chr) ~ "Not reported",
        !county_ok             ~ "Not the expected dimensions",
        (county_match_2000 | county_match_2010 | county_match_2020) ~ "Matched",
        TRUE                 ~ "None"
      ),
      
      county_vintages_raw = paste0(
        dplyr::if_else(county_match_2000, "2000, ", "", missing = ""),
        dplyr::if_else(county_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(county_match_2020, "2020, ", "", missing = "")
      ),
      county_vintages = dplyr::case_when(
        is.na(county_code_chr) ~ "Not reported",
        !county_ok             ~ "Not the expected dimensions",
        county_code_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(county_vintages_raw, ", $")
      ),
      
      # ================= census_tract (positions 6-11) =================
      tract_match_2000 = tract_ok & substr(geoid_2000, 6, 11) == census_tract_chr,
      tract_match_2010 = tract_ok & substr(geoid_2010, 6, 11) == census_tract_chr,
      tract_match_2020 = tract_ok & substr(geoid_2020, 6, 11) == census_tract_chr,
      
      census_tract_any_match = dplyr::case_when(
        is.na(census_tract_chr) ~ "Uncheckable",
        !tract_ok               ~ "Not the expected dimensions",
        (tract_match_2000 | tract_match_2010 | tract_match_2020) ~ "Matched",
        TRUE                 ~ "None"
      ),
      
      tract_vintages_raw = paste0(
        dplyr::if_else(tract_match_2000, "2000, ", "", missing = ""),
        dplyr::if_else(tract_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(tract_match_2020, "2020, ", "", missing = "")
      ),
      tract_vintages = dplyr::case_when(
        is.na(census_tract_chr) ~ "Uncheckable",
        !tract_ok               ~ "Not the expected dimensions",
        census_tract_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(tract_vintages_raw, ", $")
      ),
      
      # ======== block resolution classification (explicit diagnostics) ========
      block_type = dplyr::case_when(
        is.na(census_block_chr) ~ "None reported",
        stringr::str_length(census_block_chr) == 4 ~ "Blocks",
        stringr::str_length(census_block_chr) == 1 ~ "Block groups",
        TRUE ~ "Not the expected dimensions"
      ),
      
      # === census_block (positions 12-15 for block, 12-12 for block group) ===
      block_substr_end = dplyr::case_when(
        block_type == "Blocks" ~ 15L,
        block_type == "Block groups" ~ 12L,
        TRUE ~ NA_integer_
      ),
      
      block_match_2000 = block_type %in% c("Blocks", "Block groups") &
        substr(geoid_2000, 12, block_substr_end) == census_block_chr,
      block_match_2010 = block_type %in% c("Blocks", "Block groups") &
        substr(geoid_2010, 12, block_substr_end) == census_block_chr,
      block_match_2020 = block_type %in% c("Blocks", "Block groups") &
        substr(geoid_2020, 12, block_substr_end) == census_block_chr,
      
      census_block_any_match = block_match_2000 | block_match_2010 | block_match_2020,
      
      block_vintages_raw = paste0(
        dplyr::if_else(block_match_2000, "2000, ", "", missing = ""),
        dplyr::if_else(block_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(block_match_2020, "2020, ", "", missing = "")
      ),
      block_vintages = dplyr::case_when(
        block_type %in% c("None reported", "Not the expected dimensions") ~ "Uncheckable",
        !census_block_any_match ~ "None",
        TRUE ~ stringr::str_remove(block_vintages_raw, ", $")
      )
    ) %>%
    dplyr::select(
      abi, archive_version_year, address, census_block, census_tract,
      county_code, fips_code, n_address, geoid_2000, geoid_2010, geoid_2020,
      fips_code_any_match, fips_vintages,
      county_code_any_match, county_vintages,
      census_tract_any_match, tract_vintages,
      block_type, census_block_any_match, block_vintages
    ) %>%
    dplyr::group_by(dplyr::across(-archive_version_year)) %>%
    dplyr::summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    dplyr::relocate(archive_versions_present, .after = address) %>%
    dplyr::mutate(
      fips_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable")) return(NA)
          out <- suppressWarnings(check_alignment(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, fips_vintages
      ),
      county_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable")) return(NA)
          out <- suppressWarnings(check_alignment(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, county_vintages
      ),
      tract_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable")) return(NA)
          out <- suppressWarnings(check_alignment(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, tract_vintages
      ),
      block_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable")) return(NA)
          out <- suppressWarnings(check_alignment(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, block_vintages
      )
    ) %>%
    dplyr::relocate(fips_vintages_aligned,   .after = fips_vintages) %>%
    dplyr::relocate(county_vintages_aligned, .after = county_vintages) %>%
    dplyr::relocate(tract_vintages_aligned,  .after = tract_vintages) %>%
    dplyr::relocate(block_vintages_aligned,  .after = block_vintages) %>%
    (\(x) { data.table::setDT(x); x })()
  
  
  # Maps archive years to the CBSA vintage label used in *_vintages strings
  cbsa_vintage_map <- data.table(
    vintage    = c(2007L, 2010L, 2020L),
    year_start = c(2000L, 2010L, 2020L),
    year_end   = c(2009L, 2019L, 2029L)
  )
  
  # (b) Metro/Micro QC: compare given and verified micro/metro census boundaries
  qc_census$qc2[[i]] <- qc_census_df %>%
    # Exclude entries where there was insufficient information
    group_by(abi) %>%
    filter(!any(enough_info %in% FALSE, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(
      # ---- coerce to character for safe comparisons ----
      cbsa_level_chr = as.character(cbsa_level),
      cbsa_code_chr  = as.character(cbsa_code),
      csa_code_chr   = as.character(csa_code),
      
      cbsa_level_2007_chr = as.character(cbsa_level_2007),
      cbsa_code_2007_chr  = as.character(cbsa_code_2007),
      csa_code_2007_chr   = as.character(csa_code_2007),
      
      cbsa_level_2010_chr = as.character(cbsa_level_2010),
      cbsa_code_2010_chr  = as.character(cbsa_code_2010),
      csa_code_2010_chr   = as.character(csa_code_2010),
      
      cbsa_level_2020_chr = as.character(cbsa_level_2020),
      cbsa_code_2020_chr  = as.character(cbsa_code_2020),
      csa_code_2020_chr   = as.character(csa_code_2020),
      
      # ---- input missing? ----
      cbsa_level_input_na = is.na(cbsa_level_chr),
      cbsa_code_input_na  = is.na(cbsa_code_chr),
      csa_code_input_na   = is.na(csa_code_chr),
      
      # ---- OPTIONAL validity checks (edit as needed) ----
      cbsa_code_ok  = !cbsa_code_input_na  & stringr::str_length(cbsa_code_chr) == 5,
      csa_code_ok   = !csa_code_input_na   & stringr::str_length(csa_code_chr)  == 3,
      cbsa_level_ok = !cbsa_level_input_na & stringr::str_length(stringr::str_trim(cbsa_level_chr)) > 0,
      
      # ===================== CBSA CODE =====================
      cbsa_code_match_2007 = cbsa_code_ok & cbsa_code_chr == cbsa_code_2007_chr,
      cbsa_code_match_2010 = cbsa_code_ok & cbsa_code_chr == cbsa_code_2010_chr,
      cbsa_code_match_2020 = cbsa_code_ok & cbsa_code_chr == cbsa_code_2020_chr,
      
      cbsa_code_any_match = dplyr::case_when(
        is.na(cbsa_code_chr) ~ "Uncheckable",
        !cbsa_code_ok        ~ "Not the expected dimensions",
        (cbsa_code_match_2007 | cbsa_code_match_2010 | cbsa_code_match_2020) ~ "Matched",
        TRUE ~ "None"
      ),
      
      cbsa_code_vintages_raw = paste0(
        dplyr::if_else(cbsa_code_match_2007, "2007, ", "", missing = ""),
        dplyr::if_else(cbsa_code_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(cbsa_code_match_2020, "2020, ", "", missing = "")
      ),
      cbsa_code_vintages = dplyr::case_when(
        is.na(cbsa_code_chr)           ~ "Uncheckable",
        !cbsa_code_ok                  ~ "Not the expected dimensions",
        cbsa_code_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(cbsa_code_vintages_raw, ", $")
      ),
      
      # ===================== CBSA LEVEL =====================
      cbsa_level_match_2007 = cbsa_level_ok & cbsa_level_chr == cbsa_level_2007_chr,
      cbsa_level_match_2010 = cbsa_level_ok & cbsa_level_chr == cbsa_level_2010_chr,
      cbsa_level_match_2020 = cbsa_level_ok & cbsa_level_chr == cbsa_level_2020_chr,
      
      cbsa_level_any_match = dplyr::case_when(
        is.na(cbsa_level_chr) ~ "Uncheckable",
        !cbsa_level_ok        ~ "Not the expected dimensions",
        (cbsa_level_match_2007 | cbsa_level_match_2010 | cbsa_level_match_2020) ~ "Matched",
        TRUE ~ "None"
      ),
      
      cbsa_level_vintages_raw = paste0(
        dplyr::if_else(cbsa_level_match_2007, "2007, ", "", missing = ""),
        dplyr::if_else(cbsa_level_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(cbsa_level_match_2020, "2020, ", "", missing = "")
      ),
      cbsa_level_vintages = dplyr::case_when(
        is.na(cbsa_level_chr)            ~ "Uncheckable",
        !cbsa_level_ok                   ~ "Not the expected dimensions",
        cbsa_level_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(cbsa_level_vintages_raw, ", $")
      ),
      
      # ===================== CSA CODE =====================
      csa_code_match_2007 = csa_code_ok & csa_code_chr == csa_code_2007_chr,
      csa_code_match_2010 = csa_code_ok & csa_code_chr == csa_code_2010_chr,
      csa_code_match_2020 = csa_code_ok & csa_code_chr == csa_code_2020_chr,
      
      csa_code_any_match = dplyr::case_when(
        is.na(csa_code_chr) ~ "Uncheckable",
        !csa_code_ok        ~ "Not the expected dimensions",
        (csa_code_match_2007 | csa_code_match_2010 | csa_code_match_2020) ~ "Matched",
        TRUE ~ "None"
      ),
      
      csa_code_vintages_raw = paste0(
        dplyr::if_else(csa_code_match_2007, "2007, ", "", missing = ""),
        dplyr::if_else(csa_code_match_2010, "2010, ", "", missing = ""),
        dplyr::if_else(csa_code_match_2020, "2020, ", "", missing = "")
      ),
      csa_code_vintages = dplyr::case_when(
        is.na(csa_code_chr)           ~ "Uncheckable",
        !csa_code_ok                  ~ "Not the expected dimensions",
        csa_code_any_match != "Matched" ~ "None",
        TRUE ~ stringr::str_remove(csa_code_vintages_raw, ", $")
      )
    ) %>%
    select(
      abi, archive_version_year, address, n_address,
      cbsa_level, cbsa_code, csa_code,
      cbsa_level_2007, cbsa_code_2007, csa_code_2007,
      cbsa_level_2010, cbsa_code_2010, csa_code_2010,
      cbsa_level_2020, cbsa_code_2020, csa_code_2020,
      cbsa_code_any_match, cbsa_code_vintages,
      cbsa_level_any_match, cbsa_level_vintages,
      csa_code_any_match, csa_code_vintages
    ) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    mutate(
      cbsa_code_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable", "Not the expected dimensions")) return(NA)
          out <- suppressWarnings(check_alignment_cbsa(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, cbsa_code_vintages
      ),
      cbsa_level_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable", "Not the expected dimensions")) return(NA)
          out <- suppressWarnings(check_alignment_cbsa(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, cbsa_level_vintages
      ),
      csa_code_vintages_aligned = mapply(
        function(present, vint) {
          if (is.na(vint) || vint %in% c("None", "Uncheckable", "Not the expected dimensions")) return(NA)
          out <- suppressWarnings(check_alignment_cbsa(present, vint))
          if (length(out) == 0L || is.na(out)) return(NA)
          out
        },
        archive_versions_present, csa_code_vintages
      )
    ) %>%
    relocate(cbsa_code_vintages_aligned,  .after = cbsa_code_vintages) %>%
    relocate(cbsa_level_vintages_aligned, .after = cbsa_level_vintages) %>%
    relocate(csa_code_vintages_aligned,   .after = csa_code_vintages) %>%
    (\(x) { data.table::setDT(x) })()
  
  
  # --------------------
  # LOOP PART I: Commit Results
  
  # The final table retains only verified outcomes plus the original address, as
  # the latter may be used differentially in the subsequent reformatting step.
  # 
  # All intermediate columns — including population patterns, API success/failures,
  # deviations from originally reported values, and all variable versions
  # (original, averaged, verified, and matched) — are preserved in the QC tables.
  
  
  finish_build[[i]] <- combined %>%
    # Remove originally reported longitude/latitude in favor of verified/averaged.
    select(-longitude, -latitude) %>%
    rename(address = best_address) %>%
    mutate(
      reported_address = str_remove(combined_address, ",(?=\\s*\\d{5})"),
      
      # Prefer verified geolocation; fall back to averaged coordinates.
      # Guard for missing *_ver columns too.
      longitude_ver = if ("longitude_ver" %in% names(.)) longitude_ver else NA_real_,
      latitude_ver  = if ("latitude_ver"  %in% names(.)) latitude_ver  else NA_real_,
      
      longitude = coalesce(longitude_ver, longitude_avg),
      latitude  = coalesce(latitude_ver,  latitude_avg)
    ) %>%
    select(
      -any_of(c(
        # ---- Address: drop raw inputs and verification columns ----
        "address_line_1","city","state","zipcode","zip4","combined_address", "do_api",
        "attempt_succeeded","usps_status","usps_status_detail","verified_address", 
        "ver_geolocation_test","matched_address","match_geolocation_test",
        
        # ---- Geolocation: drop raw geocoding and verification columns ----
        "latitude_avg","longitude_avg","latitude_ver","longitude_ver",
        "n_address","enough_geo","n_geo","n_attempts","query_statuses",
        "geo_matched_address","benchmark","vintage_input",
        
        # ---- Census Boundaries: drop raw boundary codes ----
        "census_block","census_tract","county_code","fips_code",
        "cbsa_level","cbsa_code","csa_code","enough_info"
      ))
    ) %>%
    # Organize columns
    relocate(address, .after = company_holding_status) %>%
    
    # Ensure order: address, address_verified, address_matched (when present)
    relocate(any_of(c("address_verified", "address_matched")), .after = address) %>%
    # Put reported_address after the last flag that exists; else after address
    relocate(reported_address, .after = any_of(c("address", "address_verified", "address_matched"))) %>%
    
    relocate(longitude, .after = sic6_descriptions_sic4) %>%
    relocate(latitude, .after = longitude) %>%
    relocate(geoid_match, .after = geoid_2020)
  
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}


## --------------------
## SUBSECTION C2: Save Result

# Combine all data tables in the list into one data table
finish_build <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = finish_build) %>%
  (\(x) {rbindlist(x, use.names = TRUE, fill = TRUE) })() %>%
  # Ensure order: address, address_verified, address_matched (when present)
  relocate(any_of(c("address_verified", "address_matched")), .after = address) %>%
  # Put reported_address after the last flag that exists; else after address
  relocate(reported_address, .after = any_of(c("address", "address_verified", "address_matched")))

qc_address$qc1 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_address$qc1) %>% (\(x) {bind_rows(x, .id = "i")} )()
qc_address$qc2 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_address$qc2) %>% (\(x) {bind_rows(x, .id = "i")} )()
qc_address$qc3 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_address$qc3) %>% (\(x) {bind_rows(x, .id = "i")} )()

# qc_address may or may not include specific output, depending on whether the
# USPS API was used to verify or not. Keep only elements of qc_address that are
# NOT missing/empty
qc_address <- qc_address[!vapply(qc_address, function(x) {
  is.null(x) || length(x) == 0L || all(is.na(x)) || all(trimws(as.character(x)) == "")
}, logical(1))]

qc_geo$qc1 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_geo$qc1) %>% (\(x) {bind_rows(x, .id = "i")} )()
qc_geo$qc2 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_geo$qc2) %>% (\(x) {bind_rows(x, .id = "i")} )()
qc_geo$qc3 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_geo$qc3) %>% (\(x) {bind_rows(x, .id = "i")} )()

qc_census$qc1 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_census$qc1) %>% (\(x) {bind_rows(x, .id = "i")} )()
qc_census$qc2 <- Filter(f = function(x) !is.null(x) && NROW(x) > 0, x = qc_census$qc2) %>% (\(x) {bind_rows(x, .id = "i")} )()


# Commit results for large output (HPC and local compatible)
outfile_df <- file.path(outdir, "Verified Result", sprintf(str_c("Step 2 HPC_2026 Format_Verified Result_", nums[1], " to ", nums[2], "_slurmArray_%03d.parquet"), idx))
write_parquet(finish_build, outfile_df)

outfile_address <- file.path(outdir, "Address QC", sprintf(str_c("Step 2 HPC_2026 Format_Address QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.db"), idx))
write_list_to_duckdb(qc_address, outfile_address)

outfile_geo <- file.path(outdir, "Geo QC", sprintf(str_c("Step 2 HPC_2026 Format_Geo QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.db"), idx))
write_list_to_duckdb(qc_geo, outfile_geo)

outfile_census <- file.path(outdir, "Census QC", sprintf(str_c("Step 2 HPC_2026 Format_Census QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.db"), idx))
write_list_to_duckdb(qc_census, outfile_census)


# # Commit results for smaller output (HPC and local compatible)
# outfile_df <- file.path(outdir, sprintf(str_c("Step 2 HPC_2026 Format_Verified Result_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
# write.csv(finish_build, outfile_df)
# 
# outfile_address <- file.path(outdir, sprintf(str_c("Step 2 HPC_2026 Format_Address QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.xlsx"), idx))
# write_list_to_xlsx(qc_address, outfile_address)
# 
# outfile_geo <- file.path(outdir, sprintf(str_c("Step 2 HPC_2026 Format_Geo QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.xlsx"), idx))
# write_list_to_xlsx(qc_geo, outfile_geo)
# 
# outfile_census <- file.path(outdir, sprintf(str_c("Step 2 HPC_2026 Format_Census QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.xlsx"), idx))
# write_list_to_xlsx(qc_census, outfile_census)


# HPC array result confirmation; not really necessary in a live session or local
if (is.na(current_array_index)) {
  cat("Indices out of range\n")
  
} else {
  
  expected_cols <- c(core_fields$Variable, "archive_version_year")
  
  # Compute actual diffs
  extra_cols   <- colnames(finish_build)[colnames(finish_build) %!in% expected_cols]
  missing_cols <- expected_cols[expected_cols %!in% colnames(finish_build)]
  
  # Baselines you said are the correct/expected results
  expected_extra <- c(
    "address","address_matched","address_verified","reported_address",
    "geolocation_verified", "geoid_2000","geoid_2010","geoid_2020","geoid_match",
    "cbsa_code_2007","cbsa_level_2007","csa_code_2007","zcta_2000",
    "cbsa_code_2010","cbsa_level_2010","csa_code_2010","zcta_2010",
    "cbsa_code_2020","cbsa_level_2020","csa_code_2020","zcta_2020"
  )
  
  expected_missing <- c(
    "address_line_1","city","state","zipcode","zip4","census_block","census_tract",
    "county_code","fips_code","cbsa_level","cbsa_code","csa_code"
  )
  
  # Compare computed diffs to the baselines
  unexpected_extra <- setdiff(extra_cols, expected_extra)
  unexpected_missing <- setdiff(missing_cols, expected_missing)
  
  # Also catch if the baseline items themselves are not present in the computed sets
  baseline_extra_not_found   <- setdiff(expected_extra, extra_cols)
  baseline_missing_not_found <- setdiff(expected_missing, missing_cols)
  
  # Report
  if (length(unexpected_extra) == 0 &&
      length(unexpected_missing) == 0 &&
      length(baseline_extra_not_found) == 0 &&
      length(baseline_missing_not_found) == 0) {
    
    cat("All columns in the verified results are as expected\n")
    
  } else {
    
    # Only report what is "extra beyond the expected extra" or "missing beyond the expected missing"
    if (length(unexpected_extra) > 0) {
      cat("Additional UNEXPECTED columns in the verified results (beyond expected extras):\n")
      cat(paste0(" - ", unexpected_extra), sep = "\n")
      cat("\n")
    }
    
    if (length(unexpected_missing) > 0) {
      cat("Additional MISSING expected columns in the verified results (beyond expected missing):\n")
      cat(paste0(" - ", unexpected_missing), sep = "\n")
      cat("\n")
    }
    
    # If you want, also flag when the situation differs from the expected baseline
    # (i.e., some baseline extras are no longer extra, or some baseline missings are no longer missing)
    if (length(baseline_extra_not_found) > 0) {
      cat("NOTE: Some previously-expected extras are NOT extra anymore:\n")
      cat(paste0(" - ", baseline_extra_not_found), sep = "\n")
      cat("\n")
    }
    
    if (length(baseline_missing_not_found) > 0) {
      cat("NOTE: Some previously-expected missing columns are NOT missing anymore:\n")
      cat(paste0(" - ", baseline_missing_not_found), sep = "\n")
      cat("\n")
    }
  }
  
  cat("Wrote results for: Index", sprintf(str_c(nums[1], " to ", nums[2], "; Slurm Array %03d"), idx), "\n")
}


