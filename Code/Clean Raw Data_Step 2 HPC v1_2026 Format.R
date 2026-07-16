## ----------------------------------------------------------------
## Run Address Validation and Annotate with Census Boundaries (Version 1)
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: July 3rd, 2026
## Date Modified: July 14th, 2026
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
##              the API, an agnostic string-matching step is used to help
##              mitigate redundancy caused by typographical errors.
##
##           2. Users who choose to use the API can set limits on how many
##              addresses are verified, allowing finer control over the costs
##              of running the algorithm.
##
##         More details can be found in "SUBSECTION B3: Optional USPS Address 
##         Verification (API Setup and Quota Controls)" of 
##         "Clean Raw Data_Step 2 HPC v2_2026 Format.R".
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
##          rprojroot::find_rstudio_root_file()
##          readRenviron(rprojroot::find_rstudio_root_file(".Renviron"))
## 
##       The HPC batch script includes a command that points to the ".Renviron"
##       file. However, you may still encounter issues setting this location. 
##       To resolve this, open the "Shell Access" application and run the 
##       following code, updating the R module version as needed.
## 
##          module avail R/
##          module reset
##          module load R/4.4.2-gfbf-2024a
##          normalizePath("~/FILE-PATH/.Renviron", mustWork = FALSE)
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
##        * SUBSECTION B3: Load Relevant Block-Level GeoPackages by State
## 
##    - PART C: CLEAN, VALIDATE, AND ANNOTATE ADDRESS DATA
##        * SUBSECTION C1: Algorithm
##        * SUBSECTION C2: Save Result

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Either one, the second 
source("renv/activate.R")
renv::activate()

# Check the environment is activated correctly by comparing the shell with OnDemand
.libPaths()

# # Initiate the package environment using an HPC array job
renv::activate() 


# Disable global cache sharing (project-scoped only). Keep this line AFTER
# activate and BEFORE restore.
# renv::settings$use.cache(FALSE)

# Initiate the package environment locally or in an HPC live session
# renv::restore()


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

# Load in the functions in the HPC
source("./Support Functions/General.R")
source("./Support Functions/For Step 2_2026 Format.R")
source("./Support Functions/For Step 2_Compile Decennial Data.R")

# # Load in the functions locally
# source("./Code/Support Functions/General.R")
# source("./Code/Support Functions/For Step 2_2026 Format.R")
# source("./Code/Support Functions/For Step 2_Compile Decennial Data.R")

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

# Process the defined output directory and current SLURM array index for HPC 
# array job
args   <- commandArgs(trailingOnly = TRUE)
outdir <- args[1]
idx    <- as.integer(args[2])

# Set output directory HPC live session
outdir <- "Results"
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

# # Set output directory locally
# outdir <- "Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2023 Format"




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Load in the previous step and other datasets in the HPC
# Load standardized and converted data
church_2026_form <- read_parquet("./church_2026_form_standardized_06.10.2026.parquet")
church_2026_form_dt <- as.data.table(church_2026_form)  # Convert for efficient data manipulation

# Toggle based on whether the zip code field was originally imported as a
# character type; if not, leading and trailing zeros may have been lost.
zip_codes_character <- TRUE

# Load core field handling/rename spec.
core_fields <- read_csv("./Handling Raw Variables_05.12.2026.csv")

# Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
uscities_df <- read_csv("./simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
zip_city_lookup <- build_zip_city_lookup(uscities_df)

# Load the combined national-level Metropolitan/Micropolitan Statistical Area 
# (CBSA), Combined Statistical Area (CSA), and ZIP Code Tabulation Areas (ZCTA)
# GeoPackage file. If nothing loads, skip to "SUBSECTION A3: Build Precompiled 
# TIGER/Line GeoPackages" first before coming back to this line.
# 
# State block or block group, tract, county, and state geometries are loaded 
# later in the script, as they depend on which states are present in the 
# current subset.
core_areas_layers <- sf::st_layers("./Census Bureau TIGER Line Shapefiles/core_areas.gpkg")$name

core_areas <- setNames(
  lapply(core_areas_layers, function(lyr) st_read("./Census Bureau TIGER Line Shapefiles/core_areas.gpkg", layer = lyr, quiet = TRUE)),
  core_areas_layers
)


# # Load in the previous step and other datasets locally
# # Load standardized and converted data
# church_2026_form <- read_parquet("Data/Results/KEEP LOCAL/From Clean Raw Data/Step 1_2026 Format/church_2026_form_standardized_06.10.2026.parquet")
# 
# # Toggle based on whether the zip code field was originally imported as a
# # character type; if not, leading and trailing zeros may have been lost.
# zip_codes_character <- TRUE
# 
# # Load core field handling/rename spec.
# core_fields <- read_csv("Data/Results/From Process Data Update/Handling Raw Variables_05.12.2026.csv")
# 
# # Load SimpleMaps US cities reference and build ZIP -> city/state lookup.
# uscities_df <- read_csv("Data/Raw/simplemaps_uscities_basicv1.93/uscities.csv") %>% as.data.frame()
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
# core_areas_layers <- sf::st_layers("./Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg")$name
# 
# core_areas <- setNames(
#   lapply(core_areas_layers, function(lyr) st_read("./Data/Results/Census Bureau TIGER Line Shapefiles/core_areas.gpkg", layer = lyr, quiet = TRUE)),
#   core_areas_layers
# )




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
#       - ".Renviron"
#       - renv/
#       - "renv.lock"
#       - Two scripts: 
#             1. "Clean Raw Data_Step 2 HPC_2026 Format.R"
#             2. "Validation SLURM_2026 Format.sh"
#       - Three associated function scripts. Place these in 
#         "~/church-closures/Support Functions/": 
#             1. "General.R"
#             2. "For Step 2_2026 Format.R"
#             3. "For Step 2_Compile Decennial Data.R".
#       - Three datasets or directories containing data: 
#             1. "church_2026_form_standardized_06.10.2026.parquet"
#             2. simplemaps_uscities_basicv1.93/ (from ./Data/Raw/)
#             3. Census Bureau TIGER Line Shapefiles/ (from ./Data/Results/)
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
#         normalizePath(".Renviron", mustWork = FALSE)
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
#  15. Exit R to refresh the session. Now you are ready to proceed with either
#      the live session of job array method.
# 
#         quit()
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
#       - 6 hours, 4 CPU, 10 GiB per CPU
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
#      Search Priorities" in "Clean Raw Data_Step 2 v2_2026 Format.R".
# 
#   8. In "SUBSECTION B3: Load Relevant Block-Level GeoPackages by State",
#      select the smallest geographic unit to associate with each record: block
#      or block group. The block includes block group data, but is
#      computationally intensive. The algorithm is designed to generate results
#      for both options; toggle which one you wish to use.
# 
#      NOTE: This section assumes the required GeoPackage (*.gpkg) files have
#      already been created. If these have not been generated, refer to 
#      "SUBSECTION A3: Build Precompiled TIGER/Line GeoPackages Search Priorities" 
#      in "Clean Raw Data_Step 2 v2_2026 Format.R" for further directions.
# 
#   9. Run the code under "SUBSECTION C1: Algorithm" of "PART C: CLEAN,
#      VALIDATE, AND ANNOTATE ADDRESS DATA" to start the processing algorithm.
#      A progress bar will appear indicating the current progress of the
#      function.
# 
#  10. After the function completes, save the results in
#      "SUBSECTION C2: Save Result".
# 
#  11. Once all index ranges have been processed, save results locally to their
#      respective directories specified in "PROCESS PARAMETERS FROM BATCH SCRIPT":
#
#      "~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format"
#
#  12. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2026 Format.R" to compile all results together.
# 
# 
# The array job is run entirely through the command-line interface. After 
# completing the steps outlined above, simply:
#
#   1. Click "<HPC name> Shell Access" to open the command-line interface.
# 
#   2. Request time on a compute node.
# 
#         salloc -p day -t 8:00:00 --mem=8G
#
#   3. Navigate to the project directory.
# 
#         cd "project_pi_bm895/sg2736/church_closures"
# 
#   4. After the job allocation has been approved and is ready for use, execute
#      the SLURM batch script:
#
#         chmod +x "Validation SLURM_2026 Format.sh"
#         sbatch "Validation SLURM_2026 Format.sh"
# 
#   5. OPTIONAL: Check the status of the script by running the following command.
#      Replace the batch job number with the one returned by the previous step.
# 
#         squeue -j 12924166
#
#   6. OPTIONAL: Inspect any errors that arise from running the script:
#
#         tail -n 50 Logs/<RUN NAME>.err
# 
#   7. Once all index ranges have been processed, save the results locally to
#      "~/Church-Closures-Dashboard/Data/Results/KEEP LOCAL/From Clean Raw Data/Step 2_2026 Format".
#
#   8. Return to "SUBSECTION A2: Compile the Results" in
#      "Clean Raw Data_Step 2_2026 Format.R" to compile all results together.




## ----------------------------------------------------------------
## PART B: ENVIRONMENT SETUP (GENERAL + ARRAY-SPECIFIC)

## --------------------
## SUBSECTION B1: Index Queue

# The algorithm was timed locally, where approximately 14 entries were
# processed per 5 minutes (~1,000 in six hours). Based on this, the data
# was partitioned into 1,000-entry indices (listed below) to fit within
# the HPC's 8-hour session limit.
# 
# Each index was processed in a separate session and compiled in
# "Clean Raw Data_Step 2_2023 Format.R".

# Prepare the dataframes used in the script
church_2026_form_dt <- as.data.table(church_2026_form)  # Convert for efficient data manipulation
setorder(church_2026_form_dt, state)  # Organize the table by state to increase census boundary efficiency

# Generate the parsed indices based on the dataset loaded and desired chunk size
processed_indices <- make_ranges(church_2026_form_dt, chunk_size = 1000) %>% .$label

# Set index using an HPC array job
current_array_index <- processed_indices[idx]

# # Set index locally or in an HPC live session
# idx <- 1
# current_array_index <- processed_indices[idx]

nums <- as.integer(unlist(regmatches(current_array_index, gregexpr("\\d+", current_array_index))))

# Parse index
index = seq(nums[1], nums[2])

# Define the search space
search_space <- unique(church_2026_form_dt$abi)[index]


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
## SUBSECTION B3: Load Relevant Block-Level GeoPackages by State

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
qc_address   <- vector("list", length(search_space))
qc_geo       <- list(qc1 = list(), qc2 = list(), qc3 = list())
qc_census    <- list(qc1 = list(), qc2 = list())

# Initialize progress bar
pb = txtProgressBar(min = 0, max = length(search_space), style = 3)

for (i in 1:length(search_space)) {
  # Subset to only entries associated with one business ABI.
  subset <- church_2026_form_dt[abi %in% search_space[i]]
  
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
      latitude, longitude
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
      
      # Return an ungrouped data frame
      .groups = "drop"
    ) %>%
    mutate(address_verified = NA, verified_address = NA)
  
  # Ensure the rows are ordered by ascending year of observation
  setorder(candidate_addresses, anchor_year_min)
  
  # Prepare candidate_addresses for QC output:
  candidate_addresses <- candidate_addresses %>%
    #   - Initialize attempt_succeeded as NA to track which retry attempt
    #     validated each address and capture any non-hard-stop API errors
    #     (e.g. HTTP 400 "Address Not Found") for audit
    mutate(attempt_succeeded = NA_character_) %>%
    #   - Initialize geolocation_test as NA for downstream geocoding checks
    mutate(geolocation_test = NA_character_)
  
  
  # --------------------
  # LOOP PART B: Correct Addresses with USPS Database
  
  for (j in 1:nrow(candidate_addresses)) {
    
    # Pull the j-th row into variables used by validate_usps_address()
    address1 <- candidate_addresses$address_line_1[j]
    address2 <- ""
    city     <- candidate_addresses$city[j]
    state    <- candidate_addresses$state[j]
    zip5     <- candidate_addresses$zipcode[j]
    zip4     <- ifelse(is.na(candidate_addresses$zip4[j]), "", candidate_addresses$zip4[j])
    
    # Track which attempt succeeded and collect non-hard-stop failure reasons
    # across all attempts for audit/debugging purposes.
    attempt_succeeded <- NA_character_
    attempt_log       <- list()
    
    
    # -- Attempt 1: validate using the original inputs as-is -------------------
    
    usps_validated <- validate_usps_address(
      consumer_key, consumer_secret,
      address1, address2, city, state, zip5, zip4
    )
    
    if (usps_validated$ok) {
      attempt_succeeded <- "First try"
    } else {
      attempt_log[[1]] <- list(attempt = 1L, status = usps_validated$status,
                               detail  = usps_validated$status_detail)
    }
    
    
    # -- Attempt 2: if no match, assess/correct city via ZIP lookup, then retry
    
    # Handles the case where ZIP codes were imported as numeric, stripping leading
    # and trailing zeros.
    
    if (!usps_validated$ok) {
      
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
    
    
    # -- Save results back into candidate_addresses (single write block) -------
    
    if (!usps_validated$ok) {
      
      # Nothing matched after all attempts — record failure reason from the last
      # non-empty attempt log entry for the most actionable status message.
      last_log <- Filter(Negate(is.null), attempt_log)
      last_log <- if (length(last_log) > 0) last_log[[length(last_log)]] else NULL
      
      candidate_addresses$address_verified[j]  <- FALSE
      candidate_addresses$verified_address[j]  <- "No address match found"
      candidate_addresses$attempt_succeeded[j] <- "None"
      
    } else {
      
      # Build a formatted address string ("line1, line2, city, state ZIP-EXT"),
      # treating blank/whitespace-only fields as NA to omit them from the output.
      candidate_addresses$address_verified[j]  <- TRUE
      candidate_addresses$attempt_succeeded[j] <- attempt_succeeded
      
      candidate_addresses$verified_address[j] <-
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
  # LOOP PART C: Resolve Records with No Address Match Found
  
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
    
    for(j in 1:nrow(unmatched)) {
      
      # Isolate the addresses for exact test comparison
      target_line_1 <- unmatched[j, address_line_1]
      comparisons_line_1 <- c(match_to$address_line_1, target_line_1)
      
      # Exact match tests for the address_line_1 and whole address
      match_line_1 <- find_similar_addresses(comparisons_line_1, threshold = 0)
      
      
      # -- a. Exact Match on address_line_1 with Conflicting Metadata ----------
      
      if( any(match_to$address_line_1 == target_line_1) ) {
        
        # Join verified candidates to unmatched records on address_line_1.
        # `allow.cartesian = TRUE` handles the many-to-many relationship.
        cand <- merge(
          match_to,
          unmatched[j, .(u_id, address_line_1, u_min = anchor_year_min, u_max = anchor_year_max)],
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
        unmatched[j, "verified_address"] <- best[1, verified_address]
        unmatched[j, "address_verified"] <- "Exact match"
        unmatched[j, "geolocation_test"] <- "Override"
        unmatched[j, "latitude_avg"] <- best[1, latitude_avg]
        unmatched[j, "longitude_avg"] <- best[1, longitude_avg]
        
        
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
            match_to[match_to$address_line_1 %in% match_line_1[vapply(match_line_1, length, integer(1)) > 1][[1]], ],
            unmatched[j, .(u_id, address_line_1, u_min = anchor_year_min, u_max = anchor_year_max, u_lat = latitude_avg, u_lon = longitude_avg)]
          )
          
          setDT(cand)
          
          # Apply preference rules for match selection:
          #     - First prefer a verified match whose anchor-year interval
          #       overlaps (or touches) the unmatched anchor-year window.
          #     - If no overlap exists, select the verified match whose 
          #       anchor-year interval is closest to the unmatched window, 
          #       regardless of whether it falls before or after.
          #     - “Closest” is based on the smallest non-overlap gap between the 
          #       two year ranges (overlap/touch counts as zero); for 
          #       non-overlapping ranges, use the candidate’s end year if it is 
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
          
          cand[, geolocation_test := (lat_diff < 0.002) & (lon_diff < 0.002)]
          
          # Select the best verified match per unmatched record using the 
          # preference rules (ONLY among coordinate-close candidates).
          best <- cand[geolocation_test == TRUE][
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
          
          
          if(nrow(best) == 0L) {
            
            # No candidate survived the geolocation test — flag as failed
            unmatched[j, "geolocation_test"] <- "FALSE"
            
          } else {
            
            # Geolocation test passed — assign the top candidate as the verified address
            unmatched[j, "verified_address"] <- best[1, verified_address]
            unmatched[j, "address_verified"] <- "Fuzzy match"
            unmatched[j, "geolocation_test"] <- "TRUE"
            unmatched[j, "latitude_avg"] <- best[1, latitude_avg]
            unmatched[j, "longitude_avg"] <- best[1, longitude_avg]
            
          }
          
          
          # -- iii. No Fuzzy Match Found ---------------------------------------
          
        } else if ( threshold_line1 < 0.2 && match_check > 2 ) {
          
          # Threshold reached 0 before candidates could be separated, or the
          # step-wise threshold failed to resolve to a single candidate —
          # no single best match could be isolated.
          unmatched[j, "address_verified"] <- "No Separable Match"
          
        } else if ( match_check == 1 ) {
          
          # No verified match exists at any threshold
          unmatched[j, "address_verified"] <- "FALSE"
          
        } else {
          
          # Fallback: condition was unanticipated — flag for manual review
          unmatched[j, "address_verified"] <- "Unanticipated Error"
          
        }
      }
      
    }
    
    # Recombine all separated dataset partitions into a single unified dataset
    common <- Reduce(intersect, list(names(match_to), names(unmatched)))
    candidate_addresses <- rbindlist(list(match_to[, ..common], unmatched[, ..common]), use.names = TRUE)
  }
  
  # Organize columns
  candidate_addresses <- candidate_addresses %>%
    relocate(attempt_succeeded, .after = address_verified) %>%
    relocate(geolocation_test, .after = attempt_succeeded)
  setDT(candidate_addresses)
  
  # Ensure the rows are ordered by ascending year of observation.
  setorder(candidate_addresses, anchor_year_min)
  
  
  # --------------------
  # LOOP PART D: Verify Geolocation with the US Census Bureau’s Geocoder Database
  
  # Helper to safely pull a field from each attempt
  pluck_or_na <- function(x, name) if (!is.null(x[[name]])) as.character(x[[name]]) else NA_character_
  
  out_row <- list()
  for (j in 1:nrow(candidate_addresses)) {
    address_to_check <- candidate_addresses[j, ] %>% 
      (\(x) {ifelse(
        !is.na(x$verified_address) & x$verified_address != "No address match found",
        x$verified_address,
        x$combined_address
      )
      })()
    
    
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
        geolocation_verified = TRUE,
        n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
        query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                             FUN.VALUE = character(1))),
                                              collapse = " | "),
        matched_address      = res$best$matched_address,
        benchmark            = res$best$benchmark,
        vintage_input        = res$best$vintage_input,
        latitude_ver         = res$best$lat,
        longitude_ver        = res$best$lon
      )
      
      out_row[[j]] <- dplyr::bind_cols(candidate_addresses[j, , drop = FALSE], search_res)
      
    } else {
      search_res <- tibble::tibble(
        geolocation_verified = FALSE,
        n_attempts           = Filter(Negate(is.null), res$attempts) %>% length(),
        query_statuses       = stringr::str_c(na.omit(vapply(res$attempts, pluck_or_na, name = "status",
                                                             FUN.VALUE = character(1))),
                                              collapse = " | "),
        matched_address      = NA_character_,
        benchmark            = NA_real_,
        vintage_input        = NA_character_,
        latitude_ver         = NA_real_,
        longitude_ver        = NA_real_
      )
      
      out_row[[j]] <- dplyr::bind_cols(candidate_addresses[j, , drop = FALSE], search_res)
    }
  }
  
  # Save results
  candidate_addresses <- bind_rows(out_row)
  
  
  # --------------------
  # LOOP PART E: Add Census Information by GEO Coordinates
  
  candidate_addresses <- candidate_addresses %>%
    mutate(
      # Create a stable key for joining results back later
      row_id = row_number(),
      
      # Prefer verified coordinates; fall back to averaged coordinates
      lon = coalesce(longitude_ver, longitude_avg),
      lat = coalesce(latitude_ver,  latitude_avg),
      
      # Flag rows that have enough information to attempt spatial matching
      enough_info = !(is.na(lon) | is.na(lat) | is.na(state))
    )
  
  setDT(candidate_addresses)
  
  # Build an sf points table (only rows with usable lon/lat/state)
  cand_sf <- candidate_addresses %>%
    transmute(row_id, state, lon, lat) %>%
    filter(!is.na(lon), !is.na(lat), !is.na(state)) %>%
    mutate(state = toupper(trimws(state))) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  
  # --- Add DECENNIAL census geography (Census blocks) for each vintage ---
  # Adds: geoid_2000, geoid_2010, geoid_2020 (block GEOIDs)
  census_results <- add_decennial_geoid_block(
    cand_sf, 
    blocks_by_state, 
    geography = block_geography
  )
  
  setDT(census_results)
  
  # --- Add METRO/MICRO areas (CBSA) and CSA codes for each vintage ---
  # Adds: cbsa_2007/csa_2007, cbsa_2010/csa_2010, and cbsa_2020/csa_2020
  # Adds: zcta_2000, zcta_2010, and zcta_2020
  
  vintages <- list(
    list(cbsa_csa_year = 2007, cbsa_csa = core_areas$cbsa_csa_2007, zcta_year = "zcta_2000", zcta = core_areas$zcta_2000),
    list(cbsa_csa_year = 2010, cbsa_csa = core_areas$cbsa_csa_2010, zcta_year = "zcta_2010", zcta = core_areas$zcta_2010),
    list(cbsa_csa_year = 2020, cbsa_csa = core_areas$cbsa_csa_2020, zcta_year = "zcta_2020", zcta = core_areas$zcta_2020)
  )
  
  # Process each vintage and collect results
  core_areas_list <- lapply(vintages, function(v) {
    
    out <- list()
    
    # Decode CBSA/CSA for this vintage if a layer is provided
    if (!is.null(v$cbsa_csa)) {
      out$cbsa_csa <- decode_cbsa_csa(
        cand_sf     = cand_sf,
        cbsa_csa_sf = v$cbsa_csa,
        year        = v$cbsa_csa_year,
        state_col   = "state"
      )
    }
    
    # Decode ZCTA for this vintage if a layer is provided
    if (!is.null(v$zcta)) {
      out$zcta <- decode_zcta(
        cand_sf      = cand_sf,
        zcta_sf      = v$zcta,
        zcta_colname = v$zcta_year,
        state_col    = "state"
      )
    }
    
    out
  })
  
  # Join all results back onto cand_sf
  core_areas_results <- sf::st_drop_geometry(cand_sf)
  
  for (v in core_areas_list) {
    
    if (!is.null(v$cbsa_csa)) {
      core_areas_results <- dplyr::left_join(core_areas_results, v$cbsa_csa, by = "row_id")
    }
    
    if (!is.null(v$zcta)) {
      core_areas_results <- dplyr::left_join(core_areas_results, v$zcta, by = "row_id")
    }
  }
  
  setDT(core_areas_results)
  
  # Join DECENNIAL block GEOIDs back onto candidate_addresses
  cols_to_add <- c("row_id", "geoid_2000", "geoid_2010", "geoid_2020")
  candidate_addresses[census_results[, ..cols_to_add], on = "row_id",
                      `:=`(
                        geoid_2000 = i.geoid_2000,
                        geoid_2010 = i.geoid_2010,
                        geoid_2020 = i.geoid_2020
                      )]
  
  # Join METRO/MICRO CBSA + CSA codes back onto candidate_addresses
  cols_to_add <- c(
    "row_id",
    "cbsa_code_2007", "cbsa_level_2007", "csa_code_2007",
    "cbsa_code_2010", "cbsa_level_2010", "csa_code_2010",
    "cbsa_code_2020", "cbsa_level_2020", "csa_code_2020",
    "zcta_2000", "zcta_2010", "zcta_2020"
  )
  candidate_addresses[core_areas_results[, ..cols_to_add], on = "row_id",
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
                      )]
  
  # Summarize match completeness across the three decennial block vintages
  candidate_addresses[, geoid_match :=
                        fifelse(
                          !enough_info,
                          "Not enough info",
                          fifelse(
                            is.na(geoid_2000) & is.na(geoid_2010) & is.na(geoid_2020),
                            "Matches not found",
                            fifelse(
                              is.na(geoid_2000) | is.na(geoid_2010) | is.na(geoid_2020),
                              "Some matches not found",
                              "Matched"
                            )
                          )
                        )]
  
  # Drop temporary helper columns and organize columns
  candidate_addresses <- candidate_addresses %>%
    select(-anchor_year_min, -anchor_year_max, -row_id, -lon, -lat, -enough_info) %>%
    relocate(geoid_match, .after = longitude_ver)
  
  
  # --------------------
  # LOOP PART F: Add Back to Main Dataset
  
  # Define column structure
  combined_cols <- c(
    # ---- company / identifiers ----
    "archive_version_year", "company", "abi", "year_established", 
    "primary_naics6_code", "naics6_descriptions",
    "subsidiary_number", "company_holding_status",
    
    # ---- address ----
    "address_line_1", "city", "state", "zipcode", "zip4", "combined_address",
    "address_verified", "attempt_succeeded",   # validation cols
    "geolocation_test", "verified_address",    # validation cols
    
    # ---- industry codes / descriptions ----
    "primary_sic_code", "sic6_descriptions",
    "sic_code", "sic6_descriptions_sic",
    "sic_code_1", "sic6_descriptions_sic1",
    "sic_code_2", "sic6_descriptions_sic2",
    "sic_code_3", "sic6_descriptions_sic3",
    "sic_code_4", "sic6_descriptions_sic4",
    
    # ---- geocoding / verification ----
    "latitude", "longitude",
    "n_geo", "latitude_avg", "longitude_avg",                     # validation cols
    "geolocation_verified", "n_attempts", "query_statuses",       # validation cols
    "matched_address", "benchmark", "vintage_input",              # validation cols
    "latitude_ver", "longitude_ver",                              # validation cols
    
    # ---- census geography (current + vintages) ----
    "census_block", "census_tract", "county_code", "fips_code",
    "geoid_match", "geoid_2000", "geoid_2010", "geoid_2020",      # validation cols
    
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
    candidate_addresses,
    subset,
    by = c("address_line_1", "city", "state", "zipcode", "zip4", "combined_address"),
    all.x = TRUE
  ) %>%
    select(all_of(combined_cols))
  
  setorder(combined, archive_version_year)
  
  
  # --------------------
  # LOOP PART G: Quality Checks — Address Validation Results
  
  # Address verification proceeds in two stages, summarised across three columns:
  #
  #   Stage 1 — USPS API (attempt_succeeded): each address is submitted to the
  #   USPS API with up to two attempts. The second attempt corrects the city via
  #   ZIP lookup before retrying.
  #
  #   Stage 2 — String matching (geolocation_test): addresses that failed Stage 1
  #   are matched against verified addresses using address_line_1 string
  #   comparisons to inherit a verified result.
  #
  #   address_verified reflects the final outcome of both stages.
  
  # Address QC: summarize verification stages
  qc_address[[i]] <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = str_remove(coalesce(verified_address, combined_address), ",(?=\\s*\\d{5})"),
      reported_address = str_remove(combined_address, ",(?=\\s*\\d{5})")
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- address ----
      reported_address,
      
      # ---- verification ----
      address_verified, attempt_succeeded,
      geolocation_test, verified_address,
    ) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    relocate(reported_address, .after = archive_versions_present) %>%
    (\(x) {setDT(x)} )()
  
  
  # --------------------
  # LOOP PART H: Quality Checks — Variation with Geolocation
  
  # Assesses the reliability of reported geolocation values for each address
  # using two approaches:
  #
  #   1) Deviation test: compare validated coordinates to reported/averaged coords.
  #   2) Dispersion test: measure spread of reported coords for the same address
  #      when the address appears multiple times.
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_geo_df <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = str_remove(coalesce(verified_address, combined_address), ",(?=\\s*\\d{5})")
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- geocoding ----
      latitude, longitude,              # reported coords (if present)
      latitude_avg, longitude_avg,      # averaged coords
      latitude_ver, longitude_ver,      # validated coords
      
      # ---- verification ----
      geolocation_verified, n_attempts, query_statuses, 
      matched_address, benchmark, vintage_input
    )
  
  
  # (a) Query QC: number of attempts, API status code, vintage used
  qc_geo$qc1[[i]] <- qc_geo_df %>%
    # Drop lat/lon fields here so QC focuses on request/response + matching metadata.
    # (Those coordinates may be analyzed in a separate QC stage.)
    select(
      -latitude, -longitude, -latitude_avg, -longitude_avg,
      -latitude_ver, -longitude_ver
    ) %>%
    mutate(
      # all_200:
      #   TRUE  = every recorded attempt returned HTTP 200
      #   FALSE = at least one attempt was non-200
      #   NA    = insufficient information to evaluate (missing statuses, or 
      #           fewer parsed statuses than the number of configured tries)
      #
      # query_statuses is assumed to be a single string like "200 | 200 | unverified".
      # geocoder_census_tries is assumed to represent how many attempts were made/allowed.
      all_200 = ifelse(
        is.na(query_statuses) |
          lengths(strsplit(query_statuses, " \\| ")) < length(geocoder_census_tries),
        NA,
        sapply(
          strsplit(query_statuses, " \\| "),
          function(x) all(trimws(x) == "200")
        )
      ),
      
      # matched_address_same:
      #   TRUE  = input address and matched_address are effectively the same after
      #           light normalization
      #   FALSE = they differ after normalization
      #   NA    = cannot evaluate due to missing address fields
      #
      # Normalization performed:
      #   - squish whitespace
      #   - drop trailing ZIP+4 (e.g., "-1234")
      #   - remove comma immediately before the 5-digit ZIP (to avoid punctuation-only diffs)
      matched_address_same = case_when(
        is.na(address) | is.na(matched_address) ~ NA,
        TRUE ~ str_remove_all(str_remove(str_squish(address), "-\\d{4}$"), ",(?=\\s*\\d{5})") ==
          str_remove_all(str_remove(str_squish(matched_address), "-\\d{4}$"), ",(?=\\s*\\d{5})")
      )
    ) %>%
    # Collapse across archive vintages:
    # For each unique record (everything except archive_version_year), collect 
    # the set of archive years observed and format them into compact ranges 
    # (e.g., "2000:2002, 2005").
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    # Reorder columns to make QC outputs easier to scan
    relocate(archive_versions_present, .after = address) %>%
    relocate(all_200, .after = query_statuses) %>%
    relocate(matched_address_same, .after = matched_address) %>%
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
  
  # (c) Dispersion QC: summarize the spread of reported coords within (abi, address)
  qc_geo$qc3[[i]] <- qc_geo_df %>%
    transmute(abi, address, latitude, longitude) %>%
    group_by(abi, address) %>%
    summarise(
      # Count complete (lat & lon) pairs available for this (abi, address)
      n_geo = sum(!is.na(latitude) & !is.na(longitude)),
      
      # Latitude distribution summary (return NA if no latitude values)
      lat_min    = ifelse(sum(!is.na(latitude)) > 0, min(latitude, na.rm = TRUE), NA_real_),
      lat_q1     = ifelse(sum(!is.na(latitude)) > 0, quantile(latitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lat_median = ifelse(sum(!is.na(latitude)) > 0, median(latitude, na.rm = TRUE), NA_real_),
      lat_mean   = ifelse(sum(!is.na(latitude)) > 0, mean(latitude, na.rm = TRUE), NA_real_),
      lat_q3     = ifelse(sum(!is.na(latitude)) > 0, quantile(latitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lat_max    = ifelse(sum(!is.na(latitude)) > 0, max(latitude, na.rm = TRUE), NA_real_),
      
      # Longitude distribution summary (return NA if no longitude values)
      lon_min    = ifelse(sum(!is.na(longitude)) > 0, min(longitude, na.rm = TRUE), NA_real_),
      lon_q1     = ifelse(sum(!is.na(longitude)) > 0, quantile(longitude, 0.25, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lon_median = ifelse(sum(!is.na(longitude)) > 0, median(longitude, na.rm = TRUE), NA_real_),
      lon_mean   = ifelse(sum(!is.na(longitude)) > 0, mean(longitude, na.rm = TRUE), NA_real_),
      lon_q3     = ifelse(sum(!is.na(longitude)) > 0, quantile(longitude, 0.75, na.rm = TRUE, names = FALSE, type = 7), NA_real_),
      lon_max    = ifelse(sum(!is.na(longitude)) > 0, max(longitude, na.rm = TRUE), NA_real_),
      
      # Flag if the spread of lat or lon across observations exceeds 0.02 degrees
      lat_spread_gt_002 = abs(max(latitude, na.rm = TRUE) - min(latitude, na.rm = TRUE)) > 0.02,
      lon_spread_gt_002 = abs(max(longitude, na.rm = TRUE) - min(longitude, na.rm = TRUE)) > 0.02,
      
      .groups = "drop"
    ) %>%
    # Only keep addresses with multiple geo observations (so "spread" is meaningful)
    filter(n_geo > 1) %>%
    relocate(lat_spread_gt_002, .after = lat_max) %>%
    (\(x) {setDT(x)} )()
  
  
  # --------------------
  # LOOP PART I: Quality Checks — Variation with Census Information
  
  # Preliminary assessments of the raw data indicated that the reported census
  # boundary values do not follow the expected pattern of decennial-only changes.
  # This QC step evaluates the reliability/consistency of census boundary values
  # reported for each address across query attempts and archive vintages.
  
  
  # Prep a consistent address field and keep only needed columns for QC
  qc_census_df <- combined %>%
    mutate(
      # Treat the "no match" sentinel as missing so it doesn't override the fallback
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified/standardized address; fall back to combined_address
      address = str_remove(coalesce(verified_address, combined_address), ",(?=\\s*\\d{5})")
    ) %>%
    select(
      abi, archive_version_year, address,
      
      # ---- census geography ----
      census_block, census_tract, county_code, fips_code,   # reported coords (if present)
      geoid_match, geoid_2000, geoid_2010, geoid_2020,      # validated coords
      
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
  
  # (a) Census Boundaries QC: compare given and verified census boundaries
  qc_census$qc1[[i]] <- qc_census_df %>%
    filter(geoid_match %in% "Matched") %>%
    mutate(
      # ---- coerce + standardize ----
      fips_code_chr    = as.character(fips_code),           # expect 5
      county_code_chr  = as.character(county_code),         # expect 3
      census_tract_chr = as.character(census_tract),        # expect 6
      census_block_chr = as.character(census_block),        # expect 4 (Block) or 1 (Block Group)
      
      # ---- length checks ----
      fips_ok         = str_length(fips_code_chr)    == 5,
      county_ok       = str_length(county_code_chr)  == 3,
      tract_ok        = str_length(census_tract_chr) == 6,
      
      # ---- fips_code (positions 1-5) ----
      fips_match_2000 = fips_ok & substr(geoid_2000, 1, 5) == fips_code_chr,
      fips_match_2010 = fips_ok & substr(geoid_2010, 1, 5) == fips_code_chr,
      fips_match_2020 = fips_ok & substr(geoid_2020, 1, 5) == fips_code_chr,
      
      fips_code_any_match = if_else(fips_ok, fips_match_2000 | fips_match_2010 | fips_match_2020, FALSE),
      fips_vintages_raw = paste0(
        if_else(fips_match_2000, "2000, ", ""),
        if_else(fips_match_2010, "2010, ", ""),
        if_else(fips_match_2020, "2020, ", "")
      ),
      fips_vintages = case_when(
        !fips_ok ~ NA_character_,
        !fips_code_any_match ~ "None",
        TRUE ~ str_remove(fips_vintages_raw, ", $")
      ),
      
      # ---- county_code (positions 3-5) ----
      county_match_2000 = county_ok & substr(geoid_2000, 3, 5) == county_code_chr,
      county_match_2010 = county_ok & substr(geoid_2010, 3, 5) == county_code_chr,
      county_match_2020 = county_ok & substr(geoid_2020, 3, 5) == county_code_chr,
      
      county_code_any_match = if_else(county_ok, county_match_2000 | county_match_2010 | county_match_2020, FALSE),
      county_vintages_raw = paste0(
        if_else(county_match_2000, "2000, ", ""),
        if_else(county_match_2010, "2010, ", ""),
        if_else(county_match_2020, "2020, ", "")
      ),
      county_vintages = case_when(
        !county_ok ~ NA_character_,
        !county_code_any_match ~ "None",
        TRUE ~ str_remove(county_vintages_raw, ", $")
      ),
      
      # ---- census_tract (positions 6-11) ----
      tract_match_2000 = tract_ok & substr(geoid_2000, 6, 11) == census_tract_chr,
      tract_match_2010 = tract_ok & substr(geoid_2010, 6, 11) == census_tract_chr,
      tract_match_2020 = tract_ok & substr(geoid_2020, 6, 11) == census_tract_chr,
      
      census_tract_any_match = if_else(tract_ok, tract_match_2000 | tract_match_2010 | tract_match_2020, FALSE),
      tract_vintages_raw = paste0(
        if_else(tract_match_2000, "2000, ", ""),
        if_else(tract_match_2010, "2010, ", ""),
        if_else(tract_match_2020, "2020, ", "")
      ),
      tract_vintages = case_when(
        !tract_ok ~ NA_character_,
        !census_tract_any_match ~ "None",
        TRUE ~ str_remove(tract_vintages_raw, ", $")
      ),
      
      # Classify block resolution based on the length of the raw block code:
      #   str_length == 4  -> full block-level precision
      #   str_length == 1  -> block-group-level precision
      #   anything else    -> NA (unexpected format, surfaces upstream issues)
      block_ok = case_when(
        str_length(census_block_chr) == 4 ~ "Blocks",
        str_length(census_block_chr) == 1 ~ "Block groups",
        TRUE                              ~ "FALSE"
      ),
      
      # ---- census_block ----
      block_match_2000 = block_ok != "FALSE" &
        substr(geoid_2000, 12, block_substr_end) == census_block_chr,
      block_match_2010 = block_ok != "FALSE" &
        substr(geoid_2010, 12, block_substr_end) == census_block_chr,
      block_match_2020 = block_ok != "FALSE" &
        substr(geoid_2020, 12, block_substr_end) == census_block_chr,
      
      census_block_any_match = if_else(
        block_ok != "FALSE",
        block_match_2000 | block_match_2010 | block_match_2020,
        FALSE
      ),
      
      block_vintages_raw = paste0(
        if_else(block_match_2000, "2000, ", ""),
        if_else(block_match_2010, "2010, ", ""),
        if_else(block_match_2020, "2020, ", "")
      ),
      
      block_vintages = case_when(
        block_ok == "FALSE"     ~ NA_character_,
        !census_block_any_match ~ "None",
        TRUE                    ~ str_remove(block_vintages_raw, ", $")
      )
    ) %>%
    rename(block_type = block_ok) %>%
    select(abi, archive_version_year, address, census_block, census_tract, 
           county_code, fips_code, geoid_2000, geoid_2010, geoid_2020, 
           fips_code_any_match, fips_vintages, county_code_any_match,
           county_vintages, census_tract_any_match, tract_vintages, block_type, 
           census_block_any_match, block_vintages) %>%
    group_by(across(-archive_version_year)) %>%
    summarise(
      archive_versions_present = format_year_ranges(archive_version_year),
      .groups = "drop"
    ) %>%
    relocate(archive_versions_present, .after = address) %>%
    (\(x) {setDT(x)} )()
  
  
  # (b) Metro/Micro QC: compare given and verified micro/metro census boundaries
  qc_census$qc2[[i]] <- qc_census_df %>% 
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
      
      # ---- flags: is the input missing? are all vintages missing? ----
      cbsa_level_input_na = is.na(cbsa_level_chr),
      cbsa_code_input_na  = is.na(cbsa_code_chr),
      csa_code_input_na   = is.na(csa_code_chr),
      
      cbsa_level_all_vintages_na = is.na(cbsa_level_2007_chr) & is.na(cbsa_level_2010_chr) & is.na(cbsa_level_2020_chr),
      cbsa_code_all_vintages_na  = is.na(cbsa_code_2007_chr)  & is.na(cbsa_code_2010_chr)  & is.na(cbsa_code_2020_chr),
      csa_code_all_vintages_na   = is.na(csa_code_2007_chr)   & is.na(csa_code_2010_chr)   & is.na(csa_code_2020_chr),
      
      # ---- OPTIONAL validity checks (edit if needed) ----
      cbsa_code_ok  = !cbsa_code_input_na  & str_length(cbsa_code_chr) == 5,
      csa_code_ok   = !csa_code_input_na   & str_length(csa_code_chr)  == 3,
      cbsa_level_ok = !cbsa_level_input_na & str_length(str_trim(cbsa_level_chr)) > 0,
      
      # ===================== CBSA CODE =====================
      cbsa_code_match_2007 = cbsa_code_ok & !is.na(cbsa_code_2007_chr) & cbsa_code_chr == cbsa_code_2007_chr,
      cbsa_code_match_2010 = cbsa_code_ok & !is.na(cbsa_code_2010_chr) & cbsa_code_chr == cbsa_code_2010_chr,
      cbsa_code_match_2020 = cbsa_code_ok & !is.na(cbsa_code_2020_chr) & cbsa_code_chr == cbsa_code_2020_chr,
      
      cbsa_code_any_match = case_when(
        cbsa_code_input_na ~ NA,               # input is NA -> NA
        cbsa_code_all_vintages_na ~ NA,        # all vintage fields NA -> NA
        !cbsa_code_ok ~ FALSE,                 # input present but invalid -> FALSE
        TRUE ~ (cbsa_code_match_2007 | cbsa_code_match_2010 | cbsa_code_match_2020)
      ),
      cbsa_code_vintages_raw = paste0(
        if_else(cbsa_code_match_2010, "2010, ", ""),
        if_else(cbsa_code_match_2020, "2020, ", "")
      ),
      cbsa_code_vintages = case_when(
        cbsa_code_input_na ~ NA_character_,
        cbsa_code_all_vintages_na ~ NA_character_,
        is.na(cbsa_code_any_match) ~ NA_character_,
        !cbsa_code_any_match ~ "None",
        TRUE ~ str_remove(cbsa_code_vintages_raw, ", $")
      ),
      
      # ===================== CBSA LEVEL =====================
      cbsa_level_match_2007 = cbsa_level_ok & !is.na(cbsa_level_2007_chr) & cbsa_level_chr == cbsa_level_2007_chr,
      cbsa_level_match_2010 = cbsa_level_ok & !is.na(cbsa_level_2010_chr) & cbsa_level_chr == cbsa_level_2010_chr,
      cbsa_level_match_2020 = cbsa_level_ok & !is.na(cbsa_level_2020_chr) & cbsa_level_chr == cbsa_level_2020_chr,
      
      cbsa_level_any_match = case_when(
        cbsa_level_input_na ~ NA,
        cbsa_level_all_vintages_na ~ NA,
        !cbsa_level_ok ~ FALSE,
        TRUE ~ (cbsa_level_match_2007 | cbsa_level_match_2010 | cbsa_level_match_2020)
      ),
      cbsa_level_vintages_raw = paste0(
        if_else(cbsa_level_match_2007, "2007, ", ""),
        if_else(cbsa_level_match_2010, "2010, ", ""),
        if_else(cbsa_level_match_2020, "2020, ", "")
      ),
      cbsa_level_vintages = case_when(
        cbsa_level_input_na ~ NA_character_,
        cbsa_level_all_vintages_na ~ NA_character_,
        is.na(cbsa_level_any_match) ~ NA_character_,
        !cbsa_level_any_match ~ "None",
        TRUE ~ str_remove(cbsa_level_vintages_raw, ", $")
      ),
      
      # ===================== CSA CODE =====================
      csa_code_match_2007 = csa_code_ok & !is.na(csa_code_2007_chr) & csa_code_chr == csa_code_2007_chr,
      csa_code_match_2010 = csa_code_ok & !is.na(csa_code_2010_chr) & csa_code_chr == csa_code_2010_chr,
      csa_code_match_2020 = csa_code_ok & !is.na(csa_code_2020_chr) & csa_code_chr == csa_code_2020_chr,
      
      csa_code_any_match = case_when(
        csa_code_input_na ~ NA,
        csa_code_all_vintages_na ~ NA,
        !csa_code_ok ~ FALSE,
        TRUE ~ (csa_code_match_2007 | csa_code_match_2010 | csa_code_match_2020)
      ),
      csa_code_vintages_raw = paste0(
        if_else(csa_code_match_2007, "2007, ", ""),
        if_else(csa_code_match_2010, "2010, ", ""),
        if_else(csa_code_match_2020, "2020, ", "")
      ),
      csa_code_vintages = case_when(
        csa_code_input_na ~ NA_character_,
        csa_code_all_vintages_na ~ NA_character_,
        is.na(csa_code_any_match) ~ NA_character_,
        !csa_code_any_match ~ "None",
        TRUE ~ str_remove(csa_code_vintages_raw, ", $")
      )
    ) %>%
    select(
      abi, archive_version_year, address,
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
    (\(x) {setDT(x)} )()
  
  
  # --------------------
  # LOOP PART J: Commit Results
  
  # The final table retains only verified outcomes plus the original address, as
  # the latter may be used differentially in the subsequent reformatting step.
  # 
  # All intermediate columns — including population patterns, API success/failures,
  # deviations from originally reported values, and all variable versions
  # (original, averaged, verified, and matched) — are preserved in the QC tables.
  
  
  finish_build[[i]] <- combined %>%
    # Remove originally reported longitude/latitude in favor of verified/averaged.
    select(-longitude, -latitude) %>%
    mutate(
      # Treat "no match" sentinel as NA so it doesn't suppress the fallback.
      verified_address = na_if(verified_address, "No address match found"),
      # Prefer verified address; fall back to combined_address.
      address = str_remove(coalesce(verified_address, combined_address), ",(?=\\s*\\d{5})"),
      reported_address = str_remove(combined_address, ",(?=\\s*\\d{5})"),
      # Prefer verified geolocation; fall back to averaged coordinates.
      longitude = coalesce(longitude_ver, longitude_avg),
      latitude  = coalesce(latitude_ver,  latitude_avg),
    ) %>%
    select(
      # ---- Address: drop raw inputs and verification columns ----
      -address_line_1, -city, -state, -zipcode, -zip4, -attempt_succeeded, 
      -geolocation_test, -combined_address, -verified_address,
      
      # ---- Geolocation: drop raw geocoding and verification columns ----
      -latitude_avg, -longitude_avg, -latitude_ver, -longitude_ver,
      -n_geo, -n_attempts, -query_statuses, -matched_address,
      -benchmark, -vintage_input,
      
      # ---- Census Boundaries: drop raw boundary codes ----
      -census_block, -census_tract, -county_code, -fips_code,
      -cbsa_level, -cbsa_code, -csa_code
    ) %>%
    # Organize columns
    relocate(address, .after = company_holding_status) %>%
    relocate(address_verified, .after = address) %>%
    relocate(reported_address, .after = address_verified) %>%
    relocate(longitude, .after = sic6_descriptions_sic4) %>%
    relocate(latitude, .after = longitude) %>%
    relocate(geoid_match, .after = geoid_2020)
  
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}


## --------------------
## SUBSECTION C2: Save Result


# Combine all data tables in the list into one data table
finish_build <- rbindlist(finish_build, use.names = TRUE, fill = TRUE)
qc_address <- rbindlist(qc_address, use.names = TRUE, fill = TRUE)

qc_geo$qc1 <- bind_rows(qc_geo$qc1, .id = "i")
qc_geo$qc2 <- bind_rows(qc_geo$qc2, .id = "i")
qc_geo$qc3 <- bind_rows(qc_geo$qc3, .id = "i")

qc_census$qc1 <- bind_rows(qc_census$qc1, .id = "i")
qc_census$qc2 <- bind_rows(qc_census$qc2, .id = "i")


# Commit results for large output (HPC and local compatible)
outfile_df <- file.path(outdir, "Verified Result", sprintf(str_c("Step 2 HPC_2026 Format_Verified Result_", nums[1], " to ", nums[2], "_slurmArray_%03d.parquet"), idx))
write_parquet(finish_build, outfile_df)

outfile_address <- file.path(outdir, "Address QC", sprintf(str_c("Step 2 HPC_2026 Format_Address QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
write_list_to_duckdb(qc_address, outfile_address)

outfile_geo <- file.path(outdir, "Geo QC", sprintf(str_c("Step 2 HPC_2026 Format_Geo QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
write_list_to_duckdb(qc_geo, outfile_geo)

outfile_census <- file.path(outdir, "Census QC", sprintf(str_c("Step 2 HPC_2026 Format_Census QC_", nums[1], " to ", nums[2], "_slurmArray_%03d.csv"), idx))
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
