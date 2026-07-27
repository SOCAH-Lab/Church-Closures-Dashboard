## ----------------------------------------------------------------
## Verify Geocoordinates via Address-Based Geocoding
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 24th, 2026
## 
## Description: 
## 
## NOTE: Under the Data Use Agreements (DUAs) with Data Axle and the USPS API 
##       license, raw data cannot be publicly distributed and is stored locally 
##       in "~/KEEP LOCAL" directories. Some code or results may also be 
##       restricted. All publicly distributed results are summarized, and 
##       publicly distributed code has been constructed to avoid referencing 
##       individual-level data. Executing the code below requires access to the 
##       raw data and results.
## 
##       API keys are user-specific and, where applicable, instructions have 
##       been provided to help users obtain their own and configure them locally 
##       or on a High Performance Computer (HPC).
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Validating the Geocoordinates Using the US Census Bureau Geocoder API
##        * SUBSECTION A1: Algorithm
##        * SUBSECTION A2: Save Results
## 
##    - PART B: Assess the Algorithms Performance
##    - PART C: Subset for Next Steps

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("dplyr")            # Data manipulation and transformation
  library("stringr")          # String operations
  library("purrr")            # Functional programming tools
  library("httr")             # HTTP requests (GET/POST) for web APIs
  library("jsonlite")         # JSON parsing and generation (to/from R objects)
  library("future.apply")     # Parallel processing
  library("progress")         # Progress bars
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Load in the functions.
source("./Code/Support Functions/General.R")
source("./Code/Support Functions/For Step 3_2023 Format.R")

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Read in previously generated results.
step_2 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 02_Church Wide_Convert to Preferred USPS Address_COMPLETED_06.07.2025", 
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()

# Running this algorithm on the full dataset was not feasible within the
# time constraints of the Summer 2025 symposium prototype unveiling.
# Consequently, a random sample of approximately one-third of the data
# was used to produce results for visualization across the contiguous US.
#
# As the sample was drawn randomly, refer to the generated results to
# determine which entries were retained for downstream processing.




## ----------------------------------------------------------------
## PART A: Validating the Geocoordinates Using the US Census Bureau Geocoder API


# Reformat into the wide version
# Annotate with the seven categories
# Annotate temporal moves outside different communities


# Community Algorithm:

# if max/min within 1 block do not process

# 1. Create communities - boundaries as squares
# Draw a square around the first address that contains the largest boundary circle
# Iterate over all addresses contained and move the center to be the average
#   of these. When no more new addresses stop. The community is set.
# Do this for any addresses outside the square until block communities are
#   created. Allow overlap, but include a column saying which communities overlap
#
# 2. Assess distances away from center of a community
# Each address is assessed for max circle distance away from defined community
#   center. Do this for all communities and allow overlap (1mi from community
#   #1 and 10mi from community #2, etc.)
# 
# Values
#   - Distance to walk depreciated
#   - Distance to take public transit depreciated
#   - Distance to drive depreciated
#   - Within 1 mile
#   - Within 1-5 miles
#   - Within 5-10 miles
#   - Within 10-50 miles



# Just calculate the closure and summarize the movement
# i.e.: 10% moved more than 1-5 miles away, 5% had a drop of walk ability.
# i.e.: 4% detected more than 1 community (80% two and 20% three)
# 
# Only closed due to a move if it moved outside of the community. Also note
# if later addresses fell into previously identified community.



