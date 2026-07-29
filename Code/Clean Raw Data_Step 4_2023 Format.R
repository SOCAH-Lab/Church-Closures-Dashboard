## ----------------------------------------------------------------
## Assess the Validity of Using K-Means Geographic Clustering to Identify Relocations
## 
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: May 15th, 2025
## Date Modified: July 27th, 2026
## 
## Description: Communities can be equally impacted by the loss of social
##              services or other support offered by a religious institution
##              if it closes or relocates outside of the community. To capture
##              this, closures resulting from a relocation are also quantified,
##              though at varying gradients, as some attendees may be able to
##              travel greater distances than others. This effort does not
##              attempt to reconcile the potential role of digital or remote
##              service delivery; instead, it focuses exclusively on the impact
##              to physical, in-person services.
## 
##              In order to support this computation, PO Boxes need to be
##              specially handled. A possible assumption is that business owners
##              open PO Boxes near a physical location. If this assumption holds, 
##              then the immediately preceding or following physical address 
##              should be reasonably close to the listed PO Box.
## 
##              This document was developed to assess the relative spatial
##              proximity of addresses and PO Boxes associated with a given
##              business. Due to the computational cost of these assessments
##              and project time constraints at the time of development,
##              geographic proximities were kept broad and not all assumptions
##              were validated prior to the prototype release.
## 
##              This method was later determined to rely on assumptions too
##              strong to justify its use in the final methods. Instead,
##              businesses with a PO Box listed within the selected date range
##              are excluded from the metrics calculation altogether.
##              Additionally, the relocation identification method was
##              completely revised for the 2026 Formatted version. This
##              document is retained for posterity.
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
## NOTE: In Spring 2026, the pipeline developed for the Summer 2025 symposium
##       prototype was lightly refactored for clarity and rerun to process all
##       entries not covered in the initial pass. Core methods remained 
##       consistent with the prototype. An updated dataset delivered in May 2026 
##       prompted further expansion of the pipeline to support two designated 
##       format variations: the 2023 Format and the 2026 Format.
##
##       The Spring 2026 refactoring was not completed across all steps. Steps
##       1 and 2 reflect the full updates, with results generated for the
##       entire dataset. Steps 3-5 were updated for reporting clarity only and
##       continue to process the restricted 2023 Format produced for the
##       Summer 2025 symposium.
## 
##       Results from the refactored pipeline are stored in:
##         "~/KEEP LOCAL/From Clean Raw Data/Step *_2023 Format/"
##       Results from the original prototype run are archived in:
##         "~/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED"
##       GeoJSON files visualized on the dashboard are in:
##         "~/Dashboard Datasets/"
##       and reflect data as of June 2025.
## 
## Sections:
##    - SET UP THE ENVIRONMENT
##    - LOAD IN THE DATA
## 
##    - PART A: Quantifying Relocations Outside the Community
##        * SUBSECTION A1: Summarize Geographic Spread of Address Locations
##        * SUBSECTION A2: Geographic Clustering Among Businesses That Relocated
## 
##    - PART B: Cluster-Based Address Aggregation for Stationary Businesses
##        * SUBSECTION B1: Geographic Clustering Among Businesses That Did Not Relocate
##        * SUBSECTION B2: Representative Address Assignment
##        * SUBSECTION B3: Save Results

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# Initiate the package environment.
# renv::init()
renv::restore()

suppressPackageStartupMessages({
  library("readr")            # Reads in CSV and other delimited files
  library("dplyr")            # Data manipulation and transformation
  library("tidyr")            # Reshape/tidy data (pivot, separate)
  library("stringr")          # String operations
  library("purrr")            # Functional programming tools
  library("ggplot2")          # Graphics and visualization
  library("dbscan")           # Density-based clustering (DBSCAN)
  library("future.apply")     # Parallel processing
})

# Set up the plan for parallel processing.
plan(multisession, workers = 4)

# Define the "not in" operation
"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## LOAD IN THE DATA

# Read in previously generated results.
step_3 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 03_Church Wide_Verified Geolocation_06.16.2025.csv.gz") %>% as.data.frame()

# Only data with a verified geocoordinates will be used for the prototype.
# Stringency for this will be reviewed with the team and more closely assessed 
# for subsequent iterations of the dashboard.

# Identify ABIs where all entries passed geocoordinate verification.
abi_all_pass <- step_3 %>%
  group_by(abi) %>%
  summarize(all(verifiedGeo %in% "TRUE")) %>%
  ungroup() %>%
  as.data.frame()

# As noted in Step 3, ~82% of entries were verified, corresponding to ~80% of ABI.
round(table(abi_all_pass$`all(verifiedGeo %in% "TRUE")`)/nrow(abi_all_pass)*100, digits = 2)

# Subset to ABI where all entries passed geocoordinate verification.
step_3 <- step_3 %>%
  filter(abi %in% abi_all_pass[abi_all_pass$`all(verifiedGeo %in% "TRUE")` == TRUE, "abi"]) %>%
  # Replace original coordinates with verified geocoordinates.
  rename(latitude_remove = latitude, longitude_remove = longitude) %>% 
  rename(latitude = verifiedLat, longitude = verifiedLon) %>%
  # Retain only required columns and reorder for consistency.
  relocate(verifiedGeo, .after = longitude)


# Running this algorithm on the full dataset was not feasible within the
# time constraints of the Summer 2025 symposium prototype. Consequently,
# approximately half of the data was able to be processed and used to generate 
# results for visualization across the contiguous US.
#
# Refer to the generated results to determine which entries were retained
# for downstream processing.




## ----------------------------------------------------------------
## PART A: Quantifying Relocations Outside the Community

# The original methods from Dr. Song quantified relocations by address change:
# different census tract, city, county, or state; and by coordinate distance:
# >= 100 meters, >= 500 meters, or >= 1000 meters.
# 
# The physical distance implied by a census boundary change varies depending
# on whether the region is densely populated (smaller boundaries) or rural
# (larger boundaries). Therefore, the focus will remain on coordinate-based
# distances to ensure outcomes are treated more consistently.


## --------------------
## SUBSECTION A1: Summarize Geographic Spread of Address Locations

# Entries with missing geocoordinates cannot be processed in subsequent
# steps and will therefore be removed. None are found to be missing.
any(is.na(step_3$longitude))
any(is.na(step_3$latitude))

move_check <- step_3 %>%
  # Keep rows with complete coords (both longitude and latitude present).
  filter(!is.na(longitude) & !is.na(latitude)) %>%
  # For each ABI, compute coordinate range (spread).
  group_by(abi) %>%
  summarize(
    diffLon = abs(max(longitude) - min(longitude)),
    diffLat = abs(max(longitude) - min(longitude)),
    .groups = "drop"
  )


# Gross location changes are classified by the maximum difference across
# longitude and latitude coordinates. A threshold of 0.001 degrees is
# applied uniformly to both coordinates, which approximates one city block.
#
# Note that the relationship between degrees and euclidean distance differs
# between longitude and latitude, and varies by location on the earth's
# surface. This check intentionally simplifies those details to provide a
# broad, high-level sense of address movement for a given business.

# Conversion for 0.002 degrees ~= 222 m  => 111000 m per degree
deg_to_miles <- 111000 / 1609.344  # miles per degree

# Classify location changes by the maximum distance detected across both 
# geocoordinates.
move_check <- move_check %>%
  mutate(
    # Convert each axis difference to miles (approx).
    lon_mi = diffLon * deg_to_miles,
    lat_mi = diffLat * deg_to_miles,
    
    # Classify distance, label triggered by the larger of the two.
    max_mi = pmax(lon_mi, lat_mi, na.rm = TRUE),
    
    dist_band = case_when(
      max_mi <= 0.001*deg_to_miles  ~ "within 1 block",
      max_mi <= 1  ~ "within 1 mile",
      max_mi <= 5  ~ "1–5 miles",
      max_mi <= 10 ~ "5–10 miles",
      max_mi <= 50 ~ "10–50 miles",
      TRUE         ~ "> 50 miles"
    )
  )

# Define the distance levels.
bands <- c("within 1 block", "within 1 mile", "1–5 miles", "5–10 miles", "10–50 miles", "> 50 miles")

# Summarize the results.
out <- move_check %>%
  mutate(dist_band = factor(dist_band, levels = bands, ordered = TRUE)) %>%
  count(dist_band, name = "n") %>%
  mutate(pct = round(100 * n / sum(n), 2))

# Nearly 86% of businesses remained within one block of their original
# location. An additional 5% moved within one mile, and 7% moved between
# one and five miles. 1.6% moved between five and ten miles, while less
# than 1% moved more than ten miles (0.08% moved more than 50 miles).
out


## --------------------
## SUBSECTION A2: Geographic Clustering Among Businesses That Relocated

# The Yale Center for Geospatial Solutions (YCGS) recommended associating
# PO Boxes with physical addresses by creating simple features and applying
# point-in-polygon matching. However, this method is time-intensive, and the
# project PI, Professor Ransome, did not have a physical distance threshold
# in mind and instead wanted to explore distances empirically.
# 
# Although unconventional, DBSCAN clustering was used as a simpler and faster
# way to identify clusters by geocoordinates and collapse all addresses within
# each cluster. Due to time constraints leading up to the Summer 2025 symposium,
# this method was only lightly tested. Only addresses assumed to have not moved
# outside the region of impact were carried forward; the methods for identifying
# moves outside the community were tabled for further development after the
# symposium.
# 
# The package dbscan(eps = 1, minPts = 2) groups observations into clusters 
# when at least two points fall within a radius of 1 (in the units of the input 
# coordinates), labeling points that don’t meet this density requirement as 
# noise (0) and assigning clustered points to groups 1, 2, etc. Cluster labels 
# are applied independently within each business ID.
# 
# DBSCAN cluster label per ABI: 
#   - 0 = not grouped with other addresses
#   - 1,2,... = cluster membership; addresses nearby each other

cluster_moved <- step_3 %>%
  # Focus only on clustering the businesses that might have moved
  filter(abi %!in% pull(move_check[move_check$dist_band %in% "within 1 block", "abi"])) %>%
  # Keep rows with complete coords (both longitude and latitude present)
  filter(!is.na(longitude) & !is.na(latitude)) %>%
  # Generate the clusters for addresses within a business.
  group_by(abi) %>%
  mutate(area = dbscan(as.matrix(cbind(longitude, latitude)), eps = 1, minPts = 2)$cluster) %>%
  ungroup() %>%
  # Reorder columns and convert to dataframe.
  relocate(area, .after = abi) %>%
  as.data.frame()

# Most entries fell into a single cluster, with a small number assigned to a 
# second; ~350 were not clustered with other addresses.
cluster_moved %>% count(area)

# 99.8% of PO Boxes got clustered with a nearby address. The remaining were
# not clustered with other entries.
cluster_moved %>%
  filter(is_po_box) %>%
  count(area, name = "n") %>%
  mutate(pct = round(100 * n / sum(n), 2)) %>%
  arrange(area)

# Quantify the number of clusters per ABI based on the number of available entries.
confirm_dim <- cluster_moved %>%
  group_by(abi) %>%
  summarise(
    n_rows    = n(),
    clusters  = str_flatten(sort(unique(area)), collapse = ", "),
    .groups   = "drop"
  ) %>%
  left_join(move_check, by = "abi") %>%
  as.data.frame()

# Businesses with two to four entries resulted in no clusters, while those
# with two to six entries formed one or two clusters. Most businesses
# collapsed into a single cluster, with only two businesses forming two
# distinct clusters.
table(`# Rows` = confirm_dim$n_rows, `# Clusters` = confirm_dim$clusters)

# Entries with the largest relocations detected (> 50 miles) resulted in the
# greatest heterogeneity, with some businesses grouped into a single cluster.
# All other distance bands were grouped into a single cluster, including
# those with coordinate differences between 10 and 50 miles.
#
# These results highlight the limitations of using dbscan() clustering for this
# application.
with(
  transform(confirm_dim, dist_band = factor(dist_band, levels = bands[-1], ordered = TRUE)),
  table(`Distance Band` = dist_band, `# Clusters` = clusters)
)

# Annotate presence of all PO Boxes.
cluster_moved <- cluster_moved %>%
  mutate(
    is_po_box = str_detect(
      coalesce(address_line_1, ""),
      regex("\\bP\\s*\\.?\\s*O\\s*\\.?\\s*Box\\b", ignore_case = TRUE)
    )
  )

# Quantify how many businesses that used a PO Box were clustered with a
# nearby physical address.
po_box_grouped_with_non <- cluster_moved %>%
  group_by(abi, area) %>%
  summarize(
    has_po_box     = any(is_po_box %in% TRUE, na.rm = TRUE),
    has_non_po_box = any(is_po_box %in% FALSE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(grouped_with_phys = has_non_po_box)

# 99.9% of PO Box entries got clustered with a nearby physical address.
round(prop.table(table(po_box_grouped_with_non$grouped_with_phys))*100, digits = 2)

# Of the entries that did not cluster with a nearby physical address, some
# had no associated physical address in the data (i.e., only a PO Box on
# record), while others were not within proximity of any candidate physical
# address.
cluster_moved %>%
  filter(abi %in% pull(po_box_grouped_with_non[po_box_grouped_with_non$grouped_with_phys == FALSE, "abi"]))


## ----------------------------------------------------------------
## PART B: Cluster-Based Address Aggregation for Stationary Businesses

# The results above were promising, but dbscan() clustering is not considered
# standard practice and will not be used in subsequent iterations of the
# algorithm. Additionally, it was later determined that the assumption of
# associating a PO Box with a nearby physical address was too strong to be
# adequately verified. Therefore, businesses that filed under a PO Box at any 
# point within the selected date range will be excluded from metric calculations. 
# 
# This still leaves the task of associating physical addresses with moves
# outside of a community. The following algorithm does not address this, but
# uses dbscan() to simplify the handling of addresses that showed no indication
# of a move (`move_check$dist_band = "within 1 block"`). One representative
# address was then assigned to each cluster.
# 
# NOTE: At the time of development, a coarser geolocation threshold was used
#       in place of the more accurate and granular criterion represented above.
#       Rather than filtering by `move_check$dist_band = "within 1 block"`,
#       a change of up to 0.5 degrees in longitude or latitude was used.
# 
# NOTE: PO Boxes were not excluded in this step, as that decision was reached 
#       after the initial prototype was developed.
# 
# The following algorithm was split into two parts:
#   1. Grouping addresses into areas (labeled "Area #1", "Area #2", etc.) by 
#      k-means clustering and aggregating results by area
#   2. Annotating each area with physical address metadata, preferably a
#      verified address or at least an entry with verified geocoordinates


## --------------------
## SUBSECTION B1: Geographic Clustering Among Businesses That Did Not Relocate

# 85% of businesses with a maximum difference of 0.5 degrees in longitude or
# latitude fall within a 1-block radius; however, this threshold does not
# exclude some larger moves, such as those between 10 and 50 miles.
# Approximately 2% of businesses filtered by this criterion moved more than
# 5 miles.
move_check %>%
  filter(diffLon < 0.5 & diffLat < 0.5) %>%
  mutate(dist_band = factor(dist_band, levels = bands, ordered = TRUE)) %>%
  (\(x) round(prop.table(table(x$dist_band)) * 100, digits = 2))()

# Group addresses by how similar their longitude and latitude are with one another.
cluster_not_moved <- step_3 %>%
  # Focus only on clustering businesses that moved no more than 1 degree in
  # longitude or latitude.
  filter(abi %in% pull(move_check[move_check$diffLon < 0.5 & move_check$diffLat < 0.5, "abi"])) %>%
  # Keep rows with complete coords (both longitude and latitude present).
  filter(!is.na(longitude) & !is.na(latitude)) %>%
  # Generate the clusters for addresses within a business.
  group_by(abi) %>%
  mutate(area = dbscan(as.matrix(cbind(longitude, latitude)), eps = 1, minPts = 2)$cluster) %>%
  ungroup() %>%
  # Reorder columns and convert to dataframe.
  relocate(area, .after = abi) %>%
  as.data.frame()

# Quantify the number of clusters per ABI based on the number of available entries.
confirm_dim <- cluster_not_moved %>%
  group_by(abi) %>%
  summarise(
    n_rows    = n(),
    clusters  = str_flatten(sort(unique(area)), collapse = ", "),
    .groups   = "drop"
  ) %>%
  as.data.frame()

# Entries with only one address were not assigned a cluster, as expected,
# while those with more than one entry (two to six) were all assigned to
# the same cluster.
# 
# Because these entries include those with moves exceeding 10 miles, these
# results highlight the limitations of using dbscan() clustering for this
# application.
table(`# Rows` = confirm_dim$n_rows, `# Clusters` = confirm_dim$clusters)

# Entries with the largest relocations detected (> 50 miles) resulted in the
# greatest heterogeneity, with some businesses grouped into a single cluster.
# All other distance bands were grouped into a single cluster, including
# those with coordinate differences between 10 and 50 miles.
#
# These results highlight the limitations of using dbscan() clustering for this
# application.
confirm_dim <- confirm_dim %>%
  left_join(move_check, by = "abi")

with(
  transform(confirm_dim, dist_band = factor(dist_band, levels = bands[], ordered = TRUE)),
  table(`Distance Band` = dist_band, `# Clusters` = clusters)
)

# Because there is a clean separation between noise and clusters, both
# values 0 and 1 are assigned the same area designation: "Area #1".
cluster_not_moved[cluster_not_moved$area %in% c(0, 1), "area"] <- "Area #1"

# Aggregate results over all entries assigned to the same area.
step_4 <- cluster_not_moved %>%
  group_by(abi, area) %>%
  # Sum all year columns (columns starting with "20") across entries
  # within each group.
  summarise(across(starts_with("20"), sum)) %>%
  # Ensure subsequent operations are applied row-wise.
  rowwise() %>%
  # Add a flag column `all_counts_0_or_1`: TRUE if all year column values
  # for a given row are 0 or 1, FALSE otherwise.
  mutate(all_counts_0_or_1 = all(across(starts_with("20"), ~ .  %in% c(0, 1)))) %>%
  # Remove row-wise grouping to prevent unintended side effects in
  # subsequent operations.
  ungroup() %>%
  as.data.frame()

# All entries passed the quality control check for aggregation: no
# duplicate years-open or years-closed values were introduced.
step_4$all_counts_0_or_1 %>% (\(x) { round((table(x)/nrow(step_4))*100, digits = 2) }) ()

# Retain only entries that passed the quality control check.
step_4 <- step_4 %>%
  filter(all_counts_0_or_1 == TRUE)


## --------------------
## SUBSECTION B2: Representative Address Assignment

# Ideally, the selected address is verified by the USPS database and has 
# validated longitude/latitude coordinates from the US Census Bureau's Geocoder 
# database. Additional if statements are used to prioritize by this preference 
# in the following order: verified address > verified geography > first row.
# 
# NOTE: The algorithm does not discriminate against PO Box addresses if they 
#       meet one of the criteria above.
# 
# NOTE: The geocoordinates of the selected address were retained without
#       aggregation, assuming moves were small enough to ignore address
#       variations. This assumption was later found to be incorrect and was
#       revised in the 2026 Formatted pipeline.


build <- list()   # Initialize the empty lists
pb = txtProgressBar(min = 0, max = length(unique(step_4$abi)), style = 3)   # Initialize progress bar

for( i in 1:length(unique(step_4$abi)) ) {
  # Pull all candidate addresses for the given ABI from the Step 3 results.
  subset = step_3[step_3$abi %in% unique(step_4$abi)[i], ]
  
  if( nrow(subset) == 1 ) {
    add_address <- data.frame(address_line_1 = subset[, "address_line_1"],
                              address_line_2 = subset[, "address_line_2"],
                              city = subset[, "city"],
                              state = subset[, "state"],
                              zipcode = subset[, "zipcode"],
                              zipcode_ext = subset[, "zipcode_ext"],
                              compiled_address = subset[, "compiled_address"],
                              address_verified = subset[, "address_verified"],
                              lonLat_test = subset[, "lonLat_test"],
                              latitude = subset[, "latitude"],
                              longitude = subset[, "longitude"],
                              verifiedGeo = subset[, "verifiedGeo"])
    
  } else if( nrow(subset) > 1 ) {
    
    # Prioritize entries with both a verified address and verified geolocation.
    ideal_address <- which(subset$address_verified %in% TRUE & subset$verifiedGeo %in% TRUE)
    
    # If none meet this criterion, save the entry with a verified address.
    if( length(ideal_address) == 0 ) {
      ideal_address <- which(subset$address_verified %in% TRUE)
      #message("Only select verified addresses.")
      
      # If none meet this criterion, save the entry with verified geography.
      if( length(ideal_address) == 0 ) {
        ideal_address <- which(subset$verifiedGeo %in% TRUE)
        message("No verified address. Only select verified geography.")
        
        # If none meet this criterion, save the first row.
        if( length(ideal_address) == 0 ) {
          ideal_address <- 1
          message("No verified address or geography. Randomly select the first..")
          
        }
      }
    }
    
    # If multiple entries meet this criterion, keep the first one.
    add_address <- data.frame(address_line_1 = subset[, "address_line_1"][ideal_address[1]],
                              address_line_2 = subset[, "address_line_2"][ideal_address[1]],
                              city = subset[, "city"][ideal_address[1]],
                              state = subset[, "state"][ideal_address[1]],
                              zipcode = subset[, "zipcode"][ideal_address[1]],
                              zipcode_ext = subset[, "zipcode_ext"][ideal_address[1]],
                              compiled_address = subset[, "compiled_address"][ideal_address[1]],
                              address_verified = subset[, "address_verified"][ideal_address[1]],
                              lonLat_test = subset[, "lonLat_test"][ideal_address[1]],
                              latitude = subset[, "latitude"][ideal_address[1]],
                              longitude = subset[, "longitude"][ideal_address[1]],
                              verifiedGeo = subset[, "verifiedGeo"][ideal_address[1]])
  }
  
  # Save the selected metadata entry.
  build[[i]] <- bind_cols(data.frame("abi" = unique(subset$abi)), add_address)
  
  # Print the for loop's progress.
  setTxtProgressBar(pb, i)
}

# Combine all data tables in the list into one data table.
result <- do.call(rbind, build)

# Merge the selected addresses back into the Step 4 data frame.
step_4 <- left_join(result, step_4[step_4$abi %in% result$abi, ], by = "abi") %>%
  relocate(area, .after = abi)


# Some selected addresses failed the address validation step, resulting in 
# "No address match found" being placed in the compiled_address column. The 
# following cleans these up by recompiling the address components from those 
# selected above.
fill_compiled <- which(step_4$compiled_address %in% "No address match found")

for (i in seq_along(fill_compiled)) {
  
  # Get the target row (1-row tibble)
  row_i <- step_4 %>% dplyr::slice(fill_compiled[i])
  
  # Build "addr, city, state, ..." + "zip5-zip4" (drop NAs; drop trailing "-" if no zip4)
  step_4[fill_compiled[i], "compiled_address"] <-
    str_flatten(purrr::map_chr(row_i[, 3:6], ~ ifelse(is.na(.), "", as.character(.))), collapse = ", ") %>%
    str_replace(", , ", ", ") %>%
    (\(x) str_c(x,
                str_flatten(purrr::map_chr(row_i[, 7:8], ~ ifelse(is.na(.), "", as.character(.))), collapse = "-"),
                sep = " "))() %>%
    str_replace("-$", "")
}


## --------------------
## SUBSECTION B3: Save Results

#' @description
#' Codebook for new output fields produced during the data cleaning and
#' validation step. All other fields were present in the Step 3 form of
#' the data.
#'
#' @field area The k-means cluster that the years-open and years-closed
#'             results represent.
#'
#' @field `all address fields` All address fields (`address_line_1`,
#'                             `address_line_2`, `city`, `state`, `zipcode`,
#'                             `zipcode_ext`, and `compiled_address`) selected
#'                             from one of the addresses clustered to the same
#'                             area and then aggregated together. Exact address
#'                             details and geocoordinates do not correspond to
#'                             the actual location for the years-open and
#'                             years-closed information.
#'
#' @field `other metadata` All other metadata fields (`address_verified`,
#'                         `lonLat_test`, and `verifiedGeo`) relate to the
#'                         selected address associated with this cluster area.
#'
#' @field latitude/longitude The verified longitude/latitude of the selected 
#'                           address.
#'
#' @field `2001:2021` Column-wise sum of all entries associated with the given
#'                    business ID and clustered area.
#'
#' @field all_counts_0_or_1 Boolean. TRUE if all date entry sums for the given
#'                          business ID are equal to 0 or 1.

# # Commit results.
# write_csv(step_3, "./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 04_Cluster Addresses and Collapse By Area_06.17.2025.csv.gz")

# Load in the pre-produced test results for evaluation.
step_4 <- read_csv("./Data/Results/KEEP LOCAL/From Clean Raw Data/Summer 2025 Dashboard Prototype_ARCHIVED/Step 04_Cluster Addresses and Collapse By Area_06.17.2025.csv.gz",
                   col_types = cols(...1 = col_skip())) %>% as.data.frame()


