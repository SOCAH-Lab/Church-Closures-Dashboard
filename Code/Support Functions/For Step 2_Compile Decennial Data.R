## ----------------------------------------------------------------
## Define functions for creating decennial year files, used in Step 2.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: June 30th, 2026
## Date Modified: July 7th, 2026
## 
## Description: During data cleaning/validation, longitude/latitude coordinates 
##              are used to assign census classifications (block, tract, county, 
##              state, CBSA, CSA, and ZCTA) to each address across decennial 
##              periods (2000, 2010, 2020). To improve efficiency and accuracy, 
##              these classifications are precompiled by state and loaded for 
##              each batch as needed.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
##
## Functions:
##    1. read_zip_sf: Read a TIGER/Line shapefile directly from a ZIP archive as 
##       an sf object. Uses GDAL's `/vsizip/` virtual filesystem so the ZIP does 
##       not need to be manually extracted.
## 
##    2. find_core_area_zips: Find national ZIP files (CBSA/CSA + ZCTA + States) 
##       under a root directory. Searches `root` for specific TIGER/Line (and 
##       2007 “fe_” vintage) ZIP filenames and returns their full paths. Intended 
##       for *national* files (not per-state folders).
## 
##       Strict behavior: errors if any required ZIP is missing or ambiguous 
##       (multiple matches).
## 
##    3. preflight_tiger_outputs: Preflight check for TIGER inputs/outputs. 
##       This function is designed to be run *before* downstream TIGER 
##       processing. It validates (a) raw ZIP inputs exist and are readable and 
##       (b) expected output GeoPackages exist and contain the expected layers.
## 
##       Oxygen labels used in console messages:
##            - OK:      Requirement satisfied.
##            - MISSING: Expected file(s)/layer(s) absent or incomplete.
##            - ERROR:   An operation failed (e.g., cannot read layers).
##            - NOTE:    Informational message; not necessarily a failure.
## 
##       Checks performed
##            0) Core-areas raw ZIP availability:
##                  - CBSA/CSA: 2007, 2010, 2020
##                  - ZCTA:     2000, 2010, 2020
##                  - STATES:   2000, 2010, 2020  (used to annotate ZCTA with state)
##            1) Core-areas output:
##                  - core_areas.gpkg exists
##                  - contains required layers (cbsa_csa_2007, ..., zcta_2020)
##            2) State raw inputs (for requested geography):
##                  - Only state folders where at least one matching ZIP is 
##                    detected are checked/reported
##                  - For each detected state: verify ZIP presence for 
##                    2000/2010/2020 via filename patterns
##                  - Lightweight ZIP "layer check": confirm GDAL can list a 
##                    layer inside each present ZIP
##            3) State outputs (for requested geography):
##                  - Output file exists for each detected state:
##                    blocks_statefp_XX_2000_2010_2020.gpkg  OR  
##                    bg_statefp_XX_2000_2010_2020.gpkg
##                  - Output contains expected layers:
##                    blocks_2000, blocks_2010, blocks_2020  OR  bg_2000, bg_2010, bg_2020
## 
##       Assumptions about raw_root:
##            - State folders are two-letter USPS codes at the top level (e.g., "al", "az").
##            - Core-area ZIPs live at raw_root top level (not inside state folders).
## 
##    4. find_state_dirs: Find state directories containing required TIGER/Line 
##       ZIPs for the 2000, 2010, and 2020 decennial Census, for either blocks 
##       or block groups. Scans one directory level under `root` and keeps only 
##       those folders that contain all three expected ZIP patterns.
## 
##       For geography == "blocks":
##            - tl_2010_??_tabblock00.zip
##            - tl_2010_??_tabblock10.zip
##            - tl_2020_??_tabblock20.zip
## 
##       For geography == "block groups":
##            - tl_2010_??_bg00.zip
##            - tl_2010_??_bg10.zip
##            - tl_2020_??_bg20.zip
## 
##    5. find_state_block_zips: Locate the 2000/2010/2020 TIGER/Line ZIP files 
##       within a state directory, for either blocks or block groups. Identifies 
##       exactly one ZIP for each of the required patterns. Errors if any ZIP 
##       is missing or ambiguous.
## 
##    6. pad: Left-pad character IDs with zeros. Removes whitespace and pads 
##       values on the left with `"0"` to a fixed width. This is useful for 
##       Census FIPS/GEOID-style identifiers where leading zeros are meaningful 
##       and must be preserved.
## 
##    7. standardize_geo_layer: Standardizes a TIGER/Line layer (blocks or block 
##       groups) to consistent ID fields. Returns a standardized `sf` object with:
##            - `decennial_year` (2000/2010/2020)
##            - `geoid` (standardized GEOID: 15 chars for blocks, 12 chars for block groups)
##            - `statefp`, `countyfp`, `tractce` parsed from `geoid`
##            - plus `blockce` (blocks) or `blkgrpce` (block groups)
##            - `geometry`
#'
##       ID-field selection:
##            - Blocks:
##                - 2000: BLKIDFP00 (if present)
##                - 2010: GEOID10   (if present)
##                - 2020: GEOID20   (if present)
##                - fallback: GEOID (if present)
##            - Block groups:
##                - 2000: BKGPIDFP00 (if present)
##                - 2010: GEOID10    (if present)
##                - 2020: GEOID20    (if present)
##                - fallback: GEOID  (if present)
## 
##    8. write_state_gpkg: Writes a single state GeoPackage (`.gpkg`) containing 
##       layers for either blocks or block groups from the 2000, 2010, and 2020 
##       decennial vintages.
## 
##       Layers written:
##            - If `geography == "blocks"`:
##                - `blocks_2000`
##                - `blocks_2010`
##                - `blocks_2020`
##            - If `geography == "block groups"`:
##                - `bg_2000`
##                - `bg_2010`
##                - `bg_2020`
## 
##       Optionally writes to a temporary directory and then copies the 
##       completed GeoPackage into the final destination. This can be helpful on 
##       network drives or cloud-synced folders where writing SQLite databases 
##       in-place can be flaky.
## 
##    9. pick_field: Pick the first matching field name from a set of candidates. 
##       Utility helper for schema harmonization across vintages/files where 
##       the same concept (e.g., “code” or “name”) may appear under different 
##       column names. This function searches the available names in `nms` for 
##       the first match in `candidates`, preserving the priority order implied 
##       by `candidates`.
## 
##   10. standardize_core_area_layer: Standardize a national core-area layer to 
##       a common schema. Supported `area_type` values:
##            - "cbsa" (metropolitan/micropolitan core-based statistical areas)
##            - "csa"  (combined statistical areas)
##            - "zcta" (ZIP Code Tabulation Areas)
## 
##       Allowed `vintage_year` values by type:
##            - CBSA/CSA: 2007, 2010, 2020
##            - ZCTA:     2000, 2010, 2020
## 
##       Output schema:
##            - `vintage_year`
##            - `area_type`
##            - `area_code` (CBSAFP*/CSAFP*/ZCTA5CE*/GEOID* as available)
##            - `area_name` (CBSA/CSA only; from NAME*/NAMELSAD* when available)
##            - `area_states` (ZCTA only; hyphen-separated USPS codes like "CT-MA")
##            - `geometry`
## 
##   11. add_area_states: Annotate a CBSA/CSA `sf` (or data frame) with an 
##       `area_states` column parsed from the state abbreviations embedded in 
##       `area_name`.
##
##       Parsing logic: Census CBSA/CSA names embed state abbreviations 
##       immediately before a known suffix descriptor. The two observed formats 
##       are:
##            - Comma + space delimited: `"Providence-New Bedford, RI-MA Metropolitan Statistical Area"`
##            - Space delimited only:    `"Anchorage, AK Metro Area"`
## 
##       The regex extracts the state block sitting directly before one of the 
##       known Census suffix strings using a lookahead anchor, rather than 
##       stripping suffixes first. This is robust to suffix variants and avoids 
##       leaving fragments.
## 
##       Known suffixes detected:
##            - `Metropolitan Statistical Area`
##            - `Micropolitan Statistical Area`
##            - `Metropolitan Division`
##            - `Combined Statistical Area`
##            - `Metro Area` / `Micro Area`
##            - `CSA`
## 
##       Hyphen sentinel wrapping:** Extracted values are wrapped in leading and
##       trailing hyphens (e.g. `"RI-MA"` becomes `"-RI-MA-"`) so that downstream
##       sentinel matching in `decode_cbsa_csa()` and `decode_zcta()` is
##       unambiguous and cannot partially match a different state (e.g. `"-NJ-"`cannot match `"NJX"`).
## 
##   12. build_cbsa_csa: Build standardized CBSA + CSA layers for one vintage 
##       year, and annotate each with `area_states` parsed from `area_name`.
## 
##       What this function does
##            1) Reads national CBSA and CSA TIGER/Line ZIPs.
##            2) Standardizes each to a common schema via `standardize_core_area_layer()`.
##            3) For CBSAs only, adds `area_level` from the MEMI field (if present).
##            4) Transforms both layers to `target_crs`.
##            5) Parses a trailing state list (e.g., "CT-MA") from `area_name` 
##               into `area_states`.
## 
##       Messages (minimal):
##            - NOTE: build_cbsa_csa() starting for vintage_year=YYYY (yy=YY).
##            - OK: Transformed CBSA to target_crs.
##            - OK: Transformed CSA to target_crs.
## 
##   13. must_have: Validate that a named list contains required year keys.
## 
##   14. normalize_core: Normalize one year of core areas into a single combined 
##       `sf`. Accepts either:
##            - an `sf` already containing both CBSA and CSA rows, or
##            - a list(`cbsa`=`sf`, `csa`=`sf`) which will be row-bound into one `sf`.
## 
##   15. validate_zcta_sf: Validate that each ZCTA year is a non-NULL `sf`.
## 
##   16. build_zcta: Build standardized ZCTA layer for one vintage year 
##       (2000/2010/2020), including `area_states` (hyphen-separated USPS codes) 
##       when state boundaries are provided.
## 
##       Pairing rule (decennial):
##            - ZCTA 2000  (tl_2010_us_zcta500)  -> states from tl_2010_us_state00
##            - ZCTA 2010  (tl_2010_us_zcta510)  -> states from tl_2010_us_state10
##            - ZCTA 2020  (tl_2020_us_zcta520)  -> states from tl_2020_us_state20
## 
##       Supply either:
##            - `zip_state` (path to the matching state ZIP), OR
##            - `states_sf` (already-read state polygons)
## 
##       Messages (minimal; mirrors build_cbsa_csa)
##            - NOTE: build_zcta() starting for vintage_year=YYYY.
##            - OK: State USPS added. (only when `area_states` is present)
##            - OK: Transformed ZCTA to target_crs.
## 
##   17. write_core_areas_gpkg: Write a single GeoPackage containing core area 
##       (CBSA+CSA) and ZCTA layers by vintage. Writes layers:
##            - `cbsa_csa_2007`, `cbsa_csa_2010`, `cbsa_csa_2020`
##            - `zcta_2000`, `zcta_2010`, `zcta_2020`
## 
##       This writer is tolerant to two upstream shapes for `cbsa_csa[[year]]`:
##            1) a combined `sf` that already contains both CBSA and CSA rows, OR
##            2) a list with names `c("cbsa","csa")` where each element is an `sf`.
## 
##       If the split-list form is provided, the function binds CBSA and CSA 
##       together and (optionally) drops `area_level` from CSA to keep schemas 
##       consistent.
## 
##       ZCTA note (reflects updated build_zcta()):
##            - ZCTA layers may now include an `area_states` column 
##              (hyphen-separated USPS codes) when upstream `build_zcta()` was 
##              provided matching state boundaries.
##            - This writer accepts either schema:
##                  - legacy:  vintage_year, area_type, area_code, geometry
##                  - new:     vintage_year, area_type, area_code, area_states, geometry
##                  - If `area_states` is present, this function relocates it 
##                    immediately before `geometry` for consistent output ordering.
## 
##       Oxygen labels used in console messages:
##            - OK:      Requirement satisfied.
##            - MISSING: Expected field(s)/layer(s) absent or incomplete.
##            - ERROR:   An operation failed (e.g., cannot write layers).
##            - NOTE:    Informational message; not necessarily a failure.

## ----------------------------------------------------------------
## FUNCTIONS

read_zip_sf <- function(zip_path) {
  #' Read a TIGER/Line shapefile directly from a ZIP archive as an sf object. Uses 
  #' GDAL's `/vsizip/` virtual filesystem so the ZIP does not need to be manually 
  #' extracted.
  #'
  #' @param zip_path Path to a `.zip` file containing shapefile components.
  #'
  #' @return An `sf` object read from the zipped shapefile.
  
  sf::st_read(paste0("/vsizip/", zip_path), quiet = TRUE)
}




find_core_area_zips <- function(root) {
  #' Find national ZIP files (CBSA/CSA + ZCTA + States) under a root directory.
  #' Searches `root` for specific TIGER/Line (and 2007 “fe_” vintage) ZIP 
  #' filenames and returns their full paths. Intended for *national* files 
  #' (not per-state folders).
  #'
  #' Strict behavior: errors if any required ZIP is missing or ambiguous (multiple matches).
  #'
  #' @param root Directory containing the ZIP files.
  #'
  #' @return Named list with elements:
  #' - `zip_cbsa_2007`, `zip_csa_2007`
  #' - `zip_cbsa_2010`, `zip_csa_2010`
  #' - `zip_cbsa_2020`, `zip_csa_2020`
  #' - `zip_zcta_2000`, `zip_zcta_2010`, `zip_zcta_2020`
  #' - `zip_state_2000`, `zip_state_2010`, `zip_state_2020` (used to annotate ZCTAs)
  
  # List all ZIP files directly under root (non-recursive).
  # If ZIPs are nested in subfolders, switch to list.files(..., recursive = TRUE).
  zips <- list.files(root, pattern = "\\.zip$", full.names = TRUE)
  
  # Return exactly one ZIP matching a regex pattern; error otherwise.
  get1 <- function(pattern) {
    hit <- zips[stringr::str_detect(basename(zips), pattern)]
    if (length(hit) != 1) {
      stop("Missing/ambiguous ZIP for pattern: ", pattern, " in: ", root)
    }
    hit
  }
  
  list(
    # --- CBSA / CSA ---
    zip_cbsa_2007 = get1("^fe_2007_us_cbsa\\.zip$"),
    zip_csa_2007  = get1("^fe_2007_us_csa\\.zip$"),
    zip_cbsa_2010 = get1("^tl_2010_us_cbsa10\\.zip$"),
    zip_csa_2010  = get1("^tl_2010_us_csa10\\.zip$"),
    zip_cbsa_2020 = get1("^tl_2020_us_cbsa\\.zip$"),
    zip_csa_2020  = get1("^tl_2020_us_csa\\.zip$"),
    
    # --- ZCTA ---
    # (In your pipeline you’re treating these as the “2000/2010/2020” vintages)
    zip_zcta_2000 = get1("^tl_2010_us_zcta500\\.zip$"),
    zip_zcta_2010 = get1("^tl_2010_us_zcta510\\.zip$"),
    zip_zcta_2020 = get1("^tl_2020_us_zcta520\\.zip$"),
    
    # --- States (for annotating ZCTAs with state) ---
    # Map each ZCTA vintage to the corresponding state vintage you described.
    zip_state_2000 = get1("^tl_2010_us_state00\\.zip$"),
    zip_state_2010 = get1("^tl_2010_us_state10\\.zip$"),
    zip_state_2020 = get1("^tl_2020_us_state\\.zip$")
  )
}




preflight_tiger_outputs <- function(
    raw_root = "Data/Raw/Census Bureau TIGER Line Shapefiles",
    out_root = "Data/Results/Census Bureau TIGER Line Shapefiles",
    geography = c("blocks", "block groups")
) {
  #' Preflight check for TIGER inputs/outputs. This function is designed to be
  #' run *before* downstream TIGER processing. It validates (a) raw ZIP inputs
  #' exist and are readable and (b) expected output GeoPackages exist and
  #' contain the expected layers.
  #'
  #' Oxygen labels used in console messages:
  #' - OK:      Requirement satisfied.
  #' - MISSING: Expected file(s)/layer(s) absent or incomplete.
  #' - ERROR:   An operation failed (e.g., cannot read layers).
  #' - NOTE:    Informational message; not necessarily a failure.
  #'
  #' Checks performed
  #' 0) Core-areas raw ZIP availability:
  #'    - CBSA/CSA: 2007, 2010, 2020
  #'    - ZCTA:     2000, 2010, 2020
  #'    - STATES:   2000, 2010, 2020  (used to annotate ZCTA with state)
  #' 1) Core-areas output:
  #'    - core_areas.gpkg exists
  #'    - contains required layers (cbsa_csa_2007, ..., zcta_2020)
  #' 2) State raw inputs (for requested geography):
  #'    - Only state folders where at least one matching ZIP is detected are checked/reported
  #'    - For each detected state: verify ZIP presence for 2000/2010/2020 via filename patterns
  #'    - Lightweight ZIP "layer check": confirm GDAL can list a layer inside each present ZIP
  #' 3) State outputs (for requested geography):
  #'    - Output file exists for each detected state:
  #'      blocks_statefp_XX_2000_2010_2020.gpkg  OR  bg_statefp_XX_2000_2010_2020.gpkg
  #'    - Output contains expected layers:
  #'      blocks_2000, blocks_2010, blocks_2020  OR  bg_2000, bg_2010, bg_2020
  #'
  #' Assumptions about raw_root:
  #' - State folders are two-letter USPS codes at the top level (e.g., "al", "az").
  #' - Core-area ZIPs live at raw_root top level (not inside state folders).
  #'
  #' @param raw_root Root folder containing raw per-state TIGER/Line ZIPs + core-area ZIPs.
  #' @param out_root Root folder containing compiled outputs.
  #' @param geography One of "blocks" or "block groups".
  #'
  #' @return Invisibly returns a list including raw ZIP completeness and output status.
  
  # ---- Argument validation --------------------------------------------------
  geography <- match.arg(geography)
  
  # Normalize paths
  raw_root_abs <- normalizePath(raw_root, winslash = "/", mustWork = TRUE)
  out_root_abs <- normalizePath(out_root, winslash = "/", mustWork = FALSE)
  
  # --------------------------------------------------------------------------
  # [0] Core-areas raw ZIP availability (detect partial downloads)
  # --------------------------------------------------------------------------
  core_zip_paths <- list(
    cbsa  = setNames(vector("list", 3), c("2007", "2010", "2020")),
    csa   = setNames(vector("list", 3), c("2007", "2010", "2020")),
    zcta  = setNames(vector("list", 3), c("2000", "2010", "2020")),
    state = setNames(vector("list", 3), c("2000", "2010", "2020"))
  )
  core_zip_present <- list(
    cbsa  = setNames(rep(FALSE, 3), c("2007", "2010", "2020")),
    csa   = setNames(rep(FALSE, 3), c("2007", "2010", "2020")),
    zcta  = setNames(rep(FALSE, 3), c("2000", "2010", "2020")),
    state = setNames(rep(FALSE, 3), c("2000", "2010", "2020"))
  )
  
  # Attempt to locate core-area zips with a helper; if it errors we report MISSING.
  z_core <- tryCatch(find_core_area_zips(raw_root_abs), error = function(e) e)
  
  core_raw_ok <- FALSE
  missing_core_raw <- character(0)
  
  if (inherits(z_core, "error")) {
    message("MISSING: Could not locate required core-area ZIPs under raw_root.")
    message("Reason: ", conditionMessage(z_core))
    missing_core_raw <- c(
      "cbsa:  2007, 2010, 2020",
      "csa:   2007, 2010, 2020",
      "zcta:  2000, 2010, 2020",
      "state: 2000, 2010, 2020"
    )
  } else {
    # Record helper-returned paths.
    core_zip_paths$cbsa[["2007"]] <- z_core$zip_cbsa_2007
    core_zip_paths$cbsa[["2010"]] <- z_core$zip_cbsa_2010
    core_zip_paths$cbsa[["2020"]] <- z_core$zip_cbsa_2020
    
    core_zip_paths$csa[["2007"]]  <- z_core$zip_csa_2007
    core_zip_paths$csa[["2010"]]  <- z_core$zip_csa_2010
    core_zip_paths$csa[["2020"]]  <- z_core$zip_csa_2020
    
    core_zip_paths$zcta[["2000"]] <- z_core$zip_zcta_2000
    core_zip_paths$zcta[["2010"]] <- z_core$zip_zcta_2010
    core_zip_paths$zcta[["2020"]] <- z_core$zip_zcta_2020
    
    # NEW: state ZIPs used to annotate ZCTAs
    core_zip_paths$state[["2000"]] <- z_core$zip_state_2000
    core_zip_paths$state[["2010"]] <- z_core$zip_state_2010
    core_zip_paths$state[["2020"]] <- z_core$zip_state_2020
    
    # Validate existence for each expected ZIP path.
    core_zip_present$cbsa <- vapply(
      core_zip_paths$cbsa,
      function(p) is.character(p) && length(p) == 1 && file.exists(p),
      logical(1)
    )
    core_zip_present$csa <- vapply(
      core_zip_paths$csa,
      function(p) is.character(p) && length(p) == 1 && file.exists(p),
      logical(1)
    )
    core_zip_present$zcta <- vapply(
      core_zip_paths$zcta,
      function(p) is.character(p) && length(p) == 1 && file.exists(p),
      logical(1)
    )
    core_zip_present$state <- vapply(
      core_zip_paths$state,
      function(p) is.character(p) && length(p) == 1 && file.exists(p),
      logical(1)
    )
    
    # Build a human-readable missing list (grouped by product).
    if (any(!core_zip_present$cbsa)) {
      missing_core_raw <- c(
        missing_core_raw,
        paste0("cbsa: ", paste(names(core_zip_present$cbsa)[!core_zip_present$cbsa], collapse = ", "))
      )
    }
    if (any(!core_zip_present$csa)) {
      missing_core_raw <- c(
        missing_core_raw,
        paste0("csa: ", paste(names(core_zip_present$csa)[!core_zip_present$csa], collapse = ", "))
      )
    }
    if (any(!core_zip_present$zcta)) {
      missing_core_raw <- c(
        missing_core_raw,
        paste0("zcta: ", paste(names(core_zip_present$zcta)[!core_zip_present$zcta], collapse = ", "))
      )
    }
    if (any(!core_zip_present$state)) {
      missing_core_raw <- c(
        missing_core_raw,
        paste0("state: ", paste(names(core_zip_present$state)[!core_zip_present$state], collapse = ", "))
      )
    }
    
    core_raw_ok <- (length(missing_core_raw) == 0)
    
    if (core_raw_ok) {
      message("OK: Found all required core-area raw ZIPs (including states for ZCTA annotation).")
    } else {
      message("MISSING: Some core-area raw ZIPs are missing:")
      message("  - ", paste(missing_core_raw, collapse = "\n  - "))
    }
  }
  
  # --------------------------------------------------------------------------
  # [1] Core-areas output GPKG + expected layers
  # --------------------------------------------------------------------------
  core_areas_name <- "core_areas.gpkg"
  core_areas_gpkg <- file.path(out_root_abs, core_areas_name)
  
  required_layers <- c(
    "cbsa_csa_2007", "cbsa_csa_2010", "cbsa_csa_2020",
    "zcta_2000", "zcta_2010", "zcta_2020"
  )
  
  core_areas_exists <- file.exists(core_areas_gpkg)
  
  core_areas_layers <- character(0)
  missing_core_layers <- required_layers
  extra_core_layers <- character(0)
  core_areas_ok <- FALSE
  
  if (!core_areas_exists) {
    message("MISSING: core-areas GPKG not found: ", core_areas_name)
  } else {
    lyr <- tryCatch(sf::st_layers(core_areas_gpkg)$name, error = function(e) e)
    
    if (inherits(lyr, "error")) {
      message("ERROR: Could not read layers from ", core_areas_name, ": ", conditionMessage(lyr))
    } else {
      core_areas_layers <- as.character(lyr)
      missing_core_layers <- setdiff(required_layers, core_areas_layers)
      extra_core_layers <- setdiff(core_areas_layers, required_layers)
      core_areas_ok <- (length(missing_core_layers) == 0)
      
      if (core_areas_ok) {
        message("OK: Found core-areas GPKG with all expected layers.")
      } else {
        message("MISSING: core-areas GPKG is missing layer(s): ", paste(missing_core_layers, collapse = ", "))
      }
      
      if (length(extra_core_layers) > 0) {
        message("NOTE: core-areas GPKG contains extra layer(s): ", paste(extra_core_layers, collapse = ", "))
      }
    }
  }
  
  # Back-compat
  cbsa_csa_ok <- core_areas_ok
  
  # --------------------------------------------------------------------------
  # [2] State raw ZIPs + ZIP readability check (only for detected state folders)
  # --------------------------------------------------------------------------
  top_names <- list.files(raw_root_abs, full.names = FALSE, recursive = FALSE, include.dirs = TRUE)
  state_dir_names <- top_names[grepl("^[A-Za-z]{2}$", top_names)]
  state_dirs_all <- file.path(raw_root_abs, state_dir_names)
  names(state_dirs_all) <- tolower(state_dir_names)
  
  message("NOTE: Found ", length(state_dirs_all), " state folders under raw_root.")
  if (length(state_dirs_all) > 0) print(sort(names(state_dirs_all)))
  
  patterns <- switch(
    geography,
    "blocks" = c(
      zip2000 = "^tl_2010_\\d{2}_tabblock00\\.zip$",
      zip2010 = "^tl_2010_\\d{2}_tabblock10\\.zip$",
      zip2020 = "^tl_2020_\\d{2}_tabblock20\\.zip$"
    ),
    "block groups" = c(
      zip2000 = "^tl_2010_\\d{2}_bg00\\.zip$",
      zip2010 = "^tl_2010_\\d{2}_bg10\\.zip$",
      zip2020 = "^tl_2020_\\d{2}_bg\\.zip$"
    )
  )
  required_years_state <- c("2000", "2010", "2020")
  
  has_any_matching_zip <- function(sd, patterns) {
    b <- list.files(sd, pattern = "\\.zip$", full.names = FALSE, recursive = FALSE)
    if (length(b) == 0) return(FALSE)
    any(vapply(patterns, function(p) any(stringr::str_detect(b, p)), logical(1)))
  }
  
  detected_state_names <- names(state_dirs_all)[vapply(
    state_dirs_all, has_any_matching_zip, logical(1), patterns = patterns
  )]
  state_dirs_detected <- state_dirs_all[detected_state_names]
  
  message("NOTE: State folders with at least one matching ", geography, " ZIP: ", length(state_dirs_detected))
  if (length(state_dirs_detected) > 0) print(sort(names(state_dirs_detected)))
  
  zip_has_layer <- function(zip_path) {
    if (is.na(zip_path) || !is.character(zip_path) || length(zip_path) != 1 || !file.exists(zip_path)) return(FALSE)
    lyr <- tryCatch(sf::st_layers(paste0("/vsizip/", zip_path))$name, error = function(e) NULL)
    is.character(lyr) && length(lyr) >= 1
  }
  
  state_raw_zip_check <- lapply(state_dirs_detected, function(sd) {
    z <- list.files(sd, pattern = "\\.zip$", full.names = TRUE, recursive = FALSE)
    b <- basename(z)
    
    zip2000 <- z[stringr::str_detect(b, patterns[["zip2000"]])]
    zip2010 <- z[stringr::str_detect(b, patterns[["zip2010"]])]
    zip2020 <- z[stringr::str_detect(b, patterns[["zip2020"]])]
    
    present <- c(
      "2000" = length(zip2000) == 1,
      "2010" = length(zip2010) == 1,
      "2020" = length(zip2020) == 1
    )
    
    paths <- list(
      "2000" = if (length(zip2000) == 1) zip2000 else NA_character_,
      "2010" = if (length(zip2010) == 1) zip2010 else NA_character_,
      "2020" = if (length(zip2020) == 1) zip2020 else NA_character_
    )
    
    ambig <- c(
      "2000" = length(zip2000),
      "2010" = length(zip2010),
      "2020" = length(zip2020)
    )
    
    layer_ok <- c(
      "2000" = if (present[["2000"]]) zip_has_layer(paths[["2000"]]) else FALSE,
      "2010" = if (present[["2010"]]) zip_has_layer(paths[["2010"]]) else FALSE,
      "2020" = if (present[["2020"]]) zip_has_layer(paths[["2020"]]) else FALSE
    )
    
    list(
      state_dir        = sd,
      zip_paths        = paths,
      zip_present      = present,
      zip_layer_ok     = layer_ok,
      n_present        = as.integer(sum(present)),
      class            = if (all(present) && all(layer_ok)) "complete" else "partial",
      ambiguous_counts = ambig
    )
  })
  names(state_raw_zip_check) <- names(state_dirs_detected)
  
  state_dirs_complete <- names(state_raw_zip_check)[vapply(
    state_raw_zip_check, function(x) identical(x$class, "complete"), logical(1)
  )]
  state_dirs_partial <- names(state_raw_zip_check)[vapply(
    state_raw_zip_check, function(x) identical(x$class, "partial"), logical(1)
  )]
  
  raw_state_zip_ok <- (length(state_dirs_partial) == 0)
  
  missing_state_raw_by_year <- setNames(rep(0L, 3), required_years_state)
  for (yy in required_years_state) {
    missing_state_raw_by_year[[yy]] <- sum(vapply(
      state_raw_zip_check, function(x) !isTRUE(x$zip_present[[yy]]), logical(1)
    ))
  }
  
  layer_fail_by_year <- setNames(rep(0L, 3), required_years_state)
  for (yy in required_years_state) {
    layer_fail_by_year[[yy]] <- sum(vapply(
      state_raw_zip_check,
      function(x) isTRUE(x$zip_present[[yy]]) && !isTRUE(x$zip_layer_ok[[yy]]),
      logical(1)
    ))
  }
  
  if (length(state_raw_zip_check) == 0) {
    message("NOTE: No state folders contain any matching ZIPs for ", geography, " (nothing to check).")
  } else if (raw_state_zip_ok) {
    message("OK: Detected state folders have complete raw ", geography, " ZIPs for 2000/2010/2020 (and ZIPs are readable).")
  } else {
    message("MISSING: Some detected state folders have incomplete raw ", geography, " ZIPs and/or unreadable ZIP contents.")
    message(
      "Missing ZIP counts by year (among detected folders): ",
      paste0(names(missing_state_raw_by_year), "=", missing_state_raw_by_year, collapse = ", ")
    )
    message(
      "Unreadable ZIP counts by year (among detected folders, where ZIP is present): ",
      paste0(names(layer_fail_by_year), "=", layer_fail_by_year, collapse = ", ")
    )
    
    message("State folders with issues:")
    for (nm in sort(state_dirs_partial)) {
      x <- state_raw_zip_check[[nm]]
      missing_yrs <- names(x$zip_present)[!x$zip_present]
      present_yrs <- names(x$zip_present)[x$zip_present]
      unreadable_yrs <- names(x$zip_layer_ok)[x$zip_present & !x$zip_layer_ok]
      
      msg <- paste0("  - ", nm, " (have: ", paste(present_yrs, collapse = ", "))
      if (length(missing_yrs) > 0) msg <- paste0(msg, "; missing: ", paste(missing_yrs, collapse = ", "))
      if (length(unreadable_yrs) > 0) msg <- paste0(msg, "; unreadable: ", paste(unreadable_yrs, collapse = ", "))
      msg <- paste0(msg, ")")
      message(msg)
    }
  }
  
  # --------------------------------------------------------------------------
  # [3] State output GPKG files + expected layers (per state)
  # --------------------------------------------------------------------------
  prefix <- if (geography == "blocks") "blocks" else "bg"
  
  required_state_layers <- c(
    paste0(prefix, "_2000"),
    paste0(prefix, "_2010"),
    paste0(prefix, "_2020")
  )
  
  out_files <- list.files(
    out_root_abs,
    pattern = paste0("^", prefix, "_statefp_[0-9]{2}_2000_2010_2020\\.gpkg$"),
    full.names = TRUE
  )
  
  out_statefp <- sub(
    paste0("^", prefix, "_statefp_([0-9]{2})_2000_2010_2020\\.gpkg$"),
    "\\1",
    basename(out_files)
  )
  names(out_files) <- out_statefp
  out_statefp <- unique(out_statefp)
  
  expected_statefp <- vapply(names(state_dirs_detected), function(stnm) {
    chk <- state_raw_zip_check[[stnm]]$zip_paths
    
    try_year <- function(zip_path, year) {
      if (is.na(zip_path) || !is.character(zip_path) || length(zip_path) != 1 || !file.exists(zip_path)) return(NULL)
      x <- standardize_geo_layer(read_zip_sf(zip_path), year = year, geography = geography)
      st <- unique(x$statefp)
      if (length(st) == 1) st[1] else NULL
    }
    
    st <- try_year(chk[["2020"]], 2020)
    if (is.null(st)) st <- try_year(chk[["2010"]], 2010)
    if (is.null(st)) st <- try_year(chk[["2000"]], 2000)
    if (is.null(st)) NA_character_ else st
  }, character(1))
  expected_statefp <- unique(stats::na.omit(expected_statefp))
  
  missing_statefp <- setdiff(expected_statefp, out_statefp)
  extra_statefp <- setdiff(out_statefp, expected_statefp)
  
  state_output_layer_check <- setNames(vector("list", length(expected_statefp)), expected_statefp)
  output_layers_ok <- TRUE
  
  for (sfip in expected_statefp) {
    gpkg <- out_files[as.character(sfip)]
    gpkg <- if (length(gpkg) == 1) unname(gpkg) else NA_character_
    
    if (is.na(gpkg) || !file.exists(gpkg)) {
      state_output_layer_check[[sfip]] <- list(
        gpkg_exists = FALSE,
        layers = character(0),
        missing_layers = required_state_layers,
        extra_layers = character(0),
        ok = FALSE
      )
      output_layers_ok <- FALSE
      next
    }
    
    lyr <- tryCatch(sf::st_layers(gpkg)$name, error = function(e) e)
    if (inherits(lyr, "error")) {
      state_output_layer_check[[sfip]] <- list(
        gpkg_exists = TRUE,
        layers = character(0),
        missing_layers = required_state_layers,
        extra_layers = character(0),
        ok = FALSE,
        error = conditionMessage(lyr)
      )
      output_layers_ok <- FALSE
      next
    }
    
    layers <- as.character(lyr)
    missing_layers <- setdiff(required_state_layers, layers)
    extra_layers <- setdiff(layers, required_state_layers)
    ok <- (length(missing_layers) == 0)
    
    state_output_layer_check[[sfip]] <- list(
      gpkg_exists = TRUE,
      layers = layers,
      missing_layers = missing_layers,
      extra_layers = extra_layers,
      ok = ok
    )
    if (!ok) output_layers_ok <- FALSE
  }
  
  fips_to_usps <- c(
    "01"="AL","02"="AK","04"="AZ","05"="AR","06"="CA","08"="CO","09"="CT","10"="DE","11"="DC","12"="FL",
    "13"="GA","15"="HI","16"="ID","17"="IL","18"="IN","19"="IA","20"="KS","21"="KY","22"="LA","23"="ME",
    "24"="MD","25"="MA","26"="MI","27"="MN","28"="MS","29"="MO","30"="MT","31"="NE","32"="NV","33"="NH",
    "34"="NJ","35"="NM","36"="NY","37"="NC","38"="ND","39"="OH","40"="OK","41"="OR","42"="PA","44"="RI",
    "45"="SC","46"="SD","47"="TN","48"="TX","49"="UT","50"="VT","51"="VA","53"="WA","54"="WV","55"="WI",
    "56"="WY"
  )
  
  missing_usps <- unname(fips_to_usps[missing_statefp])
  missing_usps[is.na(missing_usps)] <- paste0("UNKNOWN(FIPS=", missing_statefp[is.na(missing_usps)], ")")
  
  extra_usps <- unname(fips_to_usps[extra_statefp])
  extra_usps[is.na(extra_usps)] <- paste0("UNKNOWN(FIPS=", extra_statefp[is.na(extra_usps)], ")")
  
  states_ok_files <- (length(missing_statefp) == 0)
  
  if (states_ok_files) {
    message("OK: State output GPKG files exist for all detected states.")
  } else {
    message(
      "MISSING: Output GPKG files for states: ",
      paste(sort(missing_usps), collapse = ", "),
      " (statefp: ", paste(sort(missing_statefp), collapse = ", "), ")",
      "\nExpected files like: ", prefix, "_statefp_XX_2000_2010_2020.gpkg"
    )
  }
  
  if (length(expected_statefp) == 0) {
    message("NOTE: No expected states to check output layers for.")
  } else if (output_layers_ok) {
    message("OK: State output GPKGs contain all expected layers: ", paste(required_state_layers, collapse = ", "))
  } else {
    bad <- expected_statefp[vapply(state_output_layer_check, function(x) isFALSE(x$ok), logical(1))]
    message("MISSING: Some state output GPKGs are missing expected layer(s).")
    
    for (sfip in bad) {
      usps <- unname(fips_to_usps[[sfip]])
      if (is.na(usps) || is.null(usps)) usps <- paste0("STATEFP_", sfip)
      
      x <- state_output_layer_check[[sfip]]
      if (!isTRUE(x$gpkg_exists)) {
        message("  - ", usps, " (statefp ", sfip, "): GPKG missing")
      } else if (!is.null(x$error)) {
        message("  - ", usps, " (statefp ", sfip, "): ERROR reading layers: ", x$error)
      } else {
        message("  - ", usps, " (statefp ", sfip, "): missing layers: ", paste(x$missing_layers, collapse = ", "))
      }
    }
  }
  
  if (length(extra_statefp) > 0) {
    message(
      "NOTE: Outputs exist for state(s) not present among detected state folders: ",
      paste(sort(extra_usps), collapse = ", "),
      " (statefp: ", paste(sort(extra_statefp), collapse = ", "), ")"
    )
  }
  
  states_ok <- states_ok_files && output_layers_ok
  
  # --------------------------------------------------------------------------
  # Return object (invisible): structured diagnostics for programmatic use
  # --------------------------------------------------------------------------
  invisible(list(
    geography = geography,
    
    # Core-areas raw inputs (now includes states zips for ZCTA annotation)
    core_raw_ok = core_raw_ok,
    core_zip_paths = core_zip_paths,
    core_zip_present = core_zip_present,
    missing_core_raw = missing_core_raw,
    
    # Core-areas outputs
    core_areas_gpkg = core_areas_gpkg,
    core_areas_exists = core_areas_exists,
    core_areas_layers = core_areas_layers,
    core_areas_ok = core_areas_ok,
    missing_core_layers = missing_core_layers,
    extra_core_layers = extra_core_layers,
    
    # Back-compat fields
    cbsa_csa_gpkg = core_areas_gpkg,
    cbsa_csa_ok = cbsa_csa_ok,
    
    # State raw inputs
    state_dirs_all = state_dirs_all,
    state_dirs_detected = state_dirs_detected,
    raw_state_zip_ok = raw_state_zip_ok,
    state_raw_zip_check = state_raw_zip_check,
    missing_state_raw_by_year = missing_state_raw_by_year,
    layer_fail_by_year = layer_fail_by_year,
    state_dirs_complete = state_dirs_complete,
    state_dirs_partial = state_dirs_partial,
    
    # State outputs
    required_state_layers = required_state_layers,
    expected_statefp = expected_statefp,
    out_statefp = out_statefp,
    missing_statefp = missing_statefp,
    extra_statefp = extra_statefp,
    state_output_layer_check = state_output_layer_check,
    states_ok = states_ok
  ))
}




find_state_dirs <- function(root, geography = c("blocks", "block groups")) {
  #' Find state directories containing required TIGER/Line ZIPs for the 2000,
  #' 2010, and 2020 decennial Census, for either blocks or block groups.
  #' Scans one directory level under `root` and keeps only those folders that
  #' contain all three expected ZIP patterns.
  #'
  #' For geography == "blocks":
  #' - tl_2010_??_tabblock00.zip
  #' - tl_2010_??_tabblock10.zip
  #' - tl_2020_??_tabblock20.zip
  #'
  #' For geography == "block groups":
  #' - tl_2010_??_bg00.zip
  #' - tl_2010_??_bg10.zip
  #' - tl_2020_??_bg20.zip
  #'
  #' @param root Root directory whose immediate subdirectories correspond to states.
  #' @param geography One of "blocks" or "block groups".
  #'
  #' @return A character vector of full paths to qualifying state directories.
  
  geography <- match.arg(geography)
  
  patterns <- switch(
    geography,
    "blocks" = c(
      "^tl_2010_\\d{2}_tabblock00\\.zip$",
      "^tl_2010_\\d{2}_tabblock10\\.zip$",
      "^tl_2020_\\d{2}_tabblock20\\.zip$"
    ),
    "block groups" = c(
      "^tl_2010_\\d{2}_bg00\\.zip$",
      "^tl_2010_\\d{2}_bg10\\.zip$",
      "^tl_2020_\\d{2}_bg\\.zip$"
    )
  )
  
  # List immediate subdirectories
  d <- list.dirs(root, recursive = FALSE, full.names = TRUE)
  d <- d[file.info(d)$isdir]
  
  # Keep dirs that contain all required zip patterns
  keep <- vapply(d, function(dd) {
    z <- list.files(dd, pattern = "\\.zip$", full.names = FALSE)
    all(vapply(patterns, function(p) any(stringr::str_detect(z, p)), logical(1)))
  }, logical(1))
  
  d[keep]
}




find_state_block_zips <- function(state_dir, geography = c("blocks", "block groups")) {
  #' Locate the 2000/2010/2020 TIGER/Line ZIP files within a state directory,
  #' for either blocks or block groups. Identifies exactly one ZIP for each of
  #' the required patterns. Errors if any ZIP is missing or ambiguous.
  #'
  #' @param state_dir Path to a state directory that contains TIGER/Line ZIP files.
  #' @param geography One of "blocks" or "block groups".
  #'
  #' @return A named list with elements `zip2000`, `zip2010`, `zip2020`
  #' containing full file paths.
  
  geography <- match.arg(geography)
  
  patterns <- switch(
    geography,
    "blocks" = c(
      zip2000 = "^tl_2010_\\d{2}_tabblock00\\.zip$",
      zip2010 = "^tl_2010_\\d{2}_tabblock10\\.zip$",
      zip2020 = "^tl_2020_\\d{2}_tabblock20\\.zip$"
    ),
    "block groups" = c(
      zip2000 = "^tl_2010_\\d{2}_bg00\\.zip$",
      zip2010 = "^tl_2010_\\d{2}_bg10\\.zip$",
      zip2020 = "^tl_2020_\\d{2}_bg\\.zip$"
    )
  )
  
  z <- list.files(state_dir, pattern = "\\.zip$", full.names = TRUE)
  b <- basename(z)
  
  zip2000 <- z[stringr::str_detect(b, patterns[["zip2000"]])]
  zip2010 <- z[stringr::str_detect(b, patterns[["zip2010"]])]
  zip2020 <- z[stringr::str_detect(b, patterns[["zip2020"]])]
  
  # Require exactly one match for each
  if (length(zip2000) != 1 || length(zip2010) != 1 || length(zip2020) != 1) {
    stop("Missing/ambiguous ", geography, " zips in: ", state_dir)
  }
  
  list(zip2000 = zip2000, zip2010 = zip2010, zip2020 = zip2020)
}




pad <- function(x, width) {
  #' Left-pad character IDs with zeros. Removes whitespace and pads values on the 
  #' left with `"0"` to a fixed width. This is useful for Census FIPS/GEOID-style 
  #' identifiers where leading zeros are meaningful and must be preserved.
  #'
  #' @param x Vector to pad (numeric or character). Whitespace is removed.
  #' @param width Integer target width (e.g., 15 for Census block GEOIDs).
  #'
  #' @return A character vector of length `length(x)` padded to `width`.
  
  # Remove whitespace and ensure character
  x <- gsub("\\s+", "", as.character(x))
  
  # Left-pad with zeros to fixed width
  stringr::str_pad(x, width = width, side = "left", pad = "0")
}




standardize_geo_layer <- function(sf_obj, year, geography = c("blocks", "block groups")) {
  #' Standardizes a TIGER/Line layer (blocks or block groups) to consistent ID fields.
  #' Returns a standardized `sf` object with:
  #' - `decennial_year` (2000/2010/2020)
  #' - `geoid` (standardized GEOID: 15 chars for blocks, 12 chars for block groups)
  #' - `statefp`, `countyfp`, `tractce` parsed from `geoid`
  #' - plus `blockce` (blocks) or `blkgrpce` (block groups)
  #' - `geometry`
  #'
  #' ID-field selection:
  #' - Blocks:
  #'   - 2000: BLKIDFP00 (if present)
  #'   - 2010: GEOID10   (if present)
  #'   - 2020: GEOID20   (if present)
  #'   - fallback: GEOID (if present)
  #' - Block groups:
  #'   - 2000: BKGPIDFP00 (if present)
  #'   - 2010: GEOID10    (if present)
  #'   - 2020: GEOID20    (if present)
  #'   - fallback: GEOID  (if present)
  #'
  #' @param sf_obj An `sf` object containing polygons and ID fields.
  #' @param year Integer decennial year (2000, 2010, or 2020).
  #' @param geography One of "blocks" or "block groups".
  #'
  #' @return An `sf` object with standardized columns and original geometry.
  
  geography <- match.arg(geography)
  
  id_field <- dplyr::case_when(
    geography == "blocks" &&
      year == 2000 && "BLKIDFP00" %in% names(sf_obj) ~ "BLKIDFP00",
    geography == "blocks" &&
      year == 2010 && "GEOID10"   %in% names(sf_obj) ~ "GEOID10",
    geography == "blocks" &&
      year == 2020 && "GEOID20"   %in% names(sf_obj) ~ "GEOID20",
    
    geography == "block groups" &&
      year == 2000 && "BKGPIDFP00" %in% names(sf_obj) ~ "BKGPIDFP00",
    geography == "block groups" &&
      year == 2010 && "GEOID10"    %in% names(sf_obj) ~ "GEOID10",
    geography == "block groups" &&
      year == 2020 && "GEOID20"    %in% names(sf_obj) ~ "GEOID20",
    
    "GEOID" %in% names(sf_obj) ~ "GEOID",
    TRUE ~ NA_character_
  )
  if (is.na(id_field)) stop("No suitable GEOID field found for ", geography, " in year ", year)
  
  # GEOID length differs by geography
  geoid_len <- if (geography == "blocks") 15 else 12
  
  geoid <- pad(sf_obj[[id_field]], geoid_len)
  
  out <- sf_obj |>
    dplyr::transmute(
      decennial_year = as.integer(year),
      geoid = geoid,
      statefp  = substr(geoid, 1, 2),
      countyfp = substr(geoid, 3, 5),
      tractce  = substr(geoid, 6, 11),
      geometry = sf::st_geometry(sf_obj)
    )
  
  if (geography == "blocks") {
    out <- out |>
      dplyr::mutate(blockce = substr(geoid, 12, 15)) |>
      dplyr::relocate(blockce, .after = tractce)
  } else {
    out <- out |>
      dplyr::mutate(blkgrpce = substr(geoid, 12, 12)) |>
      dplyr::relocate(blkgrpce, .after = tractce)
  }
  
  out
}




write_state_gpkg <- function(x00, x10, x20, final_path,
                             geography = c("blocks", "block groups"),
                             use_tmp_then_copy = TRUE) {
  #' Writes a single state GeoPackage (`.gpkg`) containing layers for either
  #' blocks or block groups from the 2000, 2010, and 2020 decennial vintages.
  #'
  #' Layers written:
  #' - If `geography == "blocks"`:
  #'   - `blocks_2000`
  #'   - `blocks_2010`
  #'   - `blocks_2020`
  #' - If `geography == "block groups"`:
  #'   - `bg_2000`
  #'   - `bg_2010`
  #'   - `bg_2020`
  #'
  #' Optionally writes to a temporary directory and then copies the completed
  #' GeoPackage into the final destination. This can be helpful on network drives
  #' or cloud-synced folders where writing SQLite databases in-place can be flaky.
  #'
  #' @param x00 `sf` object of standardized geography for year 2000.
  #' @param x10 `sf` object of standardized geography for year 2010.
  #' @param x20 `sf` object of standardized geography for year 2020.
  #' @param final_path Output `.gpkg` path.
  #' @param geography One of "blocks" or "block groups".
  #' @param use_tmp_then_copy Logical; if `TRUE`, write to tempdir() first then copy.
  #'
  #' @return `TRUE` invisibly on success; errors on failure.
  
  geography <- match.arg(geography)
  
  # Choose layer names based on requested geography
  layer_prefix <- if (geography == "blocks") "blocks" else "bg"
  layers <- paste0(layer_prefix, "_", c("2000", "2010", "2020"))
  
  # Choose a stable write directory
  write_dir <- if (use_tmp_then_copy) {
    file.path(tempdir(), "tiger_gpkg_tmp")
  } else {
    dirname(final_path)
  }
  dir.create(write_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Where we actually write the SQLite DB
  tmp_path <- if (use_tmp_then_copy) file.path(write_dir, basename(final_path)) else final_path
  
  # Remove pre-existing outputs to avoid layer append/locking surprises
  if (file.exists(tmp_path)) file.remove(tmp_path)
  if (file.exists(final_path) && !identical(final_path, tmp_path)) file.remove(final_path)
  
  # Write all layers; only succeed if all writes succeed
  ok <- tryCatch({
    sf::st_write(x00, tmp_path, layer = layers[[1]], quiet = TRUE)
    sf::st_write(x10, tmp_path, layer = layers[[2]], quiet = TRUE)
    sf::st_write(x20, tmp_path, layer = layers[[3]], quiet = TRUE)
    TRUE
  }, error = function(e) e)
  
  if (!isTRUE(ok)) {
    stop("st_write failed for ", basename(final_path), ": ", conditionMessage(ok))
  }
  
  # Copy finished DB into the desired output folder
  if (use_tmp_then_copy) {
    ok2 <- file.copy(tmp_path, final_path, overwrite = TRUE)
    if (!ok2) stop("Failed to copy ", tmp_path, " to ", final_path)
    file.remove(tmp_path)
  }
  
  invisible(TRUE)
}




pick_field <- function(nms, candidates) {
  #' Pick the first matching field name from a set of candidates. Utility helper 
  #' for schema harmonization across vintages/files where the same concept (e.g., 
  #' “code” or “name”) may appear under different column names. This function 
  #' searches the available names in `nms` for the first match in `candidates`, 
  #' preserving the priority order implied by `candidates`.
  #'
  #' @param nms Character vector of available field/column names (e.g., `names(sf_obj)`).
  #' @param candidates Character vector of candidate field names in priority order.
  #'
  #' @return A single character string naming the selected field, or `NA_character_`
  #' if no candidate is present in `nms`.
  
  # Identify all candidates that are present in the provided names
  hit <- intersect(candidates, nms)
  
  # Return the first match (highest-priority), or NA if none found
  if (length(hit) == 0) NA_character_ else hit[1]
}




standardize_core_area_layer <- function(sf_obj,
                                        vintage_year,
                                        area_type,
                                        states_sf = NULL) {
  #' Standardize a national core-area layer to a common schema. Supported
  #' `area_type` values:
  #' - "cbsa" (metropolitan/micropolitan core-based statistical areas)
  #' - "csa"  (combined statistical areas)
  #' - "zcta" (ZIP Code Tabulation Areas)
  #'
  #' Allowed `vintage_year` values by type:
  #' - CBSA/CSA: 2007, 2010, 2020
  #' - ZCTA:     2000, 2010, 2020
  #'
  #' Output schema:
  #' - `vintage_year`
  #' - `area_type`
  #' - `area_code` (CBSAFP*/CSAFP*/ZCTA5CE*/GEOID* as available)
  #' - `area_name` (CBSA/CSA only; from NAME*/NAMELSAD* when available)
  #' - `area_states` (ZCTA only; hyphen-separated USPS codes like "CT-MA")
  #' - `geometry`
  #'
  #' @param sf_obj An `sf` object read from a TIGER/Line ZIP.
  #' @param vintage_year Integer vintage label (CBSA/CSA: 2007/2010/2020; ZCTA: 2000/2010/2020).
  #' @param area_type One of "cbsa", "csa", "zcta".
  #' @param states_sf Optional `sf` state polygons for the corresponding decennial
  #'   (e.g., tl_2010_us_state00 for ZCTA 2000; tl_2010_us_state10 for ZCTA 2010;
  #'   tl_2020_us_state20 for ZCTA 2020). If NULL, ZCTAs are returned without `area_states`.
  #'
  #' @return An `sf` object with standardized columns and geometry.
  
  # ---- Validate inputs up front ----
  stopifnot(area_type %in% c("cbsa", "csa", "zcta"))
  vintage_year <- as.integer(vintage_year)
  
  allowed_years <- switch(
    area_type,
    cbsa = c(2007L, 2010L, 2020L),
    csa  = c(2007L, 2010L, 2020L),
    zcta = c(2000L, 2010L, 2020L)
  )
  if (!vintage_year %in% allowed_years) {
    stop(
      "Unsupported vintage_year=", vintage_year,
      " for area_type=", area_type,
      ". Allowed: ", paste(allowed_years, collapse = ", ")
    )
  }
  
  # Cache column names for field discovery
  nms <- names(sf_obj)
  
  # Choose the best identifier field for the requested area type
  code_field <- switch(
    area_type,
    cbsa = pick_field(nms, c(
      "CBSAFP00", "CBSAFP10", "CBSAFP20", "CBSAFP",
      "GEOID00",  "GEOID10",  "GEOID20",  "GEOID"
    )),
    csa  = pick_field(nms, c(
      "CSAFP00", "CSAFP10", "CSAFP20", "CSAFP",
      "GEOID00", "GEOID10", "GEOID20", "GEOID"
    )),
    zcta = pick_field(nms, c(
      "ZCTA5CE00", "ZCTA5CE10", "ZCTA5CE20",
      "GEOID00",   "GEOID10",   "GEOID20", "GEOID"
    ))
  )
  
  if (is.na(code_field)) {
    stop("No identifier field found for area_type=", area_type, " vintage_year=", vintage_year)
  }
  
  # ---- ZCTA branch: optionally annotate with states ----
  if (area_type == "zcta") {
    
    # Base standardized ZCTA schema
    out <- sf_obj |>
      dplyr::transmute(
        vintage_year = vintage_year,
        area_type    = as.character(area_type),
        area_code    = as.character(.data[[code_field]]),
        geometry     = sf::st_geometry(sf_obj)
      )
    
    # If no states provided, return as-is (backwards-compatible)
    if (is.null(states_sf)) stop("compatible `states` shapefile must be provided.")
    
    if (!inherits(states_sf, "sf")) stop("`states_sf` must be an sf object when provided.")
    
    # Pick the best USPS field available in states_sf
    st_nms <- names(states_sf)
    stusps_field <- pick_field(st_nms, c("STUSPS00", "STUSPS10", "STUSPS20", "STUSPS"))
    if (is.na(stusps_field)) stop("Could not find a STUSPS field in `states_sf` (expected STUSPS00/10/20 or STUSPS).")
    
    # Align CRS for spatial predicates
    states_x <- sf::st_transform(states_sf, sf::st_crs(out))
    
    # Use intersects on polygons to capture multi-state ZCTAs
    ix <- sf::st_intersects(out, states_x, sparse = TRUE)
    
    stusps_vec <- as.character(states_x[[stusps_field]])
    
    # Collapse possibly multiple states into a single hyphen-separated string
    area_states <- vapply(ix, function(idx) {
      if (length(idx) == 0) return(NA_character_)
      paste(sort(unique(stusps_vec[idx])), collapse = "-")
    }, character(1))
    
    out$area_states <- area_states
    out <- out %>% relocate(area_states, .before = geometry)
    out
  } else {
    # ---- CBSA/CSA branch (unchanged) ----
    name_field <- pick_field(nms, c(
      "NAMELSAD", "NAMELSAD10", "NAMELSAD20",
      "NAME", "NAME10", "NAME20"
    ))
    if (is.na(name_field)) name_field <- code_field
    
    sf_obj |>
      dplyr::transmute(
        vintage_year = vintage_year,
        area_type    = as.character(area_type),
        area_code    = as.character(.data[[code_field]]),
        area_name    = as.character(.data[[name_field]]),
        geometry     = sf::st_geometry(sf_obj)
      )
  }
}




add_area_states <- function(x) {
  #' Annotate a CBSA/CSA `sf` (or data frame) with an `area_states` column parsed
  #' from the state abbreviations embedded in `area_name`.
  #'
  #' @param x An object with an `area_name` column (typically output of
  #'   `standardize_core_area_layer()` for CBSA/CSA).
  #'
  #' @return `x` with `area_states` added (hyphen-wrapped, e.g. `"-NY-NJ-PA-"`)
  #'   and all temporary helper columns removed.
  #'
  #' @details
  #' **Parsing logic:**
  #' Census CBSA/CSA names embed state abbreviations immediately before a known
  #' suffix descriptor. The two observed formats are:
  #'
  #' \itemize{
  #'   \item Comma + space delimited: `"Providence-New Bedford, RI-MA Metropolitan Statistical Area"`
  #'   \item Space delimited only:    `"Anchorage, AK Metro Area"`
  #' }
  #'
  #' The regex extracts the state block sitting directly before one of the known
  #' Census suffix strings using a lookahead anchor, rather than stripping
  #' suffixes first. This is robust to suffix variants and avoids leaving
  #' fragments.
  #'
  #' **Known suffixes detected:**
  #' \itemize{
  #'   \item `Metropolitan Statistical Area`
  #'   \item `Micropolitan Statistical Area`
  #'   \item `Metropolitan Division`
  #'   \item `Combined Statistical Area`
  #'   \item `Metro Area` / `Micro Area`
  #'   \item `CSA`
  #' }
  #'
  #' **Hyphen sentinel wrapping:** Extracted values are wrapped in leading and
  #' trailing hyphens (e.g. `"RI-MA"` becomes `"-RI-MA-"`) so that downstream
  #' sentinel matching in `decode_cbsa_csa()` and `decode_zcta()` is
  #' unambiguous and cannot partially match a different state (e.g. `"-NJ-"`
  #' cannot match `"NJX"`).
  
  # ---- Preconditions -------------------------------------------------------
  if (!("area_name" %in% names(x))) {
    message("MISSING: add_area_states(): required column `area_name` not found.")
    stop("add_area_states(): required column `area_name` not found.")
  }
  if (!is.character(x$area_name)) {
    message("NOTE: add_area_states(): coercing `area_name` to character for parsing.")
  } else {
    message("OK: add_area_states(): found `area_name` (character).")
  }
  
  # ---- Suffix lookahead pattern --------------------------------------------
  # Anchors the state extraction to the block immediately before any known
  # Census area-type descriptor. Order matters: longer/more-specific strings
  # must appear before shorter ones to prevent partial matches (e.g.
  # "Metropolitan Statistical Area" before "Metro Area").
  suffix_pattern <- paste0(
    "(?=\\s+(?:",
    "Metropolitan Statistical Area|",
    "Micropolitan Statistical Area|",
    "Metropolitan Division|",
    "Combined Statistical Area|",
    "Metro Area|",
    "Micro Area|",
    "CSA",
    ")\\s*$)"
  )
  
  # ---- Parse + annotate ----------------------------------------------------
  out <- x |>
    dplyr::mutate(
      
      # Extract the state block (e.g. "HI", "RI-MA", "PA-NJ-DE-MD") sitting
      # immediately before the suffix. The lookbehind accepts either ", " or " "
      # as the separator between the place name and the state block.
      # Examples:
      #   "Anchorage, AK Metro Area"                           -> "AK"
      #   "Clarksdale, MS Micro Area"                          -> "MS"
      #   "Dallas-Fort Worth-Arlington, TX Metro Area"         -> "TX"
      #   "Kahului-Wailuku, HI Micropolitan Statistical Area"  -> "HI"
      #   "Providence-New Bedford, RI-MA Metro..."             -> "RI-MA"
      #   "Philadelphia-Camden, PA-NJ-DE-MD Metro..."          -> "PA-NJ-DE-MD"
      #   "Vineland-Millville-Bridgeton, NJ Metro Area"        -> "NJ"
      area_states_raw = stringr::str_extract(
        as.character(.data$area_name),
        paste0("(?<=,\\s|\\s)([A-Z]{2}(?:-[A-Z]{2})*)", suffix_pattern)
      ),
      
      # Wrap in hyphen sentinels so individual state lookups cannot partially
      # match a different embedded state string downstream.
      area_states = dplyr::if_else(
        !is.na(.data$area_states_raw),
        .data$area_states_raw,
        NA_character_
      )
    ) |>
    
    # Remove parse helper — only area_states is needed downstream
    dplyr::select(-dplyr::any_of("area_states_raw"))
  
  # ---- Post-checks ---------------------------------------------------------
  n_nonmissing <- sum(!is.na(out$area_states))
  n_total      <- nrow(out)
  
  if (n_nonmissing == 0L) {
    message("NOTE: add_area_states(): all `area_states` values are NA — ",
            "name format may not match any known Census suffix pattern. ",
            "Check a sample with: head(x$area_name, 20)")
  } else if (n_nonmissing < n_total) {
    message("OK: add_area_states(): resolved `area_states` for ", n_nonmissing,
            " of ", n_total, " row(s). ",
            n_total - n_nonmissing,
            " row(s) remain NA — check for unusual name formats.")
  } else {
    message("OK: add_area_states(): resolved `area_states` for all ",
            n_nonmissing, " row(s).")
  }
  
  out
}





build_cbsa_csa <- function(zip_cbsa, zip_csa, vintage_year, target_crs) {
  
  #' Build standardized CBSA + CSA layers for one vintage year, and annotate each
  #' with `area_states` parsed from `area_name`.
  #' 
  #' What this function does
  #' 1) Reads national CBSA and CSA TIGER/Line ZIPs.
  #' 2) Standardizes each to a common schema via `standardize_core_area_layer()`.
  #' 3) For CBSAs only, adds `area_level` from the MEMI field (if present).
  #' 4) Transforms both layers to `target_crs`.
  #' 5) Parses a trailing state list (e.g., "CT-MA") from `area_name` into `area_states`.
  #' 
  #' Messages (minimal):
  #' - NOTE: build_cbsa_csa() starting for vintage_year=YYYY (yy=YY).
  #' - OK: Transformed CBSA to target_crs.
  #' - OK: Transformed CSA to target_crs.
  #'
  #' @param zip_cbsa Path to CBSA ZIP file (national).
  #' @param zip_csa Path to CSA ZIP file (national).
  #' @param vintage_year One of 2007, 2010, 2020.
  #' @param target_crs CRS for output (e.g., 4326).
  #'
  #' @return Named list: list(cbsa = <sf>, csa = <sf>)
  
  # ---- [0] Validate arguments ----------------------------------------------
  vintage_year <- as.integer(vintage_year)
  if (!vintage_year %in% c(2007L, 2010L, 2020L)) {
    stop("build_cbsa_csa(): vintage_year must be one of 2007, 2010, 2020")
  }
  yy <- substr(as.character(vintage_year), 3, 4)
  
  message("NOTE: build_cbsa_csa() starting for vintage_year=", vintage_year, " (yy=", yy, ").")
  
  # ---- [1] Read raw TIGER/Line inputs --------------------------------------
  cbsa_raw <- read_zip_sf(zip_cbsa)
  csa_raw  <- read_zip_sf(zip_csa)
  
  # ---- [2] Standardize to common schema ------------------------------------
  cbsa <- standardize_core_area_layer(cbsa_raw, vintage_year = vintage_year, area_type = "cbsa")
  csa  <- standardize_core_area_layer(csa_raw,  vintage_year = vintage_year, area_type = "csa")
  
  # ---- [3] CBSA-only: build area_code -> area_level (MEMI) lookup ----------
  cbsa_id_col <- pick_field(
    names(cbsa_raw),
    c(paste0("CBSAFP", yy), "CBSAFP", paste0("GEOID", yy), "GEOID", "CBSA", "CBSA_CODE")
  )
  if (is.na(cbsa_id_col)) {
    stop("Could not find a CBSA id column (tried CBSAFP##/CBSAFP/GEOID##/GEOID/CBSA/CBSA_CODE).")
  }
  
  memi_col <- pick_field(names(cbsa_raw), c(paste0("MEMI", yy), "MEMI"))
  memi_col <- if (is.na(memi_col)) NULL else memi_col
  
  cbsa_level_lu <- sf::st_drop_geometry(cbsa_raw) |>
    dplyr::transmute(
      area_code_raw = as.character(.data[[cbsa_id_col]]),
      area_level    = if (!is.null(memi_col)) as.character(.data[[memi_col]]) else NA_character_
    ) |>
    dplyr::distinct(area_code_raw, .keep_all = TRUE)
  
  # ---- [4] Transform CRS (minimal messaging) -------------------------------
  cbsa <- sf::st_transform(cbsa, target_crs)
  message("OK: Transformed CBSA to target_crs.")
  
  csa <- sf::st_transform(csa, target_crs)
  message("OK: Transformed CSA to target_crs.")
  
  # ---- [5] Finalize CBSA (adds area_level + area_states) -------------------
  cbsa <- cbsa |>
    dplyr::left_join(cbsa_level_lu, by = c("area_code" = "area_code_raw")) |>
    add_area_states() |>
    dplyr::select(-dplyr::any_of("area_code_raw")) |>
    dplyr::relocate(area_level, .after = area_code) |>
    dplyr::relocate(area_states, .after = area_name)
  
  # ---- [6] Finalize CSA (adds area_states; ensures NO area_level) ----------
  csa <- csa |>
    add_area_states() |>
    dplyr::select(-dplyr::any_of("area_level")) |>
    dplyr::relocate(area_states, .after = area_name)
  
  list(cbsa = cbsa, csa = csa)
}




must_have <- function(x, nm, yrs) {
  #' Validate that a named list contains required year keys.
  #'
  #' @param x Object to validate.
  #' @param nm Name used in error messages.
  #' @param yrs Integer vector of required years.
  #'
  #' @return Invisibly `TRUE` if valid; errors otherwise.
  
  if (!is.list(x)) stop(nm, " must be a list.")
  if (is.null(names(x)) || any(!nzchar(names(x)))) stop(nm, " must be a *named* list.")
  names(x) <- trimws(names(x))
  miss <- setdiff(as.character(yrs), names(x))
  if (length(miss) > 0) {
    stop(nm, " is missing year(s): ", paste(miss, collapse = ", "),
         ". Present names: ", paste(names(x), collapse = ", "))
  }
  invisible(TRUE)
}




normalize_core <- function(x, yr) {
  #' Normalize one year of core areas into a single combined `sf`. Accepts either:
  #' - an `sf` already containing both CBSA and CSA rows, or
  #' - a list(`cbsa`=`sf`, `csa`=`sf`) which will be row-bound into one `sf`.
  #'
  #' @param x One element from `cbsa_csa[[year]]`.
  #' @param yr Year label (character) for error messages.
  #'
  #' @return Combined `sf` suitable for writing as `cbsa_csa_<year>`.
  
  # Already combined: nothing to do
  if (inherits(x, "sf")) return(x)
  
  # Split form: bind cbsa + csa into one sf
  if (is.list(x) && all(c("cbsa", "csa") %in% names(x))) {
    cbsa <- x[["cbsa"]]
    csa  <- x[["csa"]]
    
    if (!inherits(cbsa, "sf")) {
      stop("cbsa_csa[['", yr, "']]$cbsa must be sf; got: ", paste(class(cbsa), collapse = "/"))
    }
    if (!inherits(csa, "sf")) {
      stop("cbsa_csa[['", yr, "']]$csa must be sf; got: ", paste(class(csa), collapse = "/"))
    }
    
    # Keep CBSA as-is; drop `area_level` from CSA if present (schema hygiene)
    csa <- dplyr::select(csa, -dplyr::any_of("area_level"))
    
    return(dplyr::bind_rows(cbsa, csa))
  }
  
  stop(
    "cbsa_csa[['", yr, "']] must be an sf, or a list with names c('cbsa','csa'). ",
    "Got class: ", paste(class(x), collapse = "/"),
    "; names: ", paste(names(x), collapse = ", ")
  )
}




validate_zcta_sf <- function(z, yrs = c("2000","2010","2020")) {
  #' Validate that each ZCTA year is a non-NULL `sf`.
  #'
  #' @param z Named list of ZCTA layers.
  #' @param yrs Character vector of required year names.
  #'
  #' @return Invisibly `TRUE` if valid; errors otherwise.
  
  for (yr in yrs) {
    obj <- z[[yr]]
    if (is.null(obj)) stop("zcta[['", yr, "']] is NULL.")
    if (!inherits(obj, "sf")) stop("zcta[['", yr, "']] must be sf; got: ", paste(class(obj), collapse = "/"))
  }
  invisible(TRUE)
}




build_zcta <- function(zip_zcta,
                       vintage_year,
                       target_crs,
                       zip_state = NULL,
                       states_sf = NULL) {
  #' Build standardized ZCTA layer for one vintage year (2000/2010/2020),
  #' including `area_states` (hyphen-separated USPS codes) when state boundaries
  #' are provided.
  #'
  #' Pairing rule (decennial):
  #' - ZCTA 2000  (tl_2010_us_zcta500)  -> states from tl_2010_us_state00
  #' - ZCTA 2010  (tl_2010_us_zcta510)  -> states from tl_2010_us_state10
  #' - ZCTA 2020  (tl_2020_us_zcta520)  -> states from tl_2020_us_state20
  #'
  #' Supply either:
  #' - `zip_state` (path to the matching state ZIP), OR
  #' - `states_sf` (already-read state polygons)
  #'
  #' Messages (minimal; mirrors build_cbsa_csa)
  #' - NOTE: build_zcta() starting for vintage_year=YYYY.
  #' - OK: State USPS added. (only when `area_states` is present)
  #' - OK: Transformed ZCTA to target_crs.
  #'
  #' @param zip_zcta Path to ZCTA ZIP file.
  #' @param vintage_year One of 2000, 2010, 2020.
  #' @param target_crs CRS for output (e.g., 4326).
  #' @param zip_state Optional path to matching state ZIP file for this vintage.
  #' @param states_sf Optional sf of state polygons (alternative to zip_state).
  #'
  #' @return `sf` with ZCTA rows and columns:
  #'   vintage_year, area_type, area_code, area_states (if available), geometry.
  
  vintage_year <- as.integer(vintage_year)
  if (!vintage_year %in% c(2000L, 2010L, 2020L)) {
    stop("build_zcta(): vintage_year must be one of 2000, 2010, 2020")
  }
  
  if (!is.null(zip_state) && !is.null(states_sf)) {
    stop("build_zcta(): provide only one of `zip_state` or `states_sf` (not both).")
  }
  
  message("NOTE: build_zcta() starting for vintage_year=", vintage_year, ".")
  
  # ---- Read inputs ----------------------------------------------------------
  zcta_raw <- read_zip_sf(zip_zcta)
  
  # If states are provided by ZIP, read them
  if (!is.null(zip_state)) {
    states_sf <- read_zip_sf(zip_state)
  }
  
  # ---- Standardize + annotate ----------------------------------------------
  zcta_std <- standardize_core_area_layer(
    zcta_raw,
    vintage_year = vintage_year,
    area_type = "zcta",
    states_sf = states_sf
  )
  
  # Emit a concise success message only when we actually have USPS state labels
  if ("area_states" %in% names(zcta_std) && any(!is.na(zcta_std$area_states))) {
    message("OK: State USPS added.")
  }
  
  # ---- Transform CRS (minimal messaging) -----------------------------------
  zcta_out <- sf::st_transform(zcta_std, target_crs)
  message("OK: Transformed ZCTA to target_crs.")
  
  zcta_out
}




write_core_areas_gpkg <- function(cbsa_csa, zcta, final_path, use_tmp_then_copy = TRUE) {
  #' Write a single GeoPackage containing core area (CBSA+CSA) and ZCTA layers by
  #' vintage. Writes layers:
  #' - `cbsa_csa_2007`, `cbsa_csa_2010`, `cbsa_csa_2020`
  #' - `zcta_2000`, `zcta_2010`, `zcta_2020`
  #'
  #' This writer is tolerant to two upstream shapes for `cbsa_csa[[year]]`:
  #' 1) a combined `sf` that already contains both CBSA and CSA rows, OR
  #' 2) a list with names `c("cbsa","csa")` where each element is an `sf`.
  #'
  #' If the split-list form is provided, the function binds CBSA and CSA together
  #' and (optionally) drops `area_level` from CSA to keep schemas consistent.
  #'
  #' ZCTA note (reflects updated build_zcta()):
  #' - ZCTA layers may now include an `area_states` column (hyphen-separated USPS
  #'   codes) when upstream `build_zcta()` was provided matching state boundaries.
  #' - This writer accepts either schema:
  #'   - legacy:  vintage_year, area_type, area_code, geometry
  #'   - new:     vintage_year, area_type, area_code, area_states, geometry
  #' - If `area_states` is present, this function relocates it immediately before
  #'   `geometry` for consistent output ordering.
  #'
  #' Oxygen labels used in console messages:
  #' - OK:      Requirement satisfied.
  #' - MISSING: Expected field(s)/layer(s) absent or incomplete.
  #' - ERROR:   An operation failed (e.g., cannot write layers).
  #' - NOTE:    Informational message; not necessarily a failure.
  #'
  #' @param cbsa_csa Named list keyed by "2007","2010","2020". Each element is either
  #'   a combined `sf` (CBSA+CSA) or a list(`cbsa`=`sf`, `csa`=`sf`).
  #' @param zcta Named list keyed by "2000","2010","2020" of `sf` objects.
  #'   Each ZCTA `sf` may optionally include `area_states`.
  #' @param final_path Output `.gpkg` path.
  #' @param use_tmp_then_copy Logical; if `TRUE`, write to a temp file then copy into place.
  #'
  #' @return `TRUE` on success; errors otherwise.
  
  must_have(cbsa_csa, "cbsa_csa", c(2007, 2010, 2020))
  must_have(zcta,     "zcta",     c(2000, 2010, 2020))
  
  # validate_zcta_sf() in your codebase likely enforced the legacy schema.
  # We keep it, but also allow the new optional `area_states` column by
  # validating required columns here (and only *optionally* calling the old validator).
  required_zcta_cols <- c("vintage_year", "area_type", "area_code", "geometry")
  for (yr in c("2000", "2010", "2020")) {
    if (!inherits(zcta[[yr]], "sf")) stop("zcta[[", yr, "]] is not an sf object.")
    missing_cols <- setdiff(required_zcta_cols, names(zcta[[yr]]))
    if (length(missing_cols) > 0) {
      message("MISSING: zcta_", yr, " is missing required column(s): ", paste(missing_cols, collapse = ", "))
      stop("write_core_areas_gpkg(): invalid ZCTA schema for year ", yr)
    }
  }
  
  # Attempt legacy validator if it exists; tolerate failure if it rejects `area_states`.
  if (exists("validate_zcta_sf", mode = "function")) {
    ok_val <- tryCatch({ validate_zcta_sf(zcta); TRUE }, error = function(e) e)
    if (!isTRUE(ok_val)) {
      message("NOTE: validate_zcta_sf(zcta) failed (may be due to new `area_states` column). Proceeding with relaxed validation.")
    } else {
      message("OK: validate_zcta_sf(zcta) passed.")
    }
  } else {
    message("NOTE: validate_zcta_sf() not found; using relaxed validation.")
  }
  
  # ---- Choose temp vs final write location ---------------------------------
  write_dir <- if (use_tmp_then_copy) file.path(tempdir(), "tiger_gpkg_tmp") else dirname(final_path)
  dir.create(write_dir, recursive = TRUE, showWarnings = FALSE)
  tmp_path <- if (use_tmp_then_copy) file.path(write_dir, basename(final_path)) else final_path
  
  # Remove existing outputs to avoid layer appends / schema conflicts
  if (file.exists(tmp_path)) file.remove(tmp_path)
  if (file.exists(final_path) && final_path != tmp_path) file.remove(final_path)
  
  # ---- Write layers ---------------------------------------------------------
  ok <- tryCatch({
    
    # Core areas: combined CBSA+CSA per vintage year
    for (yr in c("2007", "2010", "2020")) {
      core_sf <- normalize_core(cbsa_csa[[yr]], yr)
      sf::st_write(core_sf, tmp_path, layer = paste0("cbsa_csa_", yr), quiet = TRUE)
      message("OK: Wrote layer cbsa_csa_", yr)
    }
    
    # ZCTAs per vintage year (ensure area_states, if present, is before geometry)
    for (yr in c("2000", "2010", "2020")) {
      z <- zcta[[yr]]
      
      # If present, enforce consistent column order: area_states right before geometry
      if ("area_states" %in% names(z)) {
        z <- z |>
          dplyr::relocate(area_states, .before = geometry)
      } else {
        warning(paste0("NOTE: zcta_", yr, " has no area_states (legacy schema)."), call. = FALSE)
      }
      
      sf::st_write(z, tmp_path, layer = paste0("zcta_", yr), quiet = TRUE)
      message("OK: Wrote layer zcta_", yr)
    }
    
    TRUE
  }, error = function(e) e)
  
  if (!isTRUE(ok)) {
    message("ERROR: st_write failed for ", basename(final_path))
    stop("st_write failed for ", basename(final_path), ": ", conditionMessage(ok))
  }
  
  # ---- Copy temp -> final (optional) ---------------------------------------
  if (use_tmp_then_copy) {
    ok2 <- file.copy(tmp_path, final_path, overwrite = TRUE)
    if (!ok2) stop("Failed to copy into final_path: ", final_path)
    file.remove(tmp_path)
    message("OK: Copied temp GPKG into final_path and removed temp file.")
  } else {
    message("OK: Wrote GPKG directly to final_path.")
  }
  
  TRUE
}








