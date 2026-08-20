## ----------------------------------------------------------------
## Define functions used in the Generate the Metrics script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: August 16th, 2026
## Date Modified: August 18th, 2026
## 
## Description: This script defines functions specific to the "Generate the 
##              Metrics" of the data cleaning and validation process. These 
##              supplement the general-purpose functions defined in a separate 
##              script, and were developed in response to findings from the 
##              initial exploratory data analysis, improvements identified from
##              processing the 2023 Formatted data, and variations
##              encountered in the process data update script.
##
## NOTE: Much of this content was developed with the assistance of Yale's
##       AI Clarity.
## 
## Functions
## 
##    1. get_year_from_name: Extract a Four-Digit Year from a String. Searches 
##       a character string for the first occurrence of a four-digit year 
##       beginning with 19 or 20, and returns it as an integer.
## 
##    2. area_table_one: Compute Area in Square Miles for a Given Vintage/Year. 
##       Reprojects an sf object to an equal-area CRS (EPSG:5070), computes 
##       polygon area, converts to acres, then to square miles, and returns an 
##       ID-to-area lookup table.
## 
##    3. count_sf_in_blocks_by_state: Count \code{sf} Objects Across a Nested 
##       Blocks-by-State List. Counts the total number of \code{sf} objects 
##       across all states and decennial layers in a nested list structure 
##       returned by \code{read_state_gpkgs_for_data()}. This allows a progress 
##       tracker to be initialized before iterating over the data, avoiding the 
##       memory cost of loading all blocks simultaneously.
## 
##       Iterates over a two-level nested list (state → layer name) and counts 
##       the total number of elements that inherit from the \code{sf} class.
## 
##    4. get_block_pop_year_by_county: Pull block-level total population for one 
##       county and one decennial year. Uses $$\mathrm{tidycensus::get\_decennial}$$ 
##       to download block-level total population for a single county, returning 
##       a minimal lookup table keyed by the Census block GEOID.
## 
##    5. get_block_pop_state_year: Pull block-level total population for one 
##       state and one decennial year. Enumerates counties for a state (via 
##       $$\mathrm{tigris::counties}$$), then pulls block-level total population 
##       for each county and row-binds the results.
## 
##    6. get_block_pop_all_states: Compile block-level total population for 
##       multiple states and decennial years. Iterates over a vector of states 
##       and a year/specification table (one row per decennial vintage), pulls 
##       block-level total population via $$\mathrm{tidycensus::get\_decennial}$$ 
##       (through `get_block_pop_state_year()`), and returns a master table 
##       keyed by `geoid` with one population column per year (e.g., `pop2000`, 
##       `pop2010`, `pop2020`). A progress bar reports the current state and 
##       year being processed. Verbose console output from tidycensus is 
##       suppressed so the progress bar remains readable.
## 
##    7. get_zcta_pop_year: Pull ZCTA-level total population for one decennial 
##       year (no geometry).
## 
##    8. get_zcta_pop_all_decennials: Pull ZCTA-level total population for 
##       2000/2010/2020 and join wide.

## ----------------------------------------------------------------
## FUNCTIONS

get_year_from_name <- function(nm) {
  #' Extract a Four-Digit Year from a String. Searches a character string for the 
  #' first occurrence of a four-digit year beginning with 19 or 20, and returns 
  #' it as an integer.
  #'
  #' @param nm A character string from which to extract the year (e.g., a file name).
  #'
  #' @return An integer year if a match is found; \code{NA_integer_} otherwise.
  
  y <- str_extract(nm, "(19|20)\\d{2}")
  if (is.na(y)) NA_integer_ else as.integer(y)
}




area_table_one <- function(x, year, id_col = NULL) {
  #' Compute Area in Square Miles for a Given Vintage/Year. Reprojects an sf 
  #' object to an equal-area CRS (EPSG:5070), computes polygon area, converts 
  #' to acres, then to square miles, and returns an ID-to-area lookup table.
  #'
  #' @param x An \code{sf} object containing an identifier column (e.g.,
  #'   \code{geoid} for blocks or \code{area_code} for ZCTAs).
  #' @param year An integer or character value (e.g., \code{2010}) used to name
  #'   the output area column.
  #' @param id_col Optional. Identifier column name (string). If \code{NULL},
  #'   the function will look for common defaults (\code{"geoid"}, \code{"area_code"}).
  #'
  #' @return A \code{tibble} with two columns:
  #'   \describe{
  #'     \item{id}{The identifier inherited from \code{x} (column name preserved).}
  #'     \item{area\{year\}_mi2}{Area in square miles, rounded to \code{sigfigs}
  #'       significant figures.}
  #'   }
  #'
  #' @note Conversion uses $$640 \text{ acres} = 1 \text{ mi}^2$$. Significant
  #'   figures are controlled by the globally defined \code{sigfigs} variable.
  
  stopifnot(inherits(x, "sf"))
  
  # Choose the identifier column
  if (is.null(id_col)) {
    candidates <- c("geoid", "area_code")
    id_col <- candidates[candidates %in% names(x)][1]
    if (is.na(id_col)) {
      stop(
        "No id column detected. Provide id_col, or include one of: ",
        paste(candidates, collapse = ", ")
      )
    }
  } else {
    if (!id_col %in% names(x)) stop("id_col not found in x: ", id_col)
  }
  
  # Reproject to EPSG:5070 (Conus Albers, equal-area)
  x_ea <- sf::st_transform(x, 5070)
  
  # Compute area and convert units to acres
  a_acre <- units::set_units(sf::st_area(x_ea), "acre")
  
  # Convert to sq. miles (640 acres = 1 mi^2)
  area_mi2 <- signif(as.numeric(a_acre) / 640, sigfigs)
  
  # Return ID + area, preserving the ID column name
  tibble::tibble(
    !!id_col := x[[id_col]],
    !!paste0("area", year, "_mi2") := area_mi2
  )
}




count_sf_in_blocks_by_state <- function(blocks_by_state) {
  #' Count \code{sf} Objects Across a Nested Blocks-by-State List. Counts the 
  #' total number of \code{sf} objects across all states and decennial layers in 
  #' a nested list structure returned by \code{read_state_gpkgs_for_data()}. This 
  #' allows a progress tracker to be initialized before iterating over the data, 
  #' avoiding the memory cost of loading all blocks simultaneously.
  #'
  #' Iterates over a two-level nested list (state → layer name) and counts the
  #' total number of elements that inherit from the \code{sf} class.
  #'
  #' @param blocks_by_state A named nested list where the first level is keyed by
  #'   state abbreviation and the second level is keyed by layer name. Elements
  #'   are expected to be \code{sf} objects or \code{NULL}/non-\code{sf} values.
  #'
  #' @return A single integer representing the total count of \code{sf} objects
  #'   across all states and layers.
  
  # Iterate over states
  sum(vapply(names(blocks_by_state), function(st) {
    # Iterate over layers within each state
    sum(vapply(names(blocks_by_state[[st]]), function(nm) {
      # TRUE if element is an sf object
      inherits(blocks_by_state[[st]][[nm]], "sf")
    }, logical(1)))
  }, integer(1)))
}




get_block_pop_year_by_county <- function(state, countyfp, year, sumfile, var_totpop) {
  #' Pull block-level total population for one county and one decennial year.
  #' Uses $$\mathrm{tidycensus::get\_decennial}$$ to download block-level total
  #' population for a single county, returning a minimal lookup table keyed by the
  #' Census block GEOID.
  #'
  #' @param state State identifier accepted by tidycensus (e.g., `"AL"` or `"01"`).
  #' @param countyfp County FIPS code *within state* as a 3-character string
  #'   (e.g., `"001"`). (This is the `county` argument to `get_decennial()`.)
  #' @param year Decennial year (e.g., `2000`, `2010`, `2020`).
  #' @param sumfile Summary file passed to `get_decennial()` (commonly `"pl"` for
  #'   PL 94-171).
  #' @param var_totpop Variable id for total population for that year/sumfile
  #'   (e.g., `"PL001001"`, `"P001001"`, `"P1_001N"`).
  
  # Name the output population column based on the decennial year
  pop_col <- paste0("pop", year)
  
  tidycensus::get_decennial(
    geography = "block",
    variables = c(totpop = var_totpop), # rename to a stable internal name
    year      = year,
    sumfile   = sumfile,
    state     = state,
    county    = countyfp,
    geometry  = FALSE,                  # faster + smaller: attributes only
    output    = "wide"                  # yields GEOID + totpop (not long format)
  ) %>%
    # Standardize the ID column name and create popYYYY
    dplyr::transmute(
      geoid = GEOID,
      !!pop_col := totpop
    )
}




get_block_pop_state_year <- function(state, year, sumfile, var_totpop) {
  #' Pull block-level total population for one state and one decennial year.
  #' Enumerates counties for a state (via $$\mathrm{tigris::counties}$$), then
  #' pulls block-level total population for each county and row-binds the results.
  #'
  #' @param state State identifier accepted by tidycensus (e.g., `"AL"` or `"01"`).
  #' @param year Decennial year (e.g., `2000`, `2010`, `2020`).
  #' @param sumfile Summary file passed to `get_decennial()` (commonly `"pl"`).
  #' @param var_totpop Variable id for total population for that year/sumfile.
  #'
  #' @return A tibble with columns `geoid` and `popYYYY` for all blocks in the state.
  
  # Get a vector of county FIPS (3-digit strings) for the state
  cts <- tigris::counties(state = state, cb = TRUE, year = year) %>%
    sf::st_drop_geometry() %>%                              # keep just attributes
    dplyr::transmute(county = stringr::str_pad(COUNTYFP, 3, pad = "0")) %>%
    dplyr::pull(county)
  
  # Pull block population for each county, then bind into one state-wide table
  purrr::map_dfr(
    cts,
    \(cty) get_block_pop_year_by_county(state, cty, year, sumfile, var_totpop)
  )
}




get_block_pop_all_states <- function(states, spec) {
  #' Compile block-level total population for multiple states and decennial years.
  #' Iterates over a vector of states and a year/specification table (one row per
  #' decennial vintage), pulls block-level total population via
  #' $$\mathrm{tidycensus::get\_decennial}$$ (through `get_block_pop_state_year()`),
  #' and returns a master table keyed by `geoid` with one population column per year
  #' (e.g., `pop2000`, `pop2010`, `pop2020`). A progress bar reports the current
  #' state and year being processed. Verbose console output from tidycensus is
  #' suppressed so the progress bar remains readable.
  #'
  #' @param states Character vector of states to process. Accepts abbreviations
  #'   (e.g., `"AL"`, `"DC"`) or other state identifiers supported by tidycensus.
  #' @param spec A data frame/tibble describing which decennial datasets to pull.
  #'   Must contain columns `year`, `sumfile`, and `var_totpop` (one row per year):
  #'   \describe{
  #'     \item{year}{Decennial year (e.g., `2000`, `2010`, `2020`).}
  #'     \item{sumfile}{Summary file identifier passed to `get_decennial()`
  #'       (often `"pl"` for PL 94-171).}
  #'     \item{var_totpop}{Variable id for total population in that vintage
  #'       (e.g., `"PL001001"`, `"P001001"`, `"P1_001N"`).}
  #'   }
  #'
  #' @return A tibble keyed by `geoid`, containing one column per requested year:
  #' \describe{
  #'   \item{geoid}{Block GEOID.}
  #'   \item{popYYYY}{Total population for each decennial year in `spec`.}
  #' }
  #'
  #' @details
  #' This function relies on `get_block_pop_state_year()` returning a tibble with
  #' columns `geoid` and `pop{year}` for a single state-year pull (all counties in
  #' the state). Within each state, year-tables are joined using a full join on
  #' `geoid`; then all states are row-bound together.
  #'
  #' @note
  #' The tidycensus API can be slow and may rate-limit large pulls. Consider caching
  #' (e.g., `tigris::use_cache = TRUE`) and/or adding retries if you see failures.
  #'
  #' @examples
  #' \dontrun{
  #' spec <- tibble::tribble(
  #'   ~year, ~sumfile, ~var_totpop,
  #'   2000,  "pl",     "PL001001",
  #'   2010,  "pl",     "P001001",
  #'   2020,  "pl",     "P1_001N"
  #' )
  #' states <- c("AL", "AK", "DC")
  #' pop_block_master <- get_block_pop_all_states(states, spec)
  #' }
  
  # Initialize progress bar (state × year)
  pb <- progress::progress_bar$new(
    format = "Blocks [:bar] :current/:total (:percent) | :state | :year",
    total  = length(states) * nrow(spec),
    clear  = FALSE,
    width  = 80
  )
  
  # ---- pull and assemble data ----------------------------------------------
  # For each state, pull each requested decennial year then join the year tables
  # on geoid to produce one wide table per state.
  state_tables <- purrr::map(states, function(st) {
    
    # Pull one table per spec row (year/sumfile/variable)
    pop_list_state <- purrr::pmap(
      spec,
      \(year, sumfile, var_totpop) {
        
        # Update progress bar with the state + decennial period being fetched
        pb$tick(tokens = list(state = st, year = as.character(year)))
        
        # Silence tidycensus chatter (messages + warnings + printed output)
        # so the progress bar stays readable.
        invisible(
          capture.output(
            suppressWarnings(
              suppressMessages(
                out <- get_block_pop_state_year(st, year, sumfile, var_totpop)
              )
            ),
            type = "output"
          )
        )
        
        out
      }
    )
    
    # Join the years side-by-side within this state: geoid + pop2000 + pop2010 + ...
    purrr::reduce(pop_list_state, dplyr::full_join, by = "geoid")
  })
  
  # Bind all states into one master table and ensure one row per geoid
  bind_rows(state_tables) %>%
    distinct(geoid, .keep_all = TRUE) %>%
    arrange(geoid)
}




get_zcta_pop_year <- function(year, sumfile, var_totpop) {
  #' Pull ZCTA-level total population for one decennial year (no geometry)
  #'
  #' @param year Decennial year (2000, 2010, 2020)
  #' @param sumfile Summary file (commonly "pl")
  #' @param var_totpop Total-pop variable for that year/sumfile
  #'
  #' @return tibble with columns: zcta, popYYYY
  
  # Create a year-specific output column name (e.g., pop2000, pop2010, pop2020)
  pop_col <- paste0("pop", year)
  
  tidycensus::get_decennial(
    geography = "zcta",                     # ZIP Code Tabulation Areas
    variables = c(totpop = var_totpop),     # rename variable to a stable name
    year      = year,                       # decennial vintage
    sumfile   = sumfile,                    # usually "pl" (PL 94-171)
    geometry  = FALSE,                      # no sf polygons (much smaller/faster)
    output    = "wide"                      # returns GEOID + totpop (not long)
  ) %>%
    # Keep only the ID and population, and standardize names
    dplyr::transmute(
      zcta = GEOID,                         # GEOID is the 5-digit ZCTA code
      !!pop_col := totpop                   # dynamic column name for this year
    )
}




get_zcta_pop_all_decennials <- function(spec) {
  #' Pull ZCTA-level total population for 2000/2010/2020 and join wide
  #'
  #' @param spec tibble/data.frame with columns year, sumfile, var_totpop
  #'   (should include rows for 2000, 2010, 2020)
  #'
  #' @return tibble with columns: zcta, pop2000, pop2010, pop2020
  
  # spec must have columns: year, sumfile, var_totpop (one row per year)
  pop_list <- purrr::pmap(
    spec,
    \(year, sumfile, var_totpop) {
      get_zcta_pop_year(year, sumfile, var_totpop)
    }
  )
  
  # Join year-specific tables side-by-side: zcta + pop2000 + pop2010 + pop2020
  purrr::reduce(pop_list, dplyr::full_join, by = "zcta") %>%
    dplyr::arrange(zcta)
}



