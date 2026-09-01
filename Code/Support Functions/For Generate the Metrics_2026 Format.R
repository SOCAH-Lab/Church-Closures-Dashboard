## ----------------------------------------------------------------
## Define functions used in the Generate the Metrics script for the 2026 Formatted data.
##
##       Authors: Shelby Golden, MS from Yale's YSPH DSDE group
##  Date Created: August 16th, 2026
## Date Modified: August 31st, 2026
## 
## Description: This script defines functions specific to the "Generate the 
##              Metrics" of the data cleaning and validation process. These 
##              supplement the general-purpose functions defined in a separate 
##              script, and were developed in response to findings from the 
##              initial exploratory data analysis, improvements identified from
##              processing the 2023 Formatted data, and variations encountered 
##              in the process data update script.
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
## 
##    9. filter_ts: Filter a Time-Series ABI Data Table by Selected Year Columns. 
##       Retains only rows that have at least one non-zero value across the 
##       specified year columns, and drops any year columns that fall outside 
##       the requested range.
## 
##   10. calculate_closure: Compute Church Closure and Reopening Events from a 
##       Time-Series Data Table. Given a wide-format \code{data.table} with one 
##       column per year, counts the number of closure and reopening events per 
##       ABI (Aggregated Business Identifier) using a run-length state machine.  
##       Multi-address ABIs are handled either by exclusion (\code{"skip"}) or 
##       by column-wise compression (\code{"compress"}).
## 
##   11. abi_any_true: Return ABI Identifiers Where Any Specified Column Is TRUE. 
##       Scans one or more logical (or logical-coercible) columns within a 
##       \code{data.table} and returns the distinct values of an identifier 
##       column for every group in which \emph{at least one} of those columns is 
##       \code{TRUE}.
## 
##   12. build_year_windows: Build a Table of All Valid Year-Range Windows. 
##       Inspects the column names of a \code{data.table} (or any named object), 
##       extracts four-digit year columns, and returns a \code{data.table} 
##       listing every contiguous start/end pair whose span is at least 
##       \code{min_span} years — up to and including the full available range.
## 
##   13. rollup_results: Aggregate ABI-year discrete results to multiple GEOID 
##       prefix levels and to ZCTA, optionally stratified by religion.
## 
##       Key semantics (refactor):
##          Event-count columns (closures/reopenings) represent counts of events 
##          within a window, which may exceed 1. For rollups we now produce:
##             - <event>_any__<lab>         : count of ABIs with > 0 events 
##                                            (treat >0 as 1)
##             - <event>_avg__<lab>         : mean events per ABI in that area
##             - <event>_max__<lab>         : max events on any ABI in that area
## 
##       Rates are computed off the *_any__<lab> columns (businesses with any 
##       event).
## 
##   14. reorder_rollup_cols: Reorder rollup output columns into a stable, 
##       human-readable layout. Pure presentation helper: does NOT change 
##       values, only column order.
## 
##       Layout target:
##          1) Geographic IDs + year + (optional) religion + denominators first
##          2) Window-specific "event" block next, in a deterministic order:
##             - n_open__<lab>
##             - no_moves closures: any, rates, avg, max
##             - no_moves reopenings: any, avg, max
##             - n_move__<lab> (after reopenings_no_moves_max__<lab>)
##             - all closures: any, rates, avg, max
##             - all reopenings: any, avg, max
##          3) Everything else after (moves totals, distances, flags, etc.)
## 
##       NOTE on semantics (matches your rollup_results()):
##          *_any__<lab> = count of ABIs with >0 events (binary per ABI: >0 -> 1)
##          *_avg__<lab> and *_max__<lab> are computed over ABIs with >0 events
##          (zeros excluded), so they summarize "eventful" ABIs only.
## 
##   15. preflight_a_date_labels: Validate date labels across all input parquet 
##       folders (Pre-Flight A). Scans the column names of every geography's 
##       input parquet chunks and extracts the date labels embedded after the 
##       last double-underscore separator (e.g. \code{n_open__2000_2004} yields 
##       \code{2000_2004}). Confirms that the union of all found labels matches 
##       \code{date_order} exactly -- no labels missing, no labels extra -- 
##       across every folder and in aggregate.
## 
##       The check is intentionally strict: a label present in \code{date_order}
##       but absent from even one folder will trigger a hard stop, because the
##       subsequent column-ordering and coverage logic assumes a complete,
##       consistent label set.
## 
##   16. preflight_b_metric_pairs: Validate metric x date column pairs across 
##       all input parquet folders (Pre-Flight B). For each geography, reads the 
##       merged column schema from all input parquet chunks and verifies that 
##       every combination of \code{metric_order} and \code{date_order} is 
##       represented as exactly one column, named in the form 
##       \code{<metric>__<date_label>}. Three distinct problems are detected:
## 
##             - Missing pairs: A \code{metric x date_label combination expected
##               from the \code{metric_order} x \code{date_order} grid has no
##               corresponding column in the folder. This would produce 
##               \code{NA}-filled columns or silent data loss in the compiled 
##               output.
##             - Duplicate pairs: The same \code{metric x date_label combination
##               appears more than once as a column name. This would cause 
##               ambiguity in the downstream \code{SELECT} and is treated as a 
##               hard failure.
##             - Partial metrics A metric is present for some date labels but 
##               not all. These are a subset of missing pairs and are reported 
##               separately as an informational diagnostic to help distinguish a 
##               fully absent metric from one that was only partially computed.
## 
##       Missing pairs and duplicates both trigger a hard stop; partial metrics 
##       are informational only and do not independently stop execution (they 
##       will already be captured by the missing-pairs check).
## 
##   17. preflight_c_id_lengths: Validate ID column character lengths across 
##       all input parquet folders (Pre-Flight C). For each geography, queries 
##       the full distribution of character lengths present in the identifier 
##       column (\code{geoid} or \code{zcta}) across all input parquet chunks 
##       and confirms that every value is exactly the expected Census Bureau 
##       length for that geography level:
## 
##       \tabular{ll}{
##          \strong{Geography} \tab \strong{Expected length} \cr
##          block_group        \tab 12 \cr
##          tract              \tab 11 \cr
##          county             \tab  5 \cr
##          state              \tab  2 \cr
##          zcta               \tab  5 \cr
##       }
## 
##       These lengths reflect standard Census FIPS/GEOID encoding where each
##       component is zero-padded to a fixed width (e.g. a block group GEOID is
##       composed of 2-digit state + 3-digit county + 6-digit tract + 1-digit
##       block group = 12 characters total). Any deviation -- including IDs that
##       are too short due to a missing leading zero -- is treated as a hard
##       failure, because malformed IDs would corrupt the division-filter logic
##       (\code{substring(geoid, 1, 2)}) and any downstream spatial joins.
## 
##       The full length distribution is reported on failure (not just a count
##       of bad rows) so the user can distinguish, for example, a handful of
##       truncated IDs from a systematic encoding error affecting a whole state.

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




filter_ts <- function(abi_ts, year_cols) {
  #' Filter a Time-Series ABI Data Table by Selected Year Columns. Retains only 
  #' rows that have at least one non-zero value across the specified year 
  #' columns, and drops any year columns that fall outside the requested range.
  #'
  #' @param abi_ts  A \code{data.table} containing ABI data. Must include a
  #'   column named \code{"abi"} and one or more four-digit year columns
  #'   (e.g. \code{"2018"}, \code{"2019"}, …).
  #' @param year_cols A character or numeric vector of year column names/values to
  #'   retain. Values are coerced to character for matching against
  #'   \code{names(abi_ts)}.
  #'
  #' @return A \code{data.table} containing only the rows with activity in at
  #'   least one of the selected years, with out-of-range year columns removed.
  #'
  #' @details
  #' Row filtering is performed by converting the selected year columns to a
  #' matrix and checking that at least one value per row is non-zero.
  #' Year columns are identified by a strict four-digit name pattern
  #' (\code{^\\d\{4\}$}); all other columns are preserved regardless of their
  #' position in the table.
  #'
  #' @seealso \code{\link[data.table]{data.table}}
  #'
  #' @examples
  #' library(data.table)
  #' dt <- data.table(
  #'   abi   = c("A", "B", "C"),
  #'   `2020` = c(1L, 0L, 0L),
  #'   `2021` = c(0L, 1L, 0L),
  #'   `2022` = c(0L, 0L, 0L)
  #' )
  #' filter_ts(dt, year_cols = c("2020", "2021"))
  #' # Returns rows A and B; column "2022" is dropped.
  
  # ── Input validation ────────────────────────────────────────────────────────
  
  # Ensure the input is a data.table (fails fast with a clear message).
  stopifnot(data.table::is.data.table(abi_ts))
  
  # The "abi" column is required as a stable row identifier.
  if (!"abi" %in% names(abi_ts)) stop("abi_ts must contain column 'abi'.")
  
  # ── Resolve requested year columns ──────────────────────────────────────────
  
  # Coerce to character and keep only columns that actually exist in the table,
  # allowing callers to pass numeric years (e.g. 2020L) without errors.
  yc <- intersect(as.character(year_cols), names(abi_ts))
  
  # Abort early if none of the requested years are present – avoids returning a
  # silently empty result.
  if (length(yc) < 1L) stop("No selected year columns found in abi_ts.")
  
  # ── Row filtering ────────────────────────────────────────────────────────────
  
  # Extract selected year columns as a plain matrix for vectorised row-sum.
  # data.table's `..yc` syntax evaluates `yc` as a column-name vector.
  m <- as.matrix(abi_ts[, ..yc])
  
  # A row is kept when at least one selected year has a non-zero value.
  # na.rm = TRUE treats missing entries as inactive (zero).
  # Note: if values are strictly 0/1 flags, `rowSums(m, na.rm = TRUE) > 0L`
  # is equivalent and marginally faster.
  keep <- rowSums(m != 0L, na.rm = TRUE) > 0L
  
  # ── Drop out-of-range year columns ──────────────────────────────────────────
  
  # Identify every column whose name looks like a four-digit year.
  year_cols_present <- names(abi_ts)[grepl("^\\d{4}$", names(abi_ts))]
  
  # Determine which year columns were *not* requested by the caller.
  drop_years <- setdiff(year_cols_present, yc)
  
  # Apply the row filter, then remove the unwanted year columns in-place
  # (`:=` with NULL), and return the result visibly with the trailing `[]`.
  abi_ts[keep][, (drop_years) := NULL][]
}




calculate_closure <- function(DT,
                              min_zero_run       = 4L,
                              min_one_run_reopen = 2L,
                              multi_addr_mode    = c("skip", "compress")) {
  #' Compute Church Closure and Reopening Events from a Time-Series Data Table.
  #' Given a wide-format \code{data.table} with one column per year, counts the
  #' number of closure and reopening events per ABI (Aggregated Business
  #' Identifier) using a run-length state machine.  Multi-address ABIs are
  #' handled either by exclusion (\code{"skip"}) or by column-wise compression
  #' (\code{"compress"}).
  #'
  #' @param DT A \code{data.table} containing at least one column named
  #'   \code{"abi"} and two or more four-digit year columns whose names match
  #'   \code{^(19|20)\\d\{2\}$} (e.g. \code{"1999"}, \code{"2018"}).  Year
  #'   column values should be integer-coercible activity flags (\code{0}/
  #'   \code{1} or counts).
  #' @param min_zero_run A positive integer giving the minimum consecutive number
  #'   of zero-valued years required to record a \emph{closure} event.
  #'   Default: \code{4L}.
  #' @param min_one_run_reopen A positive integer giving the minimum consecutive
  #'   number of one-valued years, following a closure, required to record a
  #'   \emph{reopening} event.  Default: \code{2L}.
  #' @param multi_addr_mode Character scalar, one of \code{"skip"} or
  #'   \code{"compress"}, controlling how ABIs that appear on more than one row
  #'   are treated. \cr
  #'   \code{"skip"} — drop all multi-row ABIs entirely; no move columns are
  #'   included in the output. \cr
  #'   \code{"compress"} — collapse all rows for each ABI into one by summing
  #'   year columns (then binarising) and aggregating move-distance metadata;
  #'   move summary columns are appended to the output.
  #'
  #' @return A \code{data.table} with one row per ABI and the following columns,
  #'   depending on \code{multi_addr_mode}:
  #'   \describe{
  #'     \item{\code{abi}}{ABI identifier.}
  #'     \item{\code{closures_no_moves} / \code{closures_all}}{Number of closure
  #'       events detected (\code{"skip"} / \code{"compress"} naming).}
  #'     \item{\code{reopenings_no_moves} / \code{reopenings_all}}{Number of
  #'       reopening events detected.}
  #'     \item{\code{moves_total}}{(\code{"compress"} only) Total number of
  #'       address moves, or \code{NA} if \code{n_moves} is absent.}
  #'     \item{\code{wavg_dist_km}}{(\code{"compress"} only) Move-count-weighted
  #'       mean distance in km.}
  #'     \item{\code{max_dist_km}}{(\code{"compress"} only) Maximum single-move
  #'       distance in km.}
  #'     \item{\code{move_gt_5mi}, \code{move_gt_10mi}, \code{move_gt_25mi}}{
  #'       (\code{"compress"} only) Logical flags indicating whether any move
  #'       exceeded 5, 10, or 25 miles respectively.}
  #'   }
  #'   Year columns are \emph{not} included in the returned object.
  #'
  #' @details
  #' \strong{State machine logic (\code{count_events}):}
  #' Each ABI's year vector is scanned left-to-right through three states:
  #' \code{preopen} (no activity seen yet), \code{open} (currently active), and
  #' \code{closed} (activity ceased).  A closure is recorded the moment a run
  #' of zeros reaches \code{min_zero_run}; a reopening is recorded when a
  #' subsequent run of ones reaches \code{min_one_run_reopen}.
  #'
  #' \strong{Compression:}
  #' In \code{"compress"} mode, year columns are summed across all rows sharing
  #' an ABI and then binarised (\code{> 0}), so a church active at \emph{any}
  #' address in a given year is treated as active.  Move metadata columns are
  #' aggregated with weighted means and logical ORs before being joined back to
  #' the compressed time series.
  #'
  #' @seealso \code{\link{filter_ts}}, \code{\link[data.table]{data.table}}
  #'
  #' @examples
  #' library(data.table)
  #' dt <- data.table(
  #'   abi   = c("A", "A", "B"),
  #'   `2018` = c(1L, 0L, 1L),
  #'   `2019` = c(1L, 0L, 1L),
  #'   `2020` = c(0L, 0L, 0L),
  #'   `2021` = c(0L, 0L, 0L),
  #'   `2022` = c(0L, 0L, 0L),
  #'   `2023` = c(0L, 0L, 0L),
  #'   `2024` = c(1L, 0L, 0L),
  #'   `2025` = c(1L, 0L, 0L)
  #' )
  #' # "skip" mode: ABI "A" has two rows and is dropped
  #' calculate_closure(dt, multi_addr_mode = "skip")
  #'
  #' # "compress" mode: ABI "A" rows are OR-merged before event counting
  #' calculate_closure(dt, multi_addr_mode = "compress")
  
  # Validate and resolve the multi-address handling strategy.
  multi_addr_mode <- match.arg(multi_addr_mode)
  
  # ── Identify year columns ────────────────────────────────────────────────────
  yc <- names(DT)[grepl("^(19|20)\\d{2}$", names(DT))]
  if (length(yc) == 0L) stop("No year columns found in DT (expected names like '2018','2019',...).")
  if (length(yc) < 2L) stop("Need at least 2 year columns to evaluate events.")
  yc <- sort(yc)
  
  # ── State-machine event counter ──────────────────────────────────────────────
  count_events <- function(x, k0, k1) {
    
    seen_open <- FALSE
    state     <- "preopen"
    zrun      <- 0L
    orun      <- 0L
    closures  <- 0L
    reopens   <- 0L
    
    for (v in x) {
      v <- as.integer(v)
      
      if (v == 1L) {
        seen_open <- TRUE
        orun <- orun + 1L
        zrun <- 0L
        
        if (state == "closed" && orun == k1) {
          reopens <- reopens + 1L
          state <- "open"
        } else if (state == "preopen") {
          state <- "open"
        }
        
      } else {
        zrun <- zrun + 1L
        orun <- 0L
        
        if (seen_open && state != "closed" && zrun == k0) {
          closures <- closures + 1L
          state <- "closed"
        }
      }
    }
    
    c(closures = closures, reopenings = reopens)
  }
  
  # ── Multi-address handling ───────────────────────────────────────────────────
  abi_rows <- DT[, .N, by = abi]
  
  if (multi_addr_mode == "skip") {
    
    church_use <- DT[abi_rows[N == 1L], on = "abi"]
    
    abi_ts <- church_use[, lapply(.SD, \(x) as.integer(max(x, na.rm = TRUE))),
                         by = abi, .SDcols = yc]
    
  } else if (multi_addr_mode == "compress") {
    
    church_use <- DT
    
    # ---- Move metadata aggregation (Option A flags derived from max_dist_km) ---
    move_agg <- church_use[, {
      
      # Total moves: keep your current semantics (sum across rows)
      # NOTE: if n_moves is replicated per ABI across rows, consider using max()
      moves_total <- if (!("n_moves" %in% names(church_use))) {
        NA_integer_
      } else if (all(is.na(n_moves))) {
        NA_integer_
      } else {
        as.integer(sum(n_moves, na.rm = TRUE))
      }
      
      if (is.na(moves_total)) {
        
        .(
          moves_total  = NA_integer_,
          wavg_dist_km = NA_real_,
          max_dist_km  = NA_real_,
          move_gt_5mi  = NA_integer_,
          move_gt_10mi = NA_integer_,
          move_gt_25mi = NA_integer_
        )
        
      } else {
        
        # Weighted mean distance (weights = n_moves)
        wavg_dist_km <- if (!all(c("n_moves", "mean_dist_km") %in% names(church_use))) {
          NA_real_
        } else {
          ok   <- !is.na(n_moves) & n_moves > 0 & !is.na(mean_dist_km)
          wsum <- sum(n_moves[ok], na.rm = TRUE)
          if (wsum == 0) NA_real_ else sum(mean_dist_km[ok] * n_moves[ok], na.rm = TRUE) / wsum
        }
        
        # Max distance across rows
        max_dist_km <- if (!("max_dist_km" %in% names(church_use))) {
          NA_real_
        } else if (all(is.na(max_dist_km))) {
          NA_real_
        } else {
          max(max_dist_km, na.rm = TRUE)
        }
        
        # ---- Option A: derive threshold flags from max_dist_km only -------------
        mi_to_km <- 1.609344
        thr_5  <-  5 * mi_to_km
        thr_10 <- 10 * mi_to_km
        thr_25 <- 25 * mi_to_km
        
        move_gt_5mi  <- if (is.na(max_dist_km)) NA_integer_ else as.integer(max_dist_km > thr_5)
        move_gt_10mi <- if (is.na(max_dist_km)) NA_integer_ else as.integer(max_dist_km > thr_10)
        move_gt_25mi <- if (is.na(max_dist_km)) NA_integer_ else as.integer(max_dist_km > thr_25)
        
        .(
          moves_total  = moves_total,
          wavg_dist_km = wavg_dist_km,
          max_dist_km  = max_dist_km,
          move_gt_5mi  = move_gt_5mi,
          move_gt_10mi = move_gt_10mi,
          move_gt_25mi = move_gt_25mi
        )
      }
    }, by = abi]
    
    # ── Compress year columns ─────────────────────────────────────────────────
    abi_ts <- church_use[, lapply(.SD, \(x) as.integer(sum(x, na.rm = TRUE))),
                         by = abi, .SDcols = yc]
    abi_ts[, (yc) := lapply(.SD, \(x) as.integer(x > 0L)), .SDcols = yc]
    
    # Join move metadata back
    data.table::setkey(move_agg, abi)
    data.table::setkey(abi_ts, abi)
    abi_ts <- move_agg[abi_ts]
    
  } else {
    stop("multi_addr_mode must be 'skip' or 'compress'")
  }
  
  # ── Event counting ───────────────────────────────────────────────────────────
  mat <- as.matrix(abi_ts[, ..yc])
  
  ev <- t(apply(mat, 1L, count_events,
                k0 = as.integer(min_zero_run),
                k1 = as.integer(min_one_run_reopen)))
  
  # ── Attach event columns ─────────────────────────────────────────────────────
  if (multi_addr_mode == "compress") {
    abi_ts[, `:=`(
      closures_all   = as.integer(ev[, "closures"]),
      reopenings_all = as.integer(ev[, "reopenings"])
    )]
  } else {
    abi_ts[, `:=`(
      closures_no_moves   = as.integer(ev[, "closures"]),
      reopenings_no_moves = as.integer(ev[, "reopenings"])
    )]
  }
  
  # ── Final column ordering ────────────────────────────────────────────────────
  move_cols <- if (multi_addr_mode == "compress") {
    c("moves_total", "wavg_dist_km", "max_dist_km",
      "move_gt_5mi", "move_gt_10mi", "move_gt_25mi")
  } else {
    character(0)
  }
  
  event_cols <- if (multi_addr_mode == "compress") {
    c("closures_all", "reopenings_all")
  } else {
    c("closures_no_moves", "reopenings_no_moves")
  }
  
  out_order <- c("abi", event_cols, move_cols)
  out_order <- out_order[out_order %in% names(abi_ts)]
  data.table::setcolorder(abi_ts, out_order)
  
  abi_ts[, (yc) := NULL]
  abi_ts[]
}




abi_any_true <- function(DT, cols, id = "abi") {
  #' Return ABI Identifiers Where Any Specified Column Is TRUE. Scans one or more 
  #' logical (or logical-coercible) columns within a \code{data.table} and returns 
  #' the distinct values of an identifier column for every group in which 
  #' \emph{at least one} of those columns is \code{TRUE}.
  #'
  #' @param DT  A \code{data.table} containing the identifier column and at least
  #'   one of the columns named in \code{cols}.
  #' @param cols A character vector of column names to test.  Names that are
  #'   absent from \code{DT} are silently dropped; an error is raised if
  #'   \emph{none} of the requested names exist.
  #' @param id A single character string giving the name of the grouping /
  #'   identifier column.  Default: \code{"abi"}.
  #'
  #' @return A vector of the same type as \code{DT[[id]]} containing the distinct
  #'   identifier values for which at least one value across the tested columns
  #'   equals \code{TRUE}.  Returns an empty vector (length 0) when no group
  #'   satisfies the condition.
  #'
  #' @details
  #' Column testing uses \code{x \%in\% TRUE} rather than \code{isTRUE(x)} or a
  #' bare \code{x == TRUE} so that \code{NA} values are treated as \code{FALSE}
  #' without raising warnings or errors.  The element-wise OR across all tested
  #' columns is accumulated with \code{Reduce(`|`, ...)} before being collapsed
  #' to a single per-group scalar via \code{any()}.
  #'
  #' @seealso \code{\link{calculate_closure}}, \code{\link{filter_ts}},
  #'   \code{\link[data.table]{data.table}}
  #'
  #' @examples
  #' library(data.table)
  #' dt <- data.table(
  #'   abi          = c("A", "A", "B", "C"),
  #'   move_gt_5mi  = c(TRUE,  FALSE, FALSE, NA),
  #'   move_gt_10mi = c(FALSE, FALSE, TRUE,  NA),
  #'   move_gt_25mi = c(FALSE, FALSE, FALSE, NA)
  #' )
  #'
  #' # "A" qualifies via move_gt_5mi; "B" qualifies via move_gt_10mi;
  #' # "C" has only NAs, which are treated as FALSE, so it is excluded.
  #' abi_any_true(dt, cols = c("move_gt_5mi", "move_gt_10mi", "move_gt_25mi"))
  #' #> [1] "A" "B"
  #'
  #' # Requesting a column that does not exist is handled gracefully.
  #' abi_any_true(dt, cols = c("move_gt_5mi", "nonexistent_col"))
  #' #> [1] "A"
  
  # ── Input validation ─────────────────────────────────────────────────────────
  
  # Restrict to columns that actually exist in DT; unknown names are silently
  # ignored so that callers can pass a fixed "candidate" vector without needing
  # to know which columns are present in a given subset of data.
  cols <- intersect(cols, names(DT))
  
  # Abort early if the intersection is empty – returning an empty vector
  # silently here would be a hard-to-diagnose bug for the caller.
  if (!length(cols)) stop("None of the requested cols are in DT.")
  
  # ── Per-group any-TRUE test ───────────────────────────────────────────────────
  
  DT[,
     .(
       # For each column, produce a logical vector with x %in% TRUE, which
       # maps TRUE → TRUE and both FALSE and NA → FALSE (NA-safe, no warnings).
       # Reduce(`|`, ...) folds the list of per-column logical vectors into a
       # single vector via element-wise OR across all tested columns.
       # any() then collapses that vector to one scalar per group, yielding
       # TRUE when at least one row × column cell is TRUE for this id value.
       keep = any(Reduce(`|`, lapply(.SD, \(x) x %in% TRUE)))
     ),
     by     = id,        # group by the identifier column (default: "abi")
     .SDcols = cols      # restrict .SD to the validated column subset
  ][
    # ── Filter to qualifying groups ───────────────────────────────────────────
    # Retain only rows where the per-group flag is TRUE, then extract the
    # identifier values as a plain vector (not a one-column data.table).
    keep == TRUE, get(id)
  ]
}




build_year_windows <- function(DT, min_span = 5L) {
  #' Build a Table of All Valid Year-Range Windows. Inspects the column names of 
  #' a \code{data.table} (or any named object), extracts four-digit year columns, 
  #' and returns a \code{data.table} listing every contiguous start/end pair whose 
  #' span is at least \code{min_span} years — up to and including the full 
  #' available range.
  #'
  #' @param DT A \code{data.table} (or any object with a \code{names()} method)
  #'   containing four-digit year columns whose names match
  #'   \code{^(19|20)\\d\{2\}$} (e.g. \code{"2000"}, \code{"2025"}).
  #'   Non-year columns are silently ignored.
  #' @param min_span A positive integer giving the minimum number of consecutive
  #'   years a window must contain to be included.  Default: \code{5L}.
  #'
  #' @return A \code{data.table} with one row per valid window and four columns:
  #'   \describe{
  #'     \item{\code{start}}{Integer. First year of the window.}
  #'     \item{\code{end}}{Integer. Last year of the window.}
  #'     \item{\code{span}}{Integer. Number of years in the window
  #'       (\code{end - start + 1}).}
  #'     \item{\code{label}}{Character. Compact window identifier in the form
  #'       \code{"<start>_<end>"} (e.g. \code{"2000_2025"}).}
  #'   }
  #'   Rows are sorted by \code{start} ascending, then \code{end} ascending.
  #'   Returns a zero-row \code{data.table} with the same columns if no valid
  #'   windows exist.
  #'
  #' @details
  #' All contiguous sub-ranges \eqn{[y_s,\, y_e]} of the detected year columns
  #' are enumerated by a double loop over start-index \eqn{s} and end-index
  #' \eqn{e \ge s + \texttt{min\_span} - 1}.  For \code{"2000"}–\code{"2025"}
  #' (26 years) and the default \code{min\_span = 5} this produces
  #' \eqn{\sum_{k=5}^{26}(26 - k + 1) = 275} windows.
  #'
  #' The function is intentionally side-effect-free and does not modify \code{DT}.
  #' It is designed to feed its output directly into a downstream loop or
  #' \code{apply}-family call that passes each \code{(start, end)} pair to
  #' \code{\link{filter_ts}} and \code{\link{calculate_closure}}.
  #'
  #' @seealso \code{\link{filter_ts}}, \code{\link{calculate_closure}}
  #'
  #' @examples
  #' library(data.table)
  #'
  #' # Toy dataset with year columns 2000-2025 plus metadata columns.
  #' dt <- data.table(
  #'   abi     = character(),
  #'   address = character()
  #' )
  #' dt[, (as.character(2000:2025)) := integer()]
  #'
  #' windows <- build_year_windows(dt)
  #' nrow(windows)          # 275 windows for a 26-year range at min_span = 5
  #' head(windows)          # shortest windows starting from 2000
  #' tail(windows)          # longest / latest-ending windows
  #' windows[span == 26L]   # the single full-range row: 2000_2025
  #'
  #' # Raise the minimum span to a decade.
  #' build_year_windows(dt, min_span = 10L)
  
  # ── Input validation ─────────────────────────────────────────────────────────
  
  min_span <- as.integer(min_span)
  if (min_span < 1L) stop("`min_span` must be a positive integer.")
  
  # ── Detect and sort available year columns ───────────────────────────────────
  
  # Match column names that are exactly a 4-digit year in range 1900–2099;
  # sort() on zero-padded strings is equivalent to numeric sort here.
  yc <- sort(names(DT)[grepl("^(19|20)\\d{2}$", names(DT))])
  
  # Return an empty table immediately if the dataset has too few year columns
  # to form even one window of the requested minimum span.
  if (length(yc) < min_span) {
    warning(sprintf(
      "Only %d year column(s) detected; no windows of span >= %d possible.",
      length(yc), min_span
    ))
    return(
      data.table::data.table(
        start = integer(),
        end   = integer(),
        span  = integer(),
        label = character()
      )
    )
  }
  
  # Convert year-name strings to integers once for arithmetic and storage.
  years    <- as.integer(yc)
  n_years  <- length(years)
  
  # ── Enumerate all valid contiguous windows ───────────────────────────────────
  
  # Pre-allocate vectors at the theoretical maximum length to avoid repeated
  # copying inside the loop.  The maximum number of valid windows is:
  #   sum_{k = min_span}^{n_years} (n_years - k + 1)
  # which is at most n_years^2 / 2, so integer() is safe.
  max_wins <- as.integer((n_years - min_span + 1L) * (n_years - min_span + 2L) / 2L)
  v_start  <- integer(max_wins)
  v_end    <- integer(max_wins)
  idx      <- 0L  # fill pointer
  
  for (s in seq_len(n_years - min_span + 1L)) {          # start index
    for (e in seq(s + min_span - 1L, n_years)) {         # end index
      
      idx          <- idx + 1L
      v_start[idx] <- years[s]
      v_end[idx]   <- years[e]
    }
  }
  
  # Trim unused pre-allocated slots (only relevant when min_span > 2).
  v_start <- v_start[seq_len(idx)]
  v_end   <- v_end[seq_len(idx)]
  
  # ── Assemble and return the result table ─────────────────────────────────────
  
  data.table::data.table(
    start = v_start,
    end   = v_end,
    span  = v_end - v_start + 1L,             # inclusive year count
    label = paste0(v_start, "_", v_end)        # e.g. "2000_2025"
  )
}




rollup_results <- function(
    discrete_results,
    agg_fun = function(x) if (is.numeric(x)) sum(x, na.rm = TRUE) else uniqueN(x)
) {
  #' Aggregate ABI-year discrete results to multiple GEOID prefix levels and to
  #' ZCTA, optionally stratified by religion.
  #'
  #' Key semantics (refactor):
  #'   Event-count columns (closures/reopenings) represent counts of events within
  #'   a window, which may exceed 1. For rollups we now produce:
  #'     - <event>_any__<lab>         : count of ABIs with > 0 events (treat >0 as 1)
  #'     - <event>_avg__<lab>         : mean events per ABI in that area
  #'     - <event>_max__<lab>         : max events on any ABI in that area
  #'
  #'   Rates are computed off the *_any__<lab> columns (businesses with any event).
  #'
  #' @return list(by_geoid = named list of DTs, by_zcta = DT)
  
  DT <- data.table::as.data.table(discrete_results)
  DT[, geoid := as.character(geoid)]
  DT[, zcta  := as.character(zcta)]
  
  has_relig <- "religion" %in% names(DT)
  
  # IDs are not aggregated
  drop_ids <- c("abi", "year", "geoid", "zcta")
  if (has_relig) drop_ids <- c(drop_ids, "religion")
  agg_cols <- setdiff(names(DT), drop_ids)
  
  # GEOID prefix lengths
  levels <- list(
    state       = 2L,
    county      = 5L,
    tract       = 11L,
    block_group = 12L,
    block       = 15L
  )
  
  # Identify the window-suffixed event count columns we want to treat specially
  is_event_col <- function(nm) {
    grepl("^(closures|reopenings)_(no_moves|all)__", nm)
  }
  event_cols <- agg_cols[is_event_col(agg_cols)]
  
  # Default aggregation rules (non-event columns)
  # NOTE: if agg_fun returns integer in some groups and double in others, that
  # can also cause type errors. We force numeric outputs to double for safety.
  agg_one <- function(nm, x) {
    
    # Move-threshold columns: count flagged ABIs, not raw sums
    if (grepl("^move_gt_(5mi|10mi|25mi)__", nm)) {
      if (all(is.na(x))) return(NA_integer_)
      return(as.integer(sum(as.logical(x), na.rm = TRUE)))
    }
    
    # moves_total: average, not sum
    if (grepl("^moves_total(__|$)", nm)) {
      if (all(is.na(x))) return(NA_real_)
      return(as.numeric(round(mean(x, na.rm = TRUE), 4)))
    }
    
    if (grepl("^wavg_dist_km__", nm)) {
      v <- if (all(is.na(x))) NA_real_ else round(mean(x, na.rm = TRUE), digits = 2)
      return(as.numeric(v))
    }
    if (grepl("^max_dist_km__", nm)) {
      v <- if (all(is.na(x))) NA_real_ else round(max(x, na.rm = TRUE), digits = 2)
      return(as.numeric(v))
    }
    
    v <- agg_fun(x)
    if (is.numeric(v)) as.numeric(v) else v
  }
  
  # Build the per-group aggregation output:
  # - Keep standard aggregated columns, EXCEPT:
  #     for event_cols we DO NOT keep the summed base column in the rollup output;
  #     instead we emit *_any__<lab>, *_avg__<lab>, *_max__<lab>.
  #
  # CRITICAL: enforce stable output types across groups:
  #   *_any__* -> integer
  #   *_avg__* -> double
  #   *_max__* -> double
  build_group_vals <- function(.SD, agg_cols_use, event_cols_use) {
    
    # (1) Standard columns to aggregate normally (exclude event_cols_use)
    base_cols_use <- setdiff(agg_cols_use, event_cols_use)
    base_vals <- lapply(base_cols_use, function(nm) agg_one(nm, .SD[[nm]]))
    names(base_vals) <- base_cols_use
    
    # (2) Event summary columns: any/avg/max
    if (!length(event_cols_use)) return(base_vals)
    
    any_vals <- vector("list", length(event_cols_use))
    avg_vals <- vector("list", length(event_cols_use))
    max_vals <- vector("list", length(event_cols_use))
    
    names(any_vals) <- character(length(event_cols_use))
    names(avg_vals) <- character(length(event_cols_use))
    names(max_vals) <- character(length(event_cols_use))
    
    for (j in seq_along(event_cols_use)) {
      nm <- event_cols_use[j]
      x  <- .SD[[nm]]
      
      stem <- sub("__(.*)$", "", nm)   # e.g., "closures_no_moves"
      suf  <- sub("^(.*)__", "__", nm) # e.g., "__2010_2015" (keeps leading "__")
      
      out_any <- paste0(stem, "_any", suf)
      out_avg <- paste0(stem, "_avg", suf)
      out_max <- paste0(stem, "_max", suf)
      
      names(any_vals)[j] <- out_any
      names(avg_vals)[j] <- out_avg
      names(max_vals)[j] <- out_max
      
      if (!is.numeric(x)) {
        any_vals[[j]] <- NA_integer_
        avg_vals[[j]] <- NA_real_
        max_vals[[j]] <- NA_real_
      } else {
        
        # ABIs with any events (>0)
        pos <- x[!is.na(x) & x > 0]
        
        # any: count of ABIs with >0 events (INTEGER)
        any_vals[[j]] <- as.integer(length(pos))
        
        # avg/max: ONLY over ABIs with >0 events (exclude zeros)
        if (!length(pos)) {
          avg_vals[[j]] <- NA_real_
          max_vals[[j]] <- NA_real_
        } else {
          avg_vals[[j]] <- as.numeric(round(mean(pos), 4))
          max_vals[[j]] <- as.numeric(max(pos))
        }
      }
    }
    
    c(base_vals, any_vals, avg_vals, max_vals)
  }
  
  # Rates based on *_any__<lab> (NOT summed event counts)
  add_rates <- function(out, id_col = c("geoid", "zcta")) {
    if (is.null(out) || !nrow(out)) return(out)
    
    id_col <- match.arg(id_col)
    pop_col  <- if (id_col == "geoid") "geoid_pop"     else "zcta_pop"
    area_col <- if (id_col == "geoid") "geoid_sqMiles" else "zcta_sqMiles"
    
    cnm_any <- grep("^closures_no_moves_any__", names(out), value = TRUE)
    ca_any  <- grep("^closures_all_any__",     names(out), value = TRUE)
    
    suf_from_any <- function(nm) sub("^closures_(no_moves|all)_any", "", nm)
    
    for (cc in cnm_any) {
      suf <- suf_from_any(cc)  # "__<lab>"
      pop_rate  <- paste0("closures_no_moves_per_10k",  suf)
      area_rate <- paste0("closures_no_moves_per_sqmi", suf)
      
      out[, (pop_rate) := data.table::fifelse(
        is.na(get(pop_col)) | get(pop_col) == 0, NA_real_,
        round((as.numeric(get(cc)) / as.numeric(get(pop_col))) * 10000, 4)
      )]
      out[, (area_rate) := data.table::fifelse(
        is.na(get(area_col)) | get(area_col) == 0, NA_real_,
        round(as.numeric(get(cc)) / as.numeric(get(area_col)), 4)
      )]
    }
    
    for (cc in ca_any) {
      suf <- suf_from_any(cc)  # "__<lab>"
      pop_rate  <- paste0("closures_all_per_10k",  suf)
      area_rate <- paste0("closures_all_per_sqmi", suf)
      
      out[, (pop_rate) := data.table::fifelse(
        is.na(get(pop_col)) | get(pop_col) == 0, NA_real_,
        round((as.numeric(get(cc)) / as.numeric(get(pop_col))) * 10000, 4)
      )]
      out[, (area_rate) := data.table::fifelse(
        is.na(get(area_col)) | get(area_col) == 0, NA_real_,
        round(as.numeric(get(cc)) / as.numeric(get(area_col)), 4)
      )]
    }
    
    out
  }
  
  # ---- GEOID rollups (named list) ----
  roll_geoid <- function(L) {
    x <- DT[nchar(geoid) >= L]
    if (!nrow(x)) return(NULL)
    
    x[, geoid_roll := substr(geoid, 1L, L)]
    by_expr <- if (has_relig) {
      quote(.(geoid = geoid_roll, year, religion))
    } else {
      quote(.(geoid = geoid_roll, year))
    }
    
    # For GEOID rollups, never aggregate zcta denominators
    agg_cols_use <- setdiff(agg_cols, intersect(c("zcta_pop", "zcta_sqMiles"), agg_cols))
    event_cols_use <- intersect(event_cols, agg_cols_use)
    
    out <- x[, build_group_vals(.SD, agg_cols_use = agg_cols_use, event_cols_use = event_cols_use),
             by = eval(by_expr), .SDcols = agg_cols_use]
    
    out <- add_rates(out, id_col = "geoid")
    
    # Safety drop
    drop <- intersect(c("zcta_pop", "zcta_sqMiles"), names(out))
    if (length(drop)) out[, (drop) := NULL]
    
    out
  }
  
  out_geoid <- lapply(levels, roll_geoid)
  out_geoid <- out_geoid[!vapply(out_geoid, is.null, logical(1))]
  
  # ---- ZCTA rollup (single DT) ----
  by_expr_z <- if (has_relig) {
    quote(.(zcta, year, religion))
  } else {
    quote(.(zcta, year))
  }
  
  # For ZCTA rollups, never aggregate geoid denominators
  agg_cols_use_z <- setdiff(agg_cols, intersect(c("geoid_pop", "geoid_sqMiles"), agg_cols))
  event_cols_use_z <- intersect(event_cols, agg_cols_use_z)
  
  out_zcta <- DT[, build_group_vals(.SD, agg_cols_use = agg_cols_use_z, event_cols_use = event_cols_use_z),
                 by = eval(by_expr_z), .SDcols = agg_cols_use_z]
  
  out_zcta <- add_rates(out_zcta, id_col = "zcta")
  
  # Safety drop
  drop <- intersect(c("geoid_pop", "geoid_sqMiles"), names(out_zcta))
  if (length(drop)) out_zcta[, (drop) := NULL]
  
  list(
    by_geoid = out_geoid,
    by_zcta  = out_zcta
  )
}




reorder_rollup_cols <- function(dt, lab, id = c("zcta", "geoid")) {
  #' Reorder rollup output columns into a stable, human-readable layout.
  #' Pure presentation helper: does NOT change values, only column order.
  #'
  #' Layout target:
  #' 1) Geographic IDs + year + (optional) religion + denominators first
  #' 2) Window-specific "event" block next, in a deterministic order:
  #'    - n_open__<lab>
  #'    - no_moves closures: any, rates, avg, max
  #'    - no_moves reopenings: any, avg, max
  #'    - n_move__<lab> (after reopenings_no_moves_max__<lab>)
  #'    - all closures: any, rates, avg, max
  #'    - all reopenings: any, avg, max
  #' 3) Everything else after (moves totals, distances, flags, etc.)
  #'
  #' NOTE on semantics (matches your rollup_results()):
  #'   *_any__<lab> = count of ABIs with >0 events (binary per ABI: >0 -> 1)
  #'   *_avg__<lab> and *_max__<lab> are computed over ABIs with >0 events
  #'   (zeros excluded), so they summarize "eventful" ABIs only.
  #'
  #' @param dt A data.table (or data.frame) rollup output (by_zcta or by_geoid).
  #' @param lab Window label suffix used in column names, e.g. "2010_2015".
  #' @param id Which geography this table is keyed on: "zcta" or "geoid".
  #' @return The same data.table, with columns reordered.
  
  id <- match.arg(id)
  if (is.null(dt) || !nrow(dt)) return(dt)
  data.table::setDT(dt)
  
  # ---------------------------------------------------------------------------
  # (A) FRONT BLOCK: IDs + year + optional religion + denominators
  # ---------------------------------------------------------------------------
  front <- if (id == "zcta") {
    c("zcta", "year", "religion", "zcta_pop", "zcta_sqMiles", "zcta_sqmi")
  } else {
    c("geoid", "year", "religion", "geoid_pop", "geoid_sqMiles", "geoid_sqmi")
  }
  front <- front[front %chin% names(dt)]
  
  # ---------------------------------------------------------------------------
  # (B) WINDOW-SUFFIXED METRICS (event family)
  # ---------------------------------------------------------------------------
  n_open <- paste0("n_open__", lab)
  n_move <- paste0("n_move__", lab)
  
  # ---- no_moves: closures ----
  cnm_any <- paste0("closures_no_moves_any__", lab)
  cnm_sq  <- paste0("closures_no_moves_per_sqmi__", lab)
  cnm_10k <- paste0("closures_no_moves_per_10k__",  lab)
  cnm_avg <- paste0("closures_no_moves_avg__", lab)
  cnm_max <- paste0("closures_no_moves_max__", lab)
  
  # ---- no_moves: reopenings ----
  rnm_any <- paste0("reopenings_no_moves_any__", lab)
  rnm_avg <- paste0("reopenings_no_moves_avg__", lab)
  rnm_max <- paste0("reopenings_no_moves_max__", lab)
  
  # ---- all: closures ----
  ca_any  <- paste0("closures_all_any__", lab)
  ca_sq   <- paste0("closures_all_per_sqmi__", lab)
  ca_10k  <- paste0("closures_all_per_10k__",  lab)
  ca_avg  <- paste0("closures_all_avg__", lab)
  ca_max  <- paste0("closures_all_max__", lab)
  
  # ---- all: reopenings ----
  ra_any  <- paste0("reopenings_all_any__", lab)
  ra_avg  <- paste0("reopenings_all_avg__", lab)
  ra_max  <- paste0("reopenings_all_max__", lab)
  
  # ---------------------------------------------------------------------------
  # (C) EVENT BLOCK ORDER (filtered to columns that exist)
  # ---------------------------------------------------------------------------
  event_block <- c(
    n_open,
    cnm_any, cnm_sq, cnm_10k, cnm_avg, cnm_max,
    rnm_any, rnm_avg, rnm_max, n_move,
    ca_any, ca_sq, ca_10k, ca_avg, ca_max,
    ra_any, ra_avg, ra_max
  )
  event_block <- event_block[event_block %chin% names(dt)]
  
  # ---------------------------------------------------------------------------
  # (D) FINAL ORDER: front block -> event block -> everything else
  # ---------------------------------------------------------------------------
  remaining <- setdiff(names(dt), c(front, event_block))
  data.table::setcolorder(dt, c(front, event_block, remaining))
  dt
}




preflight_a_date_labels <- function(geo_levels, date_order, all_keys, con) {
  #' Validate date labels across all input parquet folders (Pre-Flight A).
  #' Scans the column names of every geography's input parquet chunks and
  #' extracts the date labels embedded after the last double-underscore
  #' separator (e.g. \code{n_open__2000_2004} yields \code{2000_2004}).
  #' Confirms that the union of all found labels matches \code{date_order}
  #' exactly -- no labels missing, no labels extra -- across every folder
  #' and in aggregate.
  #'
  #' The check is intentionally strict: a label present in \code{date_order}
  #' but absent from even one folder will trigger a hard stop, because the
  #' subsequent column-ordering and coverage logic assumes a complete,
  #' consistent label set.
  #'
  #' @param geo_levels A named list of geography definitions. Each element
  #'   must contain at minimum:
  #'   \describe{
  #'     \item{\code{tag}}{Character. Short geography name (e.g.
  #'       \code{"tract"}).}
  #'     \item{\code{in_folder}}{Character. Path to the folder containing
  #'       the input \code{.parquet} chunks for this geography.}
  #'   }
  #' @param date_order Character vector of expected date-range labels in the
  #'   desired output order (e.g. from \code{date_range$label}).
  #' @param all_keys Character vector of all key/identifier column names
  #'   across every geography (e.g. the union of \code{keys_geoid} and
  #'   \code{keys_zcta}). These are excluded from the label scan so that
  #'   columns such as \code{"geoid"} or \code{"year"} are never mistaken
  #'   for metric columns.
  #' @param con A live \code{DBI} connection to a DuckDB instance, opened
  #'   and closed by the caller so that all three pre-flight checks can
  #'   share a single connection.
  #'
  #' @return Invisibly returns a named list (one element per geography tag)
  #'   of character vectors containing the unique date labels found in that
  #'   geography's column names. Returned invisibly so routine use does not
  #'   clutter the console; assign the result if you need it for later
  #'   inspection (e.g. \code{preflight_a_log <- preflight_a_date_labels(...)}).
  #'
  #' @section Side effects:
  #'   Prints a per-geography status table to the console. Calls
  #'   \code{stop()} if any mismatch is detected, halting execution before
  #'   any output files are written.
  #'
  #' @seealso \code{\link{preflight_b_metric_pairs}},
  #'   \code{\link{preflight_c_id_lengths}}
  
  cat("\n", strrep("=", 60), "\n")
  cat("  PRE-FLIGHT A: Validating date labels across all folders\n")
  cat(strrep("=", 60), "\n")
  
  # -- 1. Collect found labels --------------------------------
  # For each geography, read the merged column schema across all
  # parquet chunks (union_by_name handles differing chunk schemas),
  # keep only columns that contain a double-underscore separator and
  # are not key/identifier columns, then extract the portion after the
  # LAST double-underscore -- that is the date label.
  found_labels <- lapply(geo_levels, function(geo) {
    cols <- dbGetQuery(con, sprintf(
      "DESCRIBE SELECT * FROM read_parquet('%s/*.parquet', union_by_name = true);",
      geo$in_folder
    ))$column_name
    
    # TRUE for metric columns of the form  <metric>__<date_label>;
    # FALSE for key columns and any column without a separator.
    has_sep <- grepl("__(?!.*__)", cols, perl = TRUE) & !(cols %in% all_keys)
    
    # Strip everything up to and including the last "__".
    unique(sub("^.*__", "", cols[has_sep]))
  })
  names(found_labels) <- sapply(geo_levels, `[[`, "tag")
  
  # -- 2. Build aggregate sets --------------------------------
  # Union of all labels found across every geography folder.
  all_found             <- sort(unique(unlist(found_labels)))
  expected              <- sort(date_order)
  
  # Labels in date_order that were not found anywhere in the folders.
  in_expected_not_found <- setdiff(expected,  all_found)
  
  # Labels found in folders that are not in date_order.
  in_found_not_expected <- setdiff(all_found, expected)
  
  cat("\n  date_range labels expected :", length(expected),  "\n")
  cat(  "  Unique labels found        :", length(all_found), "\n")
  
  # -- 3. Per-geography status report -------------------------
  # Report OK / MISMATCH for each folder individually so the user can
  # immediately see which geography has the problem rather than only
  # seeing the aggregate failure.
  for (tag in names(found_labels)) {
    geo_labels   <- sort(found_labels[[tag]])
    missing_here <- setdiff(expected,   geo_labels)   # in date_order, not in folder
    extra_here   <- setdiff(geo_labels, expected)     # in folder, not in date_order
    status       <- if (length(missing_here) == 0 && length(extra_here) == 0) "OK" else "MISMATCH"
    
    cat(sprintf("  %-12s  labels found: %3d  [%s]\n", tag, length(geo_labels), status))
    
    if (length(missing_here) > 0)
      cat("    Missing from folder       :", paste(missing_here, collapse = ", "), "\n")
    if (length(extra_here)   > 0)
      cat("    Extra (not in date_range) :", paste(extra_here,   collapse = ", "), "\n")
  }
  
  # -- 4. Hard stop on any mismatch ---------------------------
  # message() is used (not cat) so the error text goes to stderr and
  # remains visible even when stdout is redirected to a log file.
  if (length(in_expected_not_found) > 0 || length(in_found_not_expected) > 0) {
    if (length(in_expected_not_found) > 0)
      message("\n  MISSING from all folders (in date_range but not found):\n  ",
              paste(in_expected_not_found, collapse = ", "))
    if (length(in_found_not_expected) > 0)
      message("\n  EXTRA labels found (not in date_range):\n  ",
              paste(in_found_not_expected, collapse = ", "))
    stop("Pre-flight A failed: date label mismatch. Resolve before compiling.")
  }
  
  cat("\n  Pre-flight A PASSED: all date labels match date_range$label.\n")
  
  # Return the per-geography label lists invisibly so the caller can
  # inspect them if needed without printing anything automatically.
  invisible(found_labels)
}




preflight_b_metric_pairs <- function(geo_levels, metric_order, date_order, con) {
  #' Validate metric x date column pairs across all input parquet folders 
  #' (Pre-Flight B). For each geography, reads the merged column schema from all 
  #' input parquet chunks and verifies that every combination of 
  #' \code{metric_order} and \code{date_order} is represented as exactly one 
  #' column, named in the form \code{<metric>__<date_label>}. Three distinct 
  #' problems are detected:
  #'
  #' \describe{
  #'   \item{Missing pairs}{A \code{metric x date_label} combination expected
  #'     from the \code{metric_order} x \code{date_order} grid has no
  #'     corresponding column in the folder. This would produce \code{NA}-filled
  #'     columns or silent data loss in the compiled output.}
  #'   \item{Duplicate pairs}{The same \code{metric x date_label} combination
  #'     appears more than once as a column name. This would cause ambiguity in
  #'     the downstream \code{SELECT} and is treated as a hard failure.}
  #'   \item{Partial metrics}{A metric is present for some date labels but not
  #'     all. These are a subset of missing pairs and are reported separately as
  #'     an informational diagnostic to help distinguish a fully absent metric
  #'     from one that was only partially computed.}
  #' }
  #'
  #' Missing pairs and duplicates both trigger a hard stop; partial metrics are
  #' informational only and do not independently stop execution (they will
  #' already be captured by the missing-pairs check).
  #'
  #' @param geo_levels A named list of geography definitions. Each element
  #'   must contain:
  #'   \describe{
  #'     \item{\code{tag}}{Character. Short geography name (e.g.
  #'       \code{"tract"}).}
  #'     \item{\code{in_folder}}{Character. Path to the folder containing
  #'       the input \code{.parquet} chunks for this geography.}
  #'     \item{\code{keys}}{Character vector. Key/identifier column names
  #'       for this geography (e.g. \code{c("geoid", "year", "religion",
  #'       "geoid_pop", "geoid_sqMiles")}). These are excluded from the
  #'       metric column scan.}
  #'   }
  #' @param metric_order Character vector of expected metric names in the
  #'   desired output order. Every element must appear as the prefix (before
  #'   the last \code{__}) of at least one column in every folder.
  #' @param date_order Character vector of expected date-range labels in the
  #'   desired output order (e.g. from \code{date_range$label}). Every
  #'   element must appear as the suffix (after the last \code{__}) of at
  #'   least one column per metric in every folder.
  #' @param con A live \code{DBI} connection to a DuckDB instance, opened
  #'   and closed by the caller so that all three pre-flight checks can
  #'   share a single connection.
  #'
  #' @return Invisibly returns a list with one element per geography,
  #'   each containing:
  #'   \describe{
  #'     \item{\code{tag}}{Character. Geography tag.}
  #'     \item{\code{n_expected}}{Integer. Total expected pairs:
  #'       \code{length(metric_order) * length(date_order)}.}
  #'     \item{\code{n_present}}{Integer. Unique \code{metric x date_label}
  #'       pairs actually found in the folder's column schema.}
  #'     \item{\code{missing_pairs}}{Data frame of \code{metric} and
  #'       \code{label} columns listing every expected pair not found.
  #'       Zero rows on pass.}
  #'     \item{\code{duplicate_pairs}}{Data frame of all rows (including
  #'       both copies) for any duplicated \code{metric x date_label}
  #'       combination. Zero rows on pass.}
  #'     \item{\code{partial_metrics}}{Data frame of metrics present for
  #'       at least one but fewer than \code{length(date_order)} date
  #'       labels. Informational; zero rows when coverage is complete.}
  #'     \item{\code{ok}}{Logical. \code{TRUE} if no missing pairs and no
  #'       duplicates were found for this geography.}
  #'   }
  #'
  #' @section Side effects:
  #'   Prints a per-geography summary line and, on failure, a compact
  #'   breakdown of missing pairs grouped by metric with the specific
  #'   missing date labels listed inline. Calls \code{stop()} if any
  #'   geography has missing pairs or duplicates, halting execution before
  #'   any output files are written.
  #'
  #' @seealso \code{\link{preflight_a_date_labels}},
  #'   \code{\link{preflight_c_id_lengths}}
  
  cat("\n", strrep("=", 60), "\n")
  cat("  PRE-FLIGHT B: Validating metric x date pairs per folder\n")
  cat(strrep("=", 60), "\n")
  
  # Accumulator for per-geography results, and a global pass/fail flag.
  # all_ok is flipped to FALSE on the first failing geography but the
  # loop continues so every geography's issues are reported in one pass
  # rather than stopping at the first failure.
  log    <- vector("list", length(geo_levels))
  all_ok <- TRUE
  
  for (i in seq_along(geo_levels)) {
    
    tag       <- geo_levels[[i]]$tag
    in_folder <- geo_levels[[i]]$in_folder
    keys      <- geo_levels[[i]]$keys
    
    # -- 1. Read column schema --------------------------------
    # union_by_name merges schemas across chunks so columns that appear
    # in only some chunks are still captured in the DESCRIBE output.
    cols <- dbGetQuery(con, sprintf(
      "DESCRIBE SELECT * FROM read_parquet('%s/*.parquet', union_by_name = true);",
      in_folder
    ))$column_name
    
    # Identify metric columns: must contain a double-underscore separator
    # and must not be a key/identifier column.
    is_key  <- cols %in% keys
    has_sep <- grepl("__(?!.*__)", cols, perl = TRUE) & !is_key
    
    # -- 2. Build present-pairs table -------------------------
    # Split each metric column name into its metric prefix (everything
    # before the last "__") and its date label suffix (everything after
    # the last "__"). Retain the original column name for duplicate
    # reporting.
    present_pairs <- data.frame(
      metric = sub("__(?!.*__).*", "", cols[has_sep], perl = TRUE),
      label  = sub("^.*__",        "", cols[has_sep]),
      col    = cols[has_sep],
      stringsAsFactors = FALSE
    )
    
    # -- 3. Detect duplicates ---------------------------------
    # A duplicated metric x date pair means the same combination appears
    # under two different column names, which would create ambiguity in
    # the downstream SELECT.  Both copies are retained in dup_pairs so
    # the user can see exactly which column names are clashing.
    dup_pairs <- present_pairs[
      duplicated(present_pairs[, c("metric","label")]) |
        duplicated(present_pairs[, c("metric","label")], fromLast = TRUE), ]
    
    # -- 4. Detect missing pairs ------------------------------
    # Build the full expected grid and left-join the found pairs onto it.
    # Any row surviving with _merge == "left_only" is a missing pair.
    expected_pairs <- expand.grid(
      metric = metric_order, label = date_order, stringsAsFactors = FALSE)
    
    missing_pairs <- merge(
      expected_pairs, unique(present_pairs[, c("metric","label")]),
      by = c("metric","label"), all.x = TRUE, indicator = TRUE
    )
    missing_pairs <- missing_pairs[
      missing_pairs$`_merge` == "left_only", c("metric","label")]
    
    # -- 5. Detect partial metrics (informational) ------------
    # A metric is "partial" if it is present for at least one date label
    # but fewer than the full set.  These will already appear in
    # missing_pairs; surfacing them separately makes it easier to
    # distinguish a metric that was never computed from one that was
    # only partially computed.
    n_dates_per_metric <- aggregate(
      label ~ metric,
      data = unique(present_pairs[, c("metric","label")]),
      FUN  = function(x) sum(x %in% date_order)
    )
    names(n_dates_per_metric)[2] <- "n_dates"
    partial_metrics <- n_dates_per_metric[
      n_dates_per_metric$n_dates > 0 &
        n_dates_per_metric$n_dates < length(date_order), ]
    
    # -- 6. Summary line --------------------------------------
    geo_ok <- nrow(missing_pairs) == 0 && nrow(dup_pairs) == 0
    if (!geo_ok) all_ok <- FALSE
    
    n_exp  <- length(metric_order) * length(date_order)
    n_pres <- nrow(unique(present_pairs[, c("metric","label")]))
    
    cat(sprintf(
      "\n  %-12s  expected: %4d  |  found: %4d  |  missing: %3d  |  dupes: %3d  [%s]\n",
      tag, n_exp, n_pres, nrow(missing_pairs), nrow(dup_pairs),
      if (geo_ok) "OK" else "FAIL"
    ))
    
    # -- 7. Detailed failure output ---------------------------
    # Missing pairs: grouped by metric to keep output compact when many
    # date labels are missing for the same metric, with the specific
    # missing labels listed inline for surgical diagnosis.
    if (nrow(missing_pairs) > 0) {
      miss_by_metric <- aggregate(label ~ metric, data = missing_pairs, FUN = length)
      names(miss_by_metric)[2] <- "n_dates_missing"
      
      # Sort by metric_order so the output matches the expected schema order.
      miss_by_metric <- miss_by_metric[order(match(miss_by_metric$metric, metric_order)), ]
      cat("    Metrics with missing dates:\n")
      
      for (j in seq_len(nrow(miss_by_metric))) {
        these_dates <- missing_pairs$label[
          missing_pairs$metric == miss_by_metric$metric[j]]
        # Sort missing dates by date_order for readability.
        these_dates <- these_dates[order(match(these_dates, date_order))]
        cat(sprintf("      %-35s  missing %d: %s\n",
                    miss_by_metric$metric[j],
                    miss_by_metric$n_dates_missing[j],
                    paste(these_dates, collapse = ", ")))
      }
    }
    
    # Duplicate columns: print the full rows so the user can see the
    # exact column names that are clashing.
    if (nrow(dup_pairs) > 0) {
      cat("    Duplicate metric x date columns:\n")
      print(dup_pairs, row.names = FALSE)
    }
    
    # Partial metrics: informational only, does not affect geo_ok.
    if (nrow(partial_metrics) > 0) {
      cat("    Metrics present for only some dates (informational):\n")
      print(partial_metrics[order(match(partial_metrics$metric, metric_order)), ],
            row.names = FALSE)
    }
    
    # -- 8. Store per-geography result ------------------------
    log[[i]] <- list(
      tag             = tag,
      n_expected      = n_exp,
      n_present       = n_pres,
      missing_pairs   = missing_pairs,
      duplicate_pairs = dup_pairs,
      partial_metrics = partial_metrics,
      ok              = geo_ok
    )
  }
  
  # -- 9. Hard stop after full loop -------------------------
  # Stopping after the loop (rather than inside it) ensures all five
  # geographies are reported before execution halts, so the user can
  # fix all issues in one round rather than discovering them one by one.
  if (!all_ok)
    stop("Pre-flight B failed: metric x date pair issues found. Resolve before compiling.")
  
  cat("\n  Pre-flight B PASSED: all metric x date pairs present in every folder.\n")
  invisible(log)
}




preflight_c_id_lengths <- function(geo_levels, con) {
  #' Validate ID column character lengths across all input parquet folders 
  #' (Pre-Flight C). For each geography, queries the full distribution of 
  #' character lengths present in the identifier column (\code{geoid} or 
  #' \code{zcta}) across all input parquet chunks and confirms that every value 
  #' is exactly the expected Census Bureau length for that geography level:
  #'
  #' \tabular{ll}{
  #'   \strong{Geography} \tab \strong{Expected length} \cr
  #'   block_group        \tab 12 \cr
  #'   tract              \tab 11 \cr
  #'   county             \tab  5 \cr
  #'   state              \tab  2 \cr
  #'   zcta               \tab  5 \cr
  #' }
  #'
  #' These lengths reflect standard Census FIPS/GEOID encoding where each
  #' component is zero-padded to a fixed width (e.g. a block group GEOID is
  #' composed of 2-digit state + 3-digit county + 6-digit tract + 1-digit
  #' block group = 12 characters total). Any deviation -- including IDs that
  #' are too short due to a missing leading zero -- is treated as a hard
  #' failure, because malformed IDs would corrupt the division-filter logic
  #' (\code{substring(geoid, 1, 2)}) and any downstream spatial joins.
  #'
  #' The full length distribution is reported on failure (not just a count
  #' of bad rows) so the user can distinguish, for example, a handful of
  #' truncated IDs from a systematic encoding error affecting a whole state.
  #'
  #' @param geo_levels A named list of geography definitions. Each element
  #'   must contain:
  #'   \describe{
  #'     \item{\code{tag}}{Character. Short geography name (e.g.
  #'       \code{"block_group"}).}
  #'     \item{\code{in_folder}}{Character. Path to the folder containing
  #'       the input \code{.parquet} chunks for this geography.}
  #'     \item{\code{id_col}}{Character. Name of the identifier column to
  #'       validate: \code{"geoid"} for all Census geographies,
  #'       \code{"zcta"} for ZIP Code Tabulation Areas.}
  #'     \item{\code{id_len}}{Integer. The exact character length every
  #'       value in \code{id_col} must have for this geography.}
  #'   }
  #' @param con A live \code{DBI} connection to a DuckDB instance, opened
  #'   and closed by the caller so that all three pre-flight checks can
  #'   share a single connection.
  #'
  #' @return Invisibly returns a list with one element per geography,
  #'   each containing:
  #'   \describe{
  #'     \item{\code{tag}}{Character. Geography tag.}
  #'     \item{\code{id_col}}{Character. Name of the validated identifier
  #'       column.}
  #'     \item{\code{id_len}}{Integer. The expected character length.}
  #'     \item{\code{total_rows}}{Integer. Total row count across all input
  #'       chunks for this geography.}
  #'     \item{\code{bad_row_n}}{Integer. Number of rows whose identifier
  #'       is not exactly \code{id_len} characters. Zero on pass.}
  #'     \item{\code{len_dist}}{Data frame with columns \code{id_length},
  #'       \code{n_rows}, and \code{n_distinct_ids} showing the full
  #'       distribution of observed identifier lengths. Useful for
  #'       diagnosing the nature and scale of any encoding problem.}
  #'     \item{\code{bad_samples}}{Character vector of up to 10 example
  #'       identifier values whose length differs from \code{id_len}.
  #'       Zero-length character vector on pass.}
  #'     \item{\code{ok}}{Logical. \code{TRUE} if all identifier values
  #'       are exactly \code{id_len} characters.}
  #'   }
  #'
  #' @section Side effects:
  #'   Prints a one-line summary per geography. On failure, also prints the
  #'   full length distribution table with unexpected lengths flagged, and a
  #'   sample of up to 10 offending identifier values. Calls \code{stop()}
  #'   after completing the loop for all geographies if any failure was
  #'   detected, so all issues are visible in a single run before execution
  #'   halts.
  #'
  #' @seealso \code{\link{preflight_a_date_labels}},
  #'   \code{\link{preflight_b_metric_pairs}}
  
  cat("\n", strrep("=", 60), "\n")
  cat("  PRE-FLIGHT C: Validating ID column lengths\n")
  cat(strrep("=", 60), "\n")
  
  # Accumulator for per-geography results, and a global pass/fail flag.
  # all_ok is flipped to FALSE on the first failing geography but the
  # loop continues so every geography's issues are visible in one run.
  log    <- vector("list", length(geo_levels))
  all_ok <- TRUE
  
  for (i in seq_along(geo_levels)) {
    
    tag       <- geo_levels[[i]]$tag
    in_folder <- geo_levels[[i]]$in_folder
    id_col    <- geo_levels[[i]]$id_col
    id_len    <- geo_levels[[i]]$id_len
    
    # -- 1. Full length distribution --------------------------
    # Groups every row by the character length of its identifier so
    # the output shows the complete picture of what is present, not
    # just whether any bad rows exist.  n_distinct_ids is included so
    # the user can see whether bad-length rows represent a small number
    # of systematically malformed IDs or a large, varied population.
    len_dist <- dbGetQuery(con, sprintf("
      SELECT
        length(%s)         AS id_length,
        COUNT(*)::BIGINT    AS n_rows,
        COUNT(DISTINCT %s)  AS n_distinct_ids
      FROM read_parquet('%s/*.parquet', union_by_name = true)
      GROUP BY id_length
      ORDER BY id_length;
    ", quote_ident(id_col), quote_ident(id_col), in_folder))
    
    # Rows in the distribution whose length deviates from the expected.
    bad_rows <- len_dist[len_dist$id_length != id_len, ]
    
    # -- 2. Sample of bad identifier values -------------------
    # Only executed when bad rows exist. Fetches up to 10 distinct
    # offending values so the user can immediately see whether the
    # problem is a missing leading zero, a wrong geography mixed into
    # the folder, a NULL cast to an empty string, etc.
    bad_samples <- if (nrow(bad_rows) > 0) {
      dbGetQuery(con, sprintf("
        SELECT DISTINCT %s AS id_value
        FROM read_parquet('%s/*.parquet', union_by_name = true)
        WHERE length(%s) != %d
        LIMIT 10;
      ", quote_ident(id_col), in_folder, quote_ident(id_col), id_len))$id_value
    } else {
      character(0)
    }
    
    # -- 3. Summarise and flag --------------------------------
    total_rows <- sum(len_dist$n_rows)
    bad_row_n  <- sum(bad_rows$n_rows)   # 0 when bad_rows is empty
    geo_ok     <- bad_row_n == 0L
    if (!geo_ok) all_ok <- FALSE
    
    cat(sprintf(
      "\n  %-12s  id_col: %-6s  expected_len: %2d  total_rows: %9d  bad_rows: %7d  [%s]\n",
      tag, id_col, id_len, total_rows, bad_row_n, if (geo_ok) "OK" else "FAIL"
    ))
    
    # -- 4. Detailed failure output ---------------------------
    # Print the full distribution so the scale and pattern of the
    # problem is clear.  Flag unexpected lengths inline with an arrow
    # so they are easy to spot when the table is long.
    if (!geo_ok) {
      cat("    Length distribution found:\n")
      for (r in seq_len(nrow(len_dist))) {
        cat(sprintf(
          "      length %2d : %9d rows  (%d distinct ids)%s\n",
          len_dist$id_length[r],
          len_dist$n_rows[r],
          len_dist$n_distinct_ids[r],
          if (len_dist$id_length[r] != id_len) "  <-- UNEXPECTED" else ""
        ))
      }
      # A small concrete sample is often enough to diagnose the root
      # cause without the user needing to open the parquet files.
      cat("    Sample of unexpected id values (up to 10):\n")
      cat("     ", paste(bad_samples, collapse = "\n      "), "\n")
    }
    
    # -- 5. Store per-geography result ------------------------
    log[[i]] <- list(
      tag         = tag,
      id_col      = id_col,
      id_len      = id_len,
      total_rows  = total_rows,
      bad_row_n   = bad_row_n,
      len_dist    = len_dist,    # full distribution; useful even on pass
      bad_samples = bad_samples, # empty character(0) on pass
      ok          = geo_ok
    )
  }
  
  # -- 6. Hard stop after full loop -------------------------
  # Stopping after the loop ensures all five geographies are reported
  # before execution halts, consistent with Pre-Flights A and B.
  if (!all_ok)
    stop("Pre-flight C failed: unexpected ID lengths found. Resolve before compiling.")
  
  cat("\n  Pre-flight C PASSED: all ID values are the expected length.\n")
  
  # Return the log invisibly so the caller can assign and inspect it
  # (e.g. preflight_c_log[[1]]$len_dist) without console noise.
  invisible(log)
}




