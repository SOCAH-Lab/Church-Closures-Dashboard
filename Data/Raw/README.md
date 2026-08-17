**Author:** Shelby Golden, M.S.

**Date Created:** May 19<sup>th</sup>, 2026

**Date Updated:** July 6<sup>th</sup>, 2026

**Purpose:**

Provides an overview of the contents of the `Data/Raw` directory and documents any associated special notes or considerations.

**About:**

Throughout the raw data assessment and data cleaning and validation steps, zip codes were occasionally matched to their most likely associated city using the SimpleMaps [United States Cities Database](https://simplemaps.com/data/us-cities). This was particularly important for the 2023 Format data, where leading and trailing zeros had been stripped from zip code values. This association also served as a fallback within the USPS 3.0 API algorithm (**Step 2** for the 2023 Format and **Step 2 LOOP PART B** for the 2026 Format) where failed address validation attempts were retried using the preferred city for the given zip code.

During the data cleaning and validation steps (**Step 5** for the 2023 Format and **Step 2 LOOP PART D** for the 2026 Format) longitude and latitude coordinates were used to assign census boundary designations across the 2000, 2010, and 2020 Decennial Census years. The initial implementation of this spatial assignment method used the `tigris` R package to retrieve decennial-year shapefiles directly from the U.S. Census Bureau's [TIGER/Line Shapefile](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) database. This approach, however, did not scale effectively and introduced processing issues with the 2000 and 2020 decennial years.

To improve performance and accuracy in preparation for migration to the Yale High Performance Computing (HPC) environment, the relevant TIGER/Line Shapefiles were manually downloaded and preprocessed. Each shapefile was structured to include the desired metadata by decennial year as discrete layers. The pipeline supports both block-level and block group-level shapefiles: both contain polygon boundaries that associate block or block group, tract, county, and state codes with any longitude/latitude coordinate falling within a given polygon. Block group-level shapefiles are preferred for generating summary statistics due to their reduced complexity compared to block-level shapefiles.

In the 2026 Format workflow, two additional geographic codes representing Core Based Statistical Areas were associated using the same method: Metropolitan/Micropolitan Statistical Areas (denoted as CBSA) and Combined Statistical Areas (CSA). These designations were first introduced in 2003, replacing the Office of Management and Budget's (OMB) prior use of "Standard Metropolitan Areas." CBSA and CSA boundaries are typically updated annually, every five years at mid-decade, and following each decennial census. For the purposes of this processing, the 2010 and 2020 decennial releases were used alongside the earliest available annual release from the 2000s decade (the 2007 vintage) to provide coverage across all relevant time periods.

Additionally, ZIP Code Tabulation Areas (ZCTA) national files were obtained for each decennial year and compiled alongside the CBSA/CSA output. To speed up processing, the state acronym is joined during preparation of the compiled core areas file (containing the combined CBSA/CSA and ZCTA decennial details as layers).

**How to Use:**

Raw data are loaded in their respective scripts. Reference the "LOAD IN THE DATA" section at the beginning of each script in the `Code/` directory to identify which datasets are used.

Six types of census boundary shapefiles are used: state block-level, state block group-level, national CBSA, national CSA, national ZCTA, and national states (and equivalents). Each was obtained for the 2000, 2010, and 2020 decennial vintages, except CBSA and CSA, for which the 2007 vintage was substituted for 2000. These shapefiles were processed and compiled as GeoPackage (`*.gpkg`) files in `Data/Results/Census Bureau TIGER Line Shapefiles/`, with state block- and block group-level data organized as layers by decennial year. CBSA, CSA, and ZCTA data (annotated with the state acronym) were compiled into a single file, with one layer per census boundary type and decennial year combination.

All census boundary files, both the downloaded raw shapefiles and the compiled GeoPackage versions, are too large for effective Git tracking and distribution. New users will need to download the source files independently and generate the GeoPackage files locally. Refer to the notes below for download instructions and links to sources. For the coded implementation, refer to "SUBSECTION A3: Build Precompiled TIGER/Line GeoPackages" in `Clean Raw Data_Step 2_2026 Format.R`.

**Directory Specific Notes:**

1.  Only data that are anonymized, summarized such that they do not contain individual-level data, or public are accessible through GitHub and tracked by Git. A copy of the `KEEP LOCAL` data has been uploaded to the SOCAH Lab OneDrive for this project.

2.  `/simplemaps_uscities_basicv1.90/` was downloaded June/July 2025

3.  `/simplemaps_uscities_basicv1.93/` was downloaded July 1<sup>st</sup>, 2026

4.  `/Census Bureau TIGER Line Shapefiles/` contents were downloaded June/July 2026

    -   Downloaded block-level TIGER/Line shapefiles for every state, where "XX" in the filename denotes the state FIPS code: `tl_2010_XX_tabblock00.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=Blocks)); `tl_2010_XX_tabblock10.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=Blocks)); `tl_2020_XX_tabblock20.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=Blocks+%282020%29)).

    -   Downloaded block group-level TIGER/Line shapefiles for every state, where "XX" in the filename denotes the state FIPS code: `tl_2010_XX_bg00.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=Block+Groups)); `tl_2010_XX_bg10.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=Block+Groups)); `tl_2020_XX_bg.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=Block+Groups)).

    -   Downloaded national-level Metropolitan/Micropolitan Statistical Area (CBSA) and Combined Statistical Area (CSA) files: `fe_2007_us_cbsa.zip` and `fe_2007_us_csa.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2007&layergroup=Core+Based+Statistical+Areas)); `tl_2010_us_cbsa10.zip` and `tl_2010_us_csa10.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=Core+Based+Statistical+Areas)); `tl_2020_us_cbsa.zip` and `tl_2020_us_csa.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=Core+Based+Statistical+Areas)).

    -   Downloaded national-level ZIP Code Tabulation Areas (ZCTA) files: `tl_2010_us_zcta500.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=ZIP+Code+Tabulation+Areas)); `tl_2010_us_zcta510.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=ZIP+Code+Tabulation+Areas)); `tl_2020_us_zcta520.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=ZIP+Code+Tabulation+Areas)).

    -   Downloaded national-level States (and equivalent) files for annotating ZCTA: `tl_2010_us_state00.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=States+%28and+equivalent%29)); `tl_2010_us_state10.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2010&layergroup=States+%28and+equivalent%29)); `tl_2020_us_state20.zip` ([source](https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=States+%28and+equivalent%29)).

5.  `bg00/tabblock00` vs `bg10/tabblock10` denotes the decennial block system (IDs/definitions); `tl_2010` denotes the processing/packaging vintage. As a result, 2000 geometries may differ from their original distribution; the raw 2000 shapefiles were unavailable, and rich text alternatives showed signs of corruption (e.g., New Haven, CT). These variations are more likely to affect block and tract levels than county or higher.

**References:**

1.  US Census Bureau, “TIGER/Line Shapefiles Database,” TIGER/Line Shapefiles. Accessed: Jul. 01, 2026. \[Online\]. Available: https://www.census.gov/cgi-bin/geo/shapefiles/index.php

2.  US Census Bureau, “TIGER/Line Shapefiles,” Census.gov. Accessed: Jul. 01, 2026. \[Online\]. Available: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

3.  US Census Bureau, “Metropolitan and Micropolitan Statistical Area Glossary,” Census.gov. Accessed: Jul. 01, 2026. \[Online\]. Available: https://www.census.gov/programs-surveys/metro-micro/about/glossary.html

4.  U.S. Department of Commerce, Economics and Statistics Administration, and US Census Bureau, “Chapter 11: Census Blocks and Block Groups,” in *Geographic Areas Reference Manual (GARM)*, 1994. Accessed: Jul. 06, 2026. \[E-book\]. Available: https://www2.census.gov/geo/pdfs/reference/GARM/

5.  K. Walker and B. Rudis, tigris: Load Census TIGER/Line Shapefiles. (Apr. 16, 2025). Accessed: Jul. 01, 2026. \[Online\]. Available: https://cran.r-project.org/web/packages/tigris/index.html

6.  SimpleMaps, “United States Cities Database.” Accessed: Jul. 01, 2026. \[Online\]. Available: https://simplemaps.com/data/us-cities

7.  T. R. Knoedl, Library of Congress, and US Congress, “Core Based Statistical Areas,” Congressional Research Service (CRS) Products. Accessed: Jul. 01, 2026. \[Online\]. Available: https://www.congress.gov/crs-product/IF12704

8.  Executive Office of the President and Office of Management and Budget (OMB), “Revised Definitions of Metropolitan Statistical Areas, New Definitions of Micropolitan Statistical Areas and Combined Statistical Areas, and Guidance on Uses of the Statistical Definitions of These Areas.” Washington, D.C. 20503, Jun. 06, 2023. Accessed: Jul. 01, 2026. \[Online\]. Available: https://www.whitehouse.gov/wp-content/uploads/2017/11/bulletins_b03-04.pdf
