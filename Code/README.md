**Author:** Shelby Golden, M.S.

**Date Created:** July 22<sup>nd</sup>, 2026

**Date Updated:** July 22<sup>nd</sup>, 2026

**Purpose:**

Provides an overview of the contents of the `Code/` directory and documents any associated special notes or considerations.

**How to Use:**

Each script used to review or process the Data Axle data is prefixed by a descriptor that denotes the step it applies to:

-   **Explore the Raw Data:** Exploratory data analysis of the first raw data provided — the 2023 version, `church_wide_form_071723.csv`. A coarse evaluation to identify anomalies and features of the dataset to be considered during the data cleaning and validation steps.

-   **Process Data Update:** Extension of the "Explore the Raw Data" phase using the updated version provided in May 2026, `church_long_form_050926.csv`.

-   **Clean Raw Data_Step \*:** Each discrete stepwise procedure determined to be necessary during the "Explore the Raw Data" phase. Some scripts include additional classifiers indicating whether they were designed for use on the High Performance Computer (HPC) and whether they apply to the 2023 or 2026 version of the Data Axle raw data.

-   **Generate the Metrics:** Script that uses the final output of the "Clean Raw Data" phase to generate the metrics visualized on the map, which includes a user-input date range slider feature.

**Directory Specific Notes:**

1. The `Support Functions/` directory contains all custom functions used across respective scripts. Some are step-specific, while others are generalized and may be used across multiple scripts.

2.  `"USPS SLURM_2023 Format.sh"`" is the preconfigured batch array shell script used for deploying batch arrays for `"Clean Raw Data_Step 2 HPC_2023 Format.R"`.

3.  `"Validation SLURM_2026 Format.sh"`" is the preconfigured batch array shell script used for deploying batch arrays for `"Clean Raw Data_Step 2 HPC v2_2026 Format.R"`. It is also compatible with `"Clean Raw Data_Step 2 HPC v1_2026 Format.R"` with minor file reference changes.