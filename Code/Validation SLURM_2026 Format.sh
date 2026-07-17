#!/bin/bash

# This array job is for Step 2 of the 2026 Format. It processes a maximum of
# 20 chunks at once, assuming a partition of all entries associated with 5,000
# unique ABIs per array (number of indices will vary).
#
# Adjust "X" in `#SBATCH --array=1-X%20` as needed based on the length of
# processed_indices under "SUBSECTION B1: Index Queue" in
# "Clean Raw Data_Step 2 HPC v2_2026 Format.R".
#
# Follow the steps provided in the header and "PART A: UTILIZING THE HPC" of
# "Clean Raw Data_Step 2 HPC v2_2026 Format.R" to configure the HPC files and
# environment prior to running.

#SBATCH --job-name=validation
#SBATCH --partition=week
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=20G
#SBATCH --array=1-15%10
#SBATCH --chdir=/home/sg2736/project_pi_bm895/sg2736/church_closures
#SBATCH --output=/home/sg2736/project_pi_bm895/sg2736/church_closures/Logs/%x_%A_%a.out
#SBATCH --error=/home/sg2736/project_pi_bm895/sg2736/church_closures/Logs/%x_%A_%a.err

set -euo pipefail

module reset
module load R/4.4.2-gfbf-2024a
module load ICU/75.1-GCCcore-13.3.0

export LD_LIBRARY_PATH="$(R RHOME)/lib:${LD_LIBRARY_PATH:-}"

BASE_DIR="/home/sg2736/project_pi_bm895/sg2736/church_closures"
export R_ENVIRON_USER="$BASE_DIR/.Renviron"
export R_RENVLOCK_USER="$BASE_DIR/renv.lock"

OUTDIR="$BASE_DIR/Results"

mkdir -p \
  "$BASE_DIR/Logs" \
  "$OUTDIR" \
  "$OUTDIR/Verified Result" \
  "$OUTDIR/Address QC" \
  "$OUTDIR/Geo QC" \
  "$OUTDIR/Census QC"

Rscript -e 'renv::activate(project="'"${BASE_DIR}"'"); source("'"${BASE_DIR}"'/Clean Raw Data_Step 2 HPC v2_2026 Format.R")' \
  "$OUTDIR" "$SLURM_ARRAY_TASK_ID"