#!/bin/bash

# Array job that processes 29 chunks (max 8 running at once) for the Step 2
# using the 2023 data format. Follow the steps provided in the header and
# "SUBSECTION A1: Utilizing the HPC" of "Clean Raw Data_Step 2 HPC_2023 Format.R"
# to configure the HPC files and environment.

#SBATCH --job-name=church_array      # Job name shown in squeue
#SBATCH --partition=day              # Queue/partition
#SBATCH --time=07:00:00              # Walltime limit
#SBATCH --cpus-per-task=1            # CPU cores per task
#SBATCH --mem=2G                     # Memory per task
#SBATCH --array=1-29%8               # Tasks 1..29, throttle to 8 concurrent
#SBATCH --chdir=/home/sg2736/project_pi_bm895/sg2736/church-closures                     # Working dir
#SBATCH --output=/home/sg2736/project_pi_bm895/sg2736/church-closures/Logs/%x_%A_%a.out  # Stdout
#SBATCH --error=/home/sg2736/project_pi_bm895/sg2736/church-closures/Logs/%x_%A_%a.err   # Stderr

set -euo pipefail

module --force purge
module load R/4.4.2-gfbf-2024a-bare

export LD_LIBRARY_PATH="$(R RHOME)/lib:${LD_LIBRARY_PATH:-}"

SCRIPT_DIR="/home/sg2736/project_pi_bm895/sg2736/church-closures"
mkdir -p "${SCRIPT_DIR}/Logs"

export R_ENVIRON_USER="/home/sg2736/project_pi_bm895/sg2736/church-closures/.Renviron"

OUTBASE="/home/sg2736/project_pi_bm895/sg2736/church-closures"
OUTDIR="${OUTBASE}/USPS_Validation_Results"
mkdir -p "$OUTDIR"

Rscript -e 'renv::restore(project="'"${SCRIPT_DIR}"'", prompt=FALSE); source("'"${SCRIPT_DIR}"'/Clean Raw Data_Step 2 HPC_2023 Format.R")' \
  "USPS_Validation_Results" "$SLURM_ARRAY_TASK_ID"