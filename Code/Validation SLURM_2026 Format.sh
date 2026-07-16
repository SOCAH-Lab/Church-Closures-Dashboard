#!/bin/bash

# Array job that processes max 8 chucks at once for Step 2 using the 2026 Format. 
# Follow the steps provided in the header and "PART A: UTILIZING THE HPC" of 
# "Clean Raw Data_Step 2_2026 Format.R" to configure the HPC files and environment.

#SBATCH --job-name=church_array      # Job name shown in squeue
#SBATCH --partition=day              # Queue/partition
#SBATCH --time=24:00:00              # Walltime limit
#SBATCH --cpus-per-task=1            # CPU cores per task
#SBATCH --mem=5G                     # Memory per task
#SBATCH --array=1-29%8               # Tasks 1..29, throttle to 8 concurrent
#SBATCH --chdir=/home/sg2736/project_pi_bm895/sg2736/church-closures                     # Working dir
#SBATCH --output=/home/sg2736/project_pi_bm895/sg2736/church-closures/Logs/%x_%A_%a.out  # Stdout
#SBATCH --error=/home/sg2736/project_pi_bm895/sg2736/church-closures/Logs/%x_%A_%a.err   # Stderr

set -euo pipefail

module reset
module load R/4.4.2-gfbf-2024a

export LD_LIBRARY_PATH="$(R RHOME)/lib:${LD_LIBRARY_PATH:-}"

BASE_DIR="/home/sg2736/project_pi_bm895/sg2736/church_closures"

export R_ENVIRON_USER="$BASE_DIR/.Renviron"

OUTDIR="$BASE_DIR/Results"

mkdir -p \
  "$BASE_DIR/Logs" \
  "$OUTDIR" \
  "$OUTDIR/Verified Result" \
  "$OUTDIR/Address QC" \
  "$OUTDIR/Geo QC" \
  "$OUTDIR/Census QC"

Rscript -e 'renv::activate(project="'"${BASE_DIR}"'", prompt=FALSE); source("'"${BASE_DIR}"'/Clean Raw Data_Step 2 HPC v2_2026 Format.R")' \
  "$OUTDIR" "$SLURM_ARRAY_TASK_ID"
  
  
  