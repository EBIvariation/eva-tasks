#!/bin/bash

#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=24:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=1   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=8G   # memory per node
#SBATCH -J "pr1"   # job name
#SBATCH -o "pr1.out"   # job output file
#SBATCH -e "pr1.err"   # job error file
export PYTHONDONTWRITEBYTECODE=1
set -euo pipefail
# ==========================================
# Configuration - user input - check these are correct
# ==========================================
# 1. create the EVA task dir
# 2. add the data_dir, tests_dir and TEST.config to that directory
# 3. double check the values in the pipeline.env

STUDY_ACCESSION="${1:-}"

BASE_DIR="$(pwd)"


PIPELINE_PATH="${BASE_DIR}/pipeline.env"
if [[ -f "${PIPELINE_PATH}" ]]; then
	set -a
    source "${PIPELINE_PATH}"
    set +a
else
    echo "Error: Configuration file missing at ${PIPELINE_PATH}" >&2
    exit 1
fi

# ==========================================
# Setup and pre-flight checks
# ==========================================
INPUT_DIR="${BASE_DIR}/data_dir"
#TEST_DIR="${BASE_DIR}/tests"
CONFIG_FILE="${BASE_DIR}/TEST.config"
OUTPUT_DIR="${OUTPUT_DIR:-${BASE_DIR}/output}"
SUBMISSION_DIR="${OUTPUT_DIR}/submission"
mkdir -p "${OUTPUT_DIR}"


SITE_PACKAGES="${CONVERT_GVF_TO_VCF_DIR}/lib/python3.11/site-packages"
FINDER_SCRIPT="${CONVERT_GVF_TO_VCF_DIR}/bin/gvf_file_finder.py"
ENV_ACTIVATE="${EVA_SUB_CLI_DIR}/bin/activate"

echo "Verifying input files and directories..."

# Check data directory is present (this mimics the DGVa FTP)
if [ ! -d "${INPUT_DIR}" ]; then
    echo "ERROR: directory not found - ${INPUT_DIR}" >&2
    exit 1
fi

# Check config file is present
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "ERROR: Config file not found - ${CONFIG_FILE}" >&2
    exit 1
fi

# check pyyaml is installed to parse the config file
echo "Checking Python dependencies..."

if ! python3 -c "import importlib.util; exit(0 if importlib.util.find_spec('yaml') else 1)" &> /dev/null; then
    echo "ERROR: Python library 'PyYAML' is not installed in the active environment." >&2
    echo "Please run 'pip install pyyaml' or activate the correct virtual environment." >&2
    exit 1
fi

# Get webin details from the config file
WEBIN_USER=$(python3 -c "import yaml; cfg = yaml.safe_load(open('$CONFIG_FILE')); print(cfg.get('webin', {}).get('username', ''))")
WEBIN_PASS=$(python3 -c "import yaml; cfg = yaml.safe_load(open('$CONFIG_FILE')); print(cfg.get('webin', {}).get('password', ''))")

if [ -z "${WEBIN_USER}" ] || [ -z "${WEBIN_PASS}" ]; then
    echo "ERROR: Failed to extract username or password from 'webin:' YAML block in ${CONFIG_FILE}" >&2
    exit 1
fi

# ==========================================
# STEP 1: Convert GVF to VCF
# ==========================================
# Gather study accession folderes
if [ -n "${STUDY_ACCESSION}" ]; then
    # User gave a specific study accession as a positional argumnet
    STUDY_FOLDERS=$(find "${INPUT_DIR}" -maxdepth 1 -type d -name "${STUDY_ACCESSION}*")
else
    # No argument given, look for all study accessions in the input
    STUDY_FOLDERS=$(find "${INPUT_DIR}" -maxdepth 1 -type d -name "[en]std*")
fi

if [ -z "${STUDY_FOLDERS}" ]; then
    ERROR_MSG="Error: Study accession directory not found in ${INPUT_DIR}" >&2
    exit 1
fi

echo "Converting GVFs, obtaining metadata and summary reports..."

FINDER_ARGS=(
    --search_dir "${INPUT_DIR}"
    --log "${BASE_DIR}/hpc.log"
    --output "${OUTPUT_DIR}"
    --config "${CONFIG_FILE}"
)

if [ -n "${STUDY_ACCESSION}" ]; then
    FINDER_ARGS+=(--study_accession "${STUDY_ACCESSION}")
fi

# Convert the GVFs to VCFs
"$FINDER_SCRIPT" "${FINDER_ARGS[@]}"

shopt -s nullglob

source "$ENV_ACTIVATE"
export LD_PRELOAD=/usr/lib64/libffi.so.8

SCRIPT_FAILED=false

shopt -s nullglob

for STUDY_FOLDER in ${STUDY_FOLDERS}; do  # full path

    STUDY_NAME=$(basename "${STUDY_FOLDER}") # estd1_Redon_et_al_2006

    STUDY_ACCESSION=$(echo "${STUDY_NAME}" | cut -d'_' -f1) # estd1

	echo "Processing accession: ${STUDY_ACCESSION}..."

    shopt -s nullglob
    GVF_FILES=( "${STUDY_FOLDER}/gvf"/*Remapped.gvf "${STUDY_FOLDER}/gvf"/*Submitted.gvf )
    shopt -u nullglob

    for GVF_FILE in "${GVF_FILES[@]}"; do

        GVF_FILENAME=$(basename "${GVF_FILE}")

        SUBMIT_STUDY_DIR="${SUBMISSION_DIR}/${STUDY_NAME}"

		# ==========================================
		# STEP 2: Validate with eva-sub-cli
		# ==========================================
		echo "Running validation with eva-sub-cli..."
		echo "Submission dir ${SUBMIT_STUDY_DIR}"
		METADATA_JSON="${SUBMIT_STUDY_DIR}/eva_submission_${STUDY_ACCESSION}.json"

		# set the arguents
		VALIDATE_ARGS=(
		    --submission_dir "$SUBMIT_STUDY_DIR"
		    --metadata_json "$METADATA_JSON"
		    --tasks validate
		)

		# add validation tasks if added
		if [ ${#VALIDATION_TASKS[@]} -gt 0 ]; then
		    VALIDATE_ARGS+=(--validation_tasks "${VALIDATION_TASKS[@]}")
		fi

		# run validation
		echo "Running validation..."
		if eva-sub-cli.py "${VALIDATE_ARGS[@]}"; then
		    echo "Validation passed successfully!"

		    # ==========================================
			# STEP 3: Submit with eva-sub-cli: if flag is set and validation passes
			# ==========================================
		    if [ "${RUN_SUBMIT}" = true ]; then
		        echo "Submission flag is enabled. Proceeding to official submission..."

		        eva-sub-cli.py \
		            --submission_dir "${SUBMIT_STUDY_DIR}" \
		            --metadata_json "$METADATA_JSON" \
		            --tasks submit \
		            --username "${WEBIN_USER}" \
		            --password "${WEBIN_PASS}"
		    else
		        echo "NOTE: Submission flag is set to false. Skipping submission step."
		    fi
		else
			echo "Validation failed!"
		    echo "ERROR: Validation failed. Submission aborted." >&2
	    	SCRIPT_FAILED=true
	    	continue
	    fi
	done
done
shopt -u nullglob

unset WEBIN_USER WEBIN_PASS
if [ "$SCRIPT_FAILED" = true ]; then
    echo "--Ended with errors--"
    exit 1
fi

echo "--Ended successfully--"
