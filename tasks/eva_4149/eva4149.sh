#!/bin/bash

#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=24:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=1   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=8G   # memory per node
#SBATCH -J "test_val"   # job name
#SBATCH -o "test_val.out"   # job output file
#SBATCH -e "test_val.err"   # job error file

set -euo pipefail
# ==========================================
# Configuration - user input - check these are correct
# ==========================================
# 1. create the EVA task dir
# 2. add the data_dir, tests_dir and TEST.config to that directory
# 3. double check the values below particularly: study_accession,  assembly names, validation_tasks and run_submit
PATH_TO_EVA="/nfs/production/keane/eva"
ASSEMBLY_NAMES=("GRCh38" "GRCh37")
TASK_ID="EVA4149"
STUDY_ACCESSION="estd1"

BASE_DIR="${PATH_TO_EVA}/tasks/${TASK_ID}"

INPUT_DIR="${BASE_DIR}/data_dir"
TEST_DIR="${BASE_DIR}/tests"
CONFIG_FILE="${BASE_DIR}/TEST.config"

CONVERT_GVF_TO_VCF_DIR="${PATH_TO_EVA}/software/convertGVFtoVCF/development_deployment/main"
EVA_SUB_CLI_DIR="${PATH_TO_EVA}/software/eva-sub-cli/production_deployment/production"
VALIDATION_TASKS=(metadata_check vcf_check) #(choose from 'vcf_check', 'assembly_check', 'metadata_check', 'sample_check')
RUN_SUBMIT=false

CURRENT_DATE=$(date +'%d%b')
# ==========================================
# Setup and pre-flight checks
# ==========================================
OUTPUT_DIR="${BASE_DIR}/output"

SITE_PACKAGES="${CONVERT_GVF_TO_VCF_DIR}/lib/python3.11/site-packages"
FINDER_SCRIPT="${CONVERT_GVF_TO_VCF_DIR}/bin/gvf_file_finder.py"
ENV_ACTIVATE="${EVA_SUB_CLI_DIR}/bin/activate"

echo "Verifying input files and directories..."

# Check tests directory is present (this contains the assembly FASTA)
if [ ! -d "${TEST_DIR}" ]; then
    echo "ERROR: directory not found - ${TEST_DIR}" >&2
    exit 1
fi

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

echo "Installing assembly tests to site-packages..."
cp -r "$TEST_DIR" "$SITE_PACKAGES"
# ==========================================
# STEP 1: Convert GVF to VCF
# ==========================================
echo "Converting GVFs, obtaining metadata and summary reports..."

"$FINDER_SCRIPT" \
    --search_dir "${INPUT_DIR}" \
    --log "${BASE_DIR}/hpc.log" \
    --output "${OUTPUT_DIR}" \
    --config "${CONFIG_FILE}" \
    --study_accession "${STUDY_ACCESSION}"

shopt -s nullglob

source "$ENV_ACTIVATE"
export LD_PRELOAD=/usr/lib64/libffi.so.8

SCRIPT_FAILED=false

for ASSEMBLY_NAME in "${ASSEMBLY_NAMES[@]}"; do
    ASSEMBLY_DIR="${OUTPUT_DIR}/assembly_${ASSEMBLY_NAME}_${CURRENT_DATE}"
    # ==========================================
	# STEP 2: Organise files
	# ==========================================
    echo "Processing assembly: ${ASSEMBLY_NAME}..."
	
	echo "Creating assembly directory..."

	mkdir -p "$ASSEMBLY_DIR"

	echo "Copying VCF and JSON results..."
	# Copy both file types to the Base Directory
	#cp "${OUTPUT_DIR}"/*/*"${ASSEMBLY_NAME}".{Remapped.vcf,eva.json} "$BASE_DIR"

	# Copy both file types to the Assembly Directory
	cp "${OUTPUT_DIR}"/*/*"${ASSEMBLY_NAME}".{Remapped.vcf,eva.json} "$ASSEMBLY_DIR"

	# ==========================================
	# STEP 3: Check the files
	# ==========================================
	echo "Detecting the output files have been generated..."

	# Find the matching files inside the assembly directory (submission directory)
	JSON_FILES=("${ASSEMBLY_DIR}"/*"${ASSEMBLY_NAME}".eva.json)
	VCF_FILES=("${ASSEMBLY_DIR}"/*"${ASSEMBLY_NAME}".Remapped.vcf)

    # Check files are present
    if [ ${#JSON_FILES[@]} -eq 0 ] || [ ! -f "${JSON_FILES[0]}" ]; then
        echo "ERROR: Expected metadata JSON file not found in ${ASSEMBLY_DIR}" >&2
        SCRIPT_FAILED=true
        continue 
    fi

    if [ ${#VCF_FILES[@]} -eq 0 ] || [ ! -f "${VCF_FILES[0]}" ]; then
        echo "ERROR: Expected VCF file not found in ${ASSEMBLY_DIR}" >&2
        SCRIPT_FAILED=true
        continue 
    fi

    METADATA_JSON="${JSON_FILES[0]}"
    VCF_FILE="${VCF_FILES[0]}"
    echo "Found meta file: ${METADATA_JSON}"
    echo "Found VCF  file: ${VCF_FILE}"

    echo "Symlinking the VCF file..."
    
    ln -sf "${ASSEMBLY_DIR}/estd1_Redon_et_al_2006.2014-04-01.${ASSEMBLY_NAME}.Remapped.vcf" "${BASE_DIR}/estd1_Redon_et_al_2006.2014-04-01.${ASSEMBLY_NAME}.Remapped.vcf"

	# ==========================================
	# STEP 4: Validate with eva-sub-cli
	# ==========================================
	echo "Running validation with eva-sub-cli..."

	# set the arguents
	VALIDATE_ARGS=(
	    --submission_dir "$ASSEMBLY_DIR"
	    --metadata_json "$METADATA_JSON"
	    --tasks validate
	)

	# add validation tasks if added
	if [ ${#VALIDATION_TASKS[@]} -gt 0 ]; then
	    VALIDATE_ARGS+=(--validation_tasks "${VALIDATION_TASKS[@]}")
	fi

	# run validation
	if eva-sub-cli.py "${VALIDATE_ARGS[@]}"; then
	    
	    echo "Validation passed successfully!"

	    # ==========================================
		# STEP 5: Submit with eva-sub-cli: if flag is set and validation passes	
		# ==========================================
	    if [ "${RUN_SUBMIT}" = true ]; then
	        echo "Submission flag is enabled. Proceeding to official submission..."
	        
	        eva-sub-cli.py \
	            --submission_dir "$ASSEMBLY_DIR" \
	            --metadata_json "$METADATA_JSON" \
	            --tasks submit \
	            --username "${WEBIN_USER}" \
	            --password "${WEBIN_PASS}"
	    else
	        echo "NOTE: Submission flag is set to false. Skipping submission step."
	    fi
	else
	    echo "ERROR: Validation failed. Submission aborted." >&2
        SCRIPT_FAILED=true
        continue
	fi
done

shopt -u nullglob

unset WEBIN_USER WEBIN_PASS
if [ "$SCRIPT_FAILED" = true ]; then
    echo "--Ended with errors--"
    exit 1
fi

echo "--Ended successfully--"
