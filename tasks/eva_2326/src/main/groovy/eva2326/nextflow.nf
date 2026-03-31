nextflow.enable.dsl=2

params.input_file             = null
params.chunk_size             = 1_000_000
params.working_dir            = null
params.env_properties_file    = null
params.db_name                = null
params.max_parallel_chunks    = 5
params.project_dir            = null
params.groovy_script          = null

// ── PROCESS 1: split into named chunks ───────────────────────────────────────
process SPLIT_INPUT {
    output:
    path "chunk_*.txt", emit: chunks

    script:
    """
    awk -v chunk_size=${params.chunk_size} '
    {
        line_num = NR
        if ((line_num - 1) % chunk_size == 0) {
            start = line_num
            end   = start + chunk_size - 1
            # zero-pad to 10 digits so filenames sort correctly
            filename = sprintf("chunk_%010d-%010d.txt", start, end)
        }
        print > filename
    }
    ' ${params.input_file}
    """
}

// ── PROCESS 2: remediate one chunk ───────────────────────────────────────────
process REMEDIATE_CHUNK {
    label 'long_time'
    label 'med_mem'
    maxForks params.max_parallel_chunks

    tag { chunk_file.name }

    input:
    path chunk_file

    output:
    path "${chunk_file.baseName}.done", emit: done_flag

    script:
    """
    CHUNK_NAME=\$(basename ${chunk_file} .txt)
    NOT_REMEDIATED_FILE=${params.working_dir}/variants_not_remediated/${params.db_name}_\$CHUNK_NAME.txt
    mkdir -p \$(dirname "\$NOT_REMEDIATED_FILE")

    bash run_groovy_script.sh \
        ${params.project_dir} \
        ${params.groovy_script} \
        -envPropertiesFile=${params.env_properties_file} \
        -dbName=${params.db_name} \
        -annotationRemediationInputFile=\$(realpath ${chunk_file}) \
        -notRemediatedVariantsFilePath="\$NOT_REMEDIATED_FILE" \
        > ${params.working_dir}/\$CHUNK_NAME.log 2>&1

    echo "Done: ${chunk_file.name}" > ${chunk_file.baseName}.done
    """
}

// ── WORKFLOW ──────────────────────────────────────────────────────────────────
workflow {
    if (!params.input_file)          error "Please provide --input_file"
    if (!params.env_properties_file) error "Please provide --env_properties_file"
    if (!params.db_name)             error "Please provide --db_name"
    if (!params.working_dir)         error "Please provide --working_dir"
    if (!params.project_dir)         error "Please provide --project_dir"
    if (!params.groovy_script)       error "Please provide --groovy_script"

    SPLIT_INPUT()

    chunks_ch = SPLIT_INPUT.out.chunks.flatten()

    REMEDIATE_CHUNK(chunks_ch)

    REMEDIATE_CHUNK.out.done_flag
        .collect()
        .view { flags -> "All chunks complete: ${flags.size()} chunks processed" }
}