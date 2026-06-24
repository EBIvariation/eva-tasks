nextflow.enable.dsl=2

params.max_parallel_chunks    = 5
params.project_dir            = null
params.groovy_script          = null
params.working_dir            = null
params.env_properties_file    = null
params.assembly_report_dir    = null

def dbNames = ['eva_athaliana_tair10','eva_cfamiliaris_31','eva_cfamiliaris_roscfam10',
               'eva_chircus_ars1','eva_cporcellus_30','eva_drerio_grcz11','eva_smansoni_23792v2',
               'eva_hsapiens_grch37','eva_zmays_zmb73referencenam50','eva_tgrandiflorum_criollococoagenomev2',
               'eva_oaries_oarv31','eva_h_morexv3pseudomoleculesassembly',
               'eva_hvulgare_morexv3pseudomoleculesassembly','eva_taestivum_iwgscrefseqv10',
               'eva_chircus_ars1','eva_acygnoides_goosev10','eva_cfamiliaris_dog10kboxertasha',
               'eva_dlabrax_seabassv10','eva_fcatus_fcatusfca126mat10','eva_vvinifera_12x',
               'eva_mmusculus_grcm38','a_ssalar_ssalv3','eva_ssalar_ssalv31','eva_hsapiens_grch38',
               'eva_cfamiliaris_uucfamgsd10','eva_ecaballus_20','eva_btaurus_arsucd12'
               ]

process REMEDIATE_DB {
    label 'long_time'
    label 'med_mem'
    maxForks params.max_parallel_chunks
    errorStrategy 'ignore'

    tag { db_name }

    input:
    val db_name

    output:
    path "${db_name}.done", emit: done_flag

    script:
    """
    bash run_groovy_script.sh \
        ${params.project_dir} \
        ${params.groovy_script} \
        -workingDir=${params.working_dir} \
        -envPropertiesFile=${params.env_properties_file} \
        -assemblyReportDir=${params.assembly_report_dir} \
        -dbName=${db_name} \
        > ${params.working_dir}/${db_name}.log 2>&1

    echo "Done: ${db_name}" > ${db_name}.done
    """
}

workflow {
    if (!params.project_dir)         error "Please provide --project_dir"
    if (!params.groovy_script)       error "Please provide --groovy_script"
    if (!params.working_dir)         error "Please provide --working_dir"
    if (!params.env_properties_file) error "Please provide --env_properties_file"
    if (!params.assembly_report_dir) error "Please provide --assembly_report_dir"

    REMEDIATE_DB(Channel.fromList(dbNames))

    REMEDIATE_DB.out.done_flag
        .collect()
        .view { flags -> "All DBs complete: ${flags.size()} DBs processed" }
}