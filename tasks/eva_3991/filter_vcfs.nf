#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Validate a set of VCF files and metadata to check if they are valid to be submitted to EVA.

    Inputs:
            --vcf_files     csv file with the mappings for vcf files, fasta and assembly report
            --output_dir            output_directory where the reports will be output
    """
}

params.vcf_files = null
params.output_dir = null
// executables
params.executable = ["bgzip": "bgzip"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files || !params.output_dir ) {
    if (!params.vcf_files)    log.warn('Provide a csv file with the mappings (vcf) --vcf_files')
    if (!params.output_dir)    log.warn('Provide an output directory where the reports will be copied using --output_dir')
    exit 1, helpMessage()
}


workflow {
    vcf_files_ch = Channel.fromPath(params.vcf_files)
        .splitCsv(header:true)
        .map{row -> file(row.vcf))}

    remove_invalid_variant(vcf_files_ch)

}

/*
 * Remove variants that are located on the Non INSDC contigs
 */
process remove_invalid_variant {
    label 'default_time', 'med_mem'

    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    path(vcf_file)

    output:
    path "output_files/$vcf_file", emit: filtered_files

    script:
    """
    set -eo pipefail
    mkdir filtered_files
    zcat $vcf_file | grep -Pv 'UNKN|Y_unplaced' | params.executables.bcftools view -O z -o filtered_files/$vcf_file -
    """
}

