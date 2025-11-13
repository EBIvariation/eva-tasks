import argparse
import csv
import glob
import json
import os

import sys
from copy import deepcopy

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.eload_submission import Eload
from eva_submission.eload_validation import EloadValidation

logger = log_cfg.get_logger(__name__)

all_tasks = ['prepare', 'validate', 'copy_back']

def modify_an_copy_metadata(eload_src, eload_dst):
    src_metadata_json = eload_src.eload_cfg.query('submission', 'analyses', 'metadata_json')
    with open(src_metadata_json) as open_file:
        source_json = json.load(open_file)
        dest_json = deepcopy(source_json)
    dst_metadata_json = os.path.join(eload_dst._get_dir('metadata'), os.path.basename(src_metadata_json))
    with open(dst_metadata_json, 'w') as open_file:
        json.dump(dest_json, open_file)

def generate_csv_mappings(src_eload):
    vcf_files_mapping_csv = os.path.join(src_eload.eload_dir, 'filtering_vcf_files.csv')
    with open(vcf_files_mapping_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['vcf'])
        analyses = src_eload.eload_cfg.query('submission', 'analyses')
        for analysis_alias, analysis_data in analyses.items():
            if analysis_data['vcf_files']:
                for vcf_file in analysis_data['vcf_files']:
                    writer.writerow([vcf_file])
    return vcf_files_mapping_csv

def filter_and_copy_vcf_files(src_eload, eload_dst):
    output_dir = src_eload.create_nextflow_temp_output_directory()
    filtering_nextflow_script = os.path.join(os.path.dirname(__file__), 'filter_vcfs.nf')
    filtering_config = {
        'vcf_files_mapping': generate_csv_mappings(src_eload),
        'output_dir': output_dir,
        'executable': cfg['executable']
    }

    filtering_config_file = os.path.join(src_eload.eload_dir, 'filtering_config_file.yaml')
    with open(filtering_config_file, 'w') as open_file:
        yaml.safe_dump(filtering_config, open_file)
    # run the filtering nextflow
    command_utils.run_command_with_output(
        'Nextflow Filtering process',
        ' '.join((
            'export NXF_OPTS="-Xms1g -Xmx8g"; ',
            cfg['executable']['nextflow'], filtering_nextflow_script,
            '-params-file', filtering_config_file,
            '-work-dir', output_dir
        ))
    )

    # Move all the filtered files in the destination eload
    for vcf in glob.glob(os.path.join(output_dir, 'filtered_files', '*.vcf.gz')):
        os.rename(vcf, os.path.join(eload_dst._get_dir('vcf'), os.path.basename(vcf)))


def process_eloads(source_eload, dest_eload, tasks):
    eload_src  = Eload(source_eload)
    eload_dst = Eload(dest_eload)
    if 'prepare' in tasks:
        modify_an_copy_metadata(eload_src, eload_dst)
        filter_and_copy_vcf_files(eload_src, eload_dst)

        with EloadPreparation(eload_number=dest_eload) as eload_prep:
            eload_prep.detect_all(reference_accession='GCA_000005575.1')

    if 'validate' in tasks:
        with EloadValidation(eload_number=dest_eload) as eload_val:
            eload_val.validate()

    if 'copy_back' in tasks:
        with EloadValidation(eload_number=source_eload) as eload_src_val, EloadValidation(eload_number=dest_eload) as eload_dest_val:
            eload_src_val.eload_cfg.set('validation', value=eload_dest_val.eload_cfg.get('validation'))
            eload_src_val.mark_valid_files_and_metadata()

def main():
    arg_parser = argparse.ArgumentParser(description='Filter and copy the VCF files from an ELOAD and copy the results to another. Run the validation then copy the results to a new ELOAD. Run the validation then copy the results to a old ELOAD.')
    arg_parser.add_argument('--source_eload', required=True, help='The source eload number.')

    arg_parser.add_argument('--dest_eload', required=True, help='The source eload number.')
    arg_parser.add_argument('--tasks', required=False, default=all_tasks, nargs='+',
                            help='The set of tasks that will be performed.')

    args = arg_parser.parse_args()

    process_eloads(args.source_eload, args.dest_eload, tasks=args.tasks)

    return


if __name__ == '__main__':
    sys.exit(main())
