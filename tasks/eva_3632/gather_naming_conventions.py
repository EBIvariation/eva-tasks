#!/usr/bin/env python

# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.eload_submission import Eload
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def report_per_files(eloads):
    for eload in eloads:
        eload_obj = Eload(eload_number=eload)
        vcf_files_2_naming_conv = eload_obj.eload_cfg.query('validation', 'naming_convention_check', 'files')
        if vcf_files_2_naming_conv:
            for vcf_file in vcf_files_2_naming_conv:
                if vcf_files_2_naming_conv[vcf_file]['naming_convention']:
                    print(f"ELOAD_{eload}: {vcf_file} {vcf_files_2_naming_conv[vcf_file]['naming_convention']}")
                elif vcf_files_2_naming_conv[vcf_file]['naming_convention_map']:
                    nc_map = vcf_files_2_naming_conv[vcf_file]['naming_convention_map']
                    if 'Not found' in nc_map:
                        print(f"ELOAD_{eload}: {vcf_file} Missing {len(nc_map['Not found'])} chromosome")
                    else:
                        print(f"ELOAD_{eload}: {vcf_file} Found {len(nc_map)} naming conventions")
                else:
                    print(f"ELOAD_{eload}: {vcf_file} naming conventions not assessed")


def report_per_eload(eloads):
    for eload in eloads:
        eload_obj = Eload(eload_number=eload)
        vcf_files_2_naming_conv = eload_obj.eload_cfg.query('validation', 'naming_convention_check', 'files')
        naming_conv = eload_obj.eload_cfg.query('validation', 'naming_convention_check', 'naming_convention')
        if naming_conv:
            pass
        elif vcf_files_2_naming_conv:
            for vcf_file in vcf_files_2_naming_conv:
                file_naming_convention = vcf_files_2_naming_conv[vcf_file]['naming_convention']
                if file_naming_convention and naming_conv not in ['Multiple', 'Not calculated', 'Not found']:
                    # If we get here it means that different files use different naming convention
                    naming_conv = 'Different per files'
                nc_map = vcf_files_2_naming_conv[vcf_file]['naming_convention_map']
                if vcf_files_2_naming_conv[vcf_file]['naming_convention']:
                    pass
                elif vcf_files_2_naming_conv[vcf_file]['naming_convention_map']:
                    if naming_conv not in ['Not calculated'] and 'Not found' in nc_map:
                        naming_conv = 'Not found'
                    elif naming_conv not in ['Not calculated', 'Not found']:
                        naming_conv = 'Multiple'
                else:
                    naming_conv = 'Not calculated'
        else:
            naming_conv = 'Not calculated'

        print(f"ELOAD_{eload}: {naming_conv}")


def main():
    argparse = ArgumentParser(description='Validate an ELOAD by checking the data and metadata format and semantics.')
    argparse.add_argument('--eloads', required=True, type=int, nargs='+',
                          help='The ELOAD numbers of the submissions to summarise')
    argparse.add_argument('--per_files', action='store_true', default=False,
                          help='Report the naming convention per file rather than per eload')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    if args.per_files:
        report_per_files(args.eloads)
    else:
        report_per_eload(args.eloads)


if __name__ == "__main__":
    main()
