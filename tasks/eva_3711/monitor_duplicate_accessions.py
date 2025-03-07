#!/usr/bin/env python3

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

import getpass
import os
import smtplib
from datetime import datetime
from urllib.parse import quote_plus

import click


import configparser
import logging
import subprocess
import sys


def init_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


def get_args_from_properties_file(properties_file):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    with open(properties_file, "r") as properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "pipeline_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        config = dict(parser.items(section=properties_section_name))
        return config


def get_mongo_connection_details_from_properties_file(properties_file):
    properties_file_args = get_args_from_properties_file(properties_file)
    mongo_connection_properties = {"mongo_host": properties_file_args["spring.data.mongodb.host"],
                                   "mongo_port": properties_file_args["spring.data.mongodb.port"],
                                   "mongo_db": properties_file_args["spring.data.mongodb.database"],
                                   "mongo_username": properties_file_args["spring.data.mongodb.username"],
                                   "mongo_password": properties_file_args["spring.data.mongodb.password"],
                                   "mongo_auth_db": properties_file_args["spring.data.mongodb.authentication-database"]}
    return mongo_connection_properties


def run_command_with_output(command_description, command, return_process_output=False):
    process_output = ""

    logger.info("Starting process: " + command_description)

    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in iter(process.stdout.readline, ''):
            line = str(line).rstrip()
            logger.info(line)
            if return_process_output:
                process_output += line
        for line in iter(process.stderr.readline, ''):
            line = str(line).rstrip()
            logger.error(line)
    if process.returncode != 0:
        logger.error(command_description + " failed! Refer to the error messages for details.")
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " - completed successfully")
    if return_process_output:
        return process_output


logger = init_logger()


def get_mongo_uri(mongo_connection_properties, timeout=60000):
    return "mongodb://{0}:{1}@{2}/{3}?authSource={4}&connectTimeoutMS={5}".format(
        mongo_connection_properties["mongo_username"],
        quote_plus(mongo_connection_properties["mongo_password"]),
        mongo_connection_properties["mongo_host"],
        mongo_connection_properties["mongo_db"],
        mongo_connection_properties["mongo_auth_db"],
        timeout
    )


def export_mongo_accessions(mongo_connection_properties, collection_name, study, export_output_filename):
    export_command = (f'mongoexport --uri "{get_mongo_uri(mongo_connection_properties)}" '
                      f'--collection {collection_name} --type=csv --fields accession '
                      f"--query  '{{\"study\": \"{study}\", \"remappedFrom\": {{\"$exists\": false}}}}' " 
                      f'-o "{export_output_filename}" 2>&1')
    run_command_with_output("Exporting accessions in the {0} collection in the {1} database at {2}..."
                            .format(collection_name, mongo_connection_properties["mongo_db"],
                                    mongo_connection_properties["mongo_host"]), export_command)


def notify_by_email(mongo_connection_properties, collection_name, duplicates_output_filename,
                    number_of_duplicate_accessions, email_recipients):
    error_message = "{0} DUPLICATE ACCESSIONS !!! in the {1} collection in the {2} database at {3}"\
                    .format(number_of_duplicate_accessions, collection_name, mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_host"])
    logger.error(error_message)
    email_message = "Subject: {0}\n\n" \
                    "Please see {1} for the list of duplicates.".format(error_message, duplicates_output_filename)
    smtplib.SMTP('localhost').sendmail(getpass.getuser(), email_recipients, email_message)


def report_duplicates_in_exported_accessions_file(mongo_connection_properties, collection_name, export_output_filename,
                                                  duplicates_output_filename, email_recipients):
    sorted_export_output_filename = export_output_filename.replace(".csv", "_sorted.csv")
    run_command_with_output("Sorting {0}...".format(duplicates_output_filename),
                            'sort -S 4G -T {0} -o "{1}" "{2}"'
                            .format(os.path.dirname(export_output_filename), sorted_export_output_filename,
                                    export_output_filename))
    run_command_with_output("Exporting duplicates to {0}...".format(duplicates_output_filename),
                            'uniq -d "{0}" > {1}'.format(sorted_export_output_filename, duplicates_output_filename))
    number_of_duplicate_accessions = run_command_with_output("Find duplicate accessions in the exported file...",
                                                             'wc -l < "{0}"'.format(duplicates_output_filename),
                                                             return_process_output=True)
    if int(number_of_duplicate_accessions) > 0:
        notify_by_email(mongo_connection_properties, collection_name, duplicates_output_filename,
                        number_of_duplicate_accessions, email_recipients)
        # Use exit code 0 as scheduler will also send email on crash
        return 0
    else:
        logger.info("NO duplicate accessions were found in the {0} collection in the {1} database at {2}..."
                    .format(collection_name, mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_host"]))
        return 0


def report_duplicate_accessions_in_mongo(pipeline_properties_file, accessions_export_output_dir,
                                         collection_name, study, email_recipients):
    mongo_connection_properties = get_mongo_connection_details_from_properties_file(pipeline_properties_file)
    export_output_filename = os.path.sep.join([accessions_export_output_dir,
                                               "accessions_in_{0}_{1}_at_{2}_as_of_{3}.csv"
                                              .format(mongo_connection_properties["mongo_db"], collection_name,
                                                      mongo_connection_properties["mongo_host"],
                                                      datetime.today().strftime('%Y%m%d%H%M%S'))])
    duplicates_output_filename = export_output_filename.replace("accessions_in", "duplicate_accessions_in")

    logger.info("Checking duplicate accessions in the {0} collection in the {1} database at {2}..."
                .format(collection_name, mongo_connection_properties["mongo_db"],
                        mongo_connection_properties["mongo_host"]))

    export_mongo_accessions(mongo_connection_properties, collection_name, study, export_output_filename)

    return report_duplicates_in_exported_accessions_file(mongo_connection_properties, collection_name,
                                                         export_output_filename, duplicates_output_filename,
                                                         email_recipients)


@click.option("-p", "--pipeline-properties-file", required=True)
@click.option("-o", "--accessions-export-output-dir", required=True)
@click.option("-s", "--study", required=True)
@click.option("-e", "--email-recipients", multiple=True, required=True)
@click.argument("collection-names", nargs=-1, required=True)
@click.command()
def main(pipeline_properties_file, accessions_export_output_dir, study, email_recipients, collection_names):
    exit_code = 0
    for collection_name in collection_names:
        exit_code = exit_code or \
                    report_duplicate_accessions_in_mongo(pipeline_properties_file, accessions_export_output_dir,
                                                         collection_name, study, email_recipients)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
