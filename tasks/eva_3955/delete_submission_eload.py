import csv
import logging
import os
import sys
from argparse import ArgumentParser
from itertools import islice

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.command_utils import run_command_with_output

logger = log_cfg.get_logger(__name__)

submission_folder = ''

def delete_eload(eload):
    eload_string = f'ELOAD_{eload}'
    # Check the existance of an ELOAD FOLDER
    eload_folder = os.path.join(submission_folder, eload_string)
    if not eload_folder:
        logger.error(f'No folder for the specified ELOAD {eload_string}')

def run_qc_submission(eload):
    command = f'qc_submission.py --eload {eload}'
    eload_string = f'ELOAD_{eload}'
    log_file = os.path.join(submission_folder, eload_string, 'qc_submission.txt')
    run_command_with_output(f'run qc_submission.py for eload {eload} > {log_file}', command)


def run_submission_status(eload):
    command = f'submission_status.py --eload {eload}'
    eload_string = f'ELOAD_{eload}'
    log_file = os.path.join(submission_folder, eload_string, 'submission_status.txt')
    run_command_with_output(f'run submission_status.py for eload {eload} > {log_file}', command)

def load_eloads_from_jira(jira_csv):
    with open(jira_csv) as open_file:
        reader = csv.DictReader(open_file, delimiter=',')
        for row in reader:
            eload_with_hyphen = row.get('Issue key')
            eload = eload_with_hyphen.split('-')[-1]
            status = row.get('Status')
            yield eload, status


def main():
    for eload, status in load_eloads_from_jira(sys.argv[1]):
        run_submission_status(eload)
        run_qc_submission(eload)
        delete_eload(eload)






if __name__ == '__main__':
    sys.exit(main())
