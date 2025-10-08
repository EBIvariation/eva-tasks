import csv
import os
import subprocess
import sys
from time import sleep

import yaml
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.command_utils import run_command_with_output

logger = log_cfg.get_logger(__name__)

submission_folder = ''

def get_eload_folder(eload):
    eload_string = f'ELOAD_{eload}'
    return os.path.join(submission_folder, eload_string)

def get_eload_config(eload):
    return os.path.join(get_eload_folder(eload), f'.ELOAD_{eload}_config.yml')

def delete_eload(eload):
    eload_string = f'ELOAD_{eload}'
    # Check the folder exists of an ELOAD FOLDER
    eload_dir = get_eload_folder(eload)
    if not eload_dir:
        logger.error(f'No folder for the specified ELOAD {eload_string}')

def run_qc_submission(eload):
    log_file = os.path.join(get_eload_folder(eload), 'qc_submission.txt')
    command = f'qc_submission.py --eload {eload} > {log_file}'

    config_file = get_eload_config(eload)
    already_run = False
    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            eload_cfg = yaml.load(f, Loader=yaml.FullLoader)
            checks = eload_cfg.get('qc_checks')
            if checks:
                already_run = True
    if not already_run:
        try:
            run_command_with_output(f'run qc_submission.py for eload {eload} > {log_file}', command)
        except subprocess.CalledProcessError:
            return 'FAIL'
        sleep(1)

    config_file = get_eload_config(eload)
    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            eload_cfg = yaml.load(f, Loader=yaml.FullLoader)
            checks = eload_cfg.get('qc_checks')
            if checks:
                if all('PASS' in checks[check_name] for check_name in checks):
                    return 'PASS'
                else:
                    list_checks = [check_name for check_name in checks if 'PASS' not in checks[check_name]]
                    return f'FAIL({",".join(list_checks)})'
            else:
                return 'FAIL'
    else:
        return 'FAIL'

def run_submission_status(eload):
    log_file = os.path.join(get_eload_folder(eload), 'submission_status.txt')

    command = f'submission_status.py --eload {eload} > {log_file}'
    if not os.path.exists(log_file):
        try:
            run_command_with_output(f'run submission_status.py for eload {eload} > {log_file}', command)
        except subprocess.CalledProcessError:
            return 'FAIL'
        sleep(1)

    with open(log_file, 'r') as f:
        for line in f:
            if line.startswith(f'ELOAD_{eload}'):
                sp_line = line.strip().split('\t')
                '''
eload   project analysis        taxonomy        source_assembly target_assembly metadata_load_status    accessioning_status     remapping_status        clustering_statusvariant_load_status      statistics_status       annotation_status
'''
                # Return the metadata_load_status
                return 'PASS' if sp_line[6] == 'Done' else sp_line[6]
    return 'Not completed'


def load_eloads_from_jira(jira_csv):
    with open(jira_csv) as open_file:
        reader = csv.DictReader(open_file, delimiter=',')
        for row in reader:
            eload_with_hyphen = row.get('Issue key')
            eload = eload_with_hyphen.split('-')[-1]
            status = row.get('Status')
            yield eload, status

def eload_size(eload):
    log_file = os.path.join(get_eload_folder(eload), 'eload_size.txt')
    command_sh = f'du -s {get_eload_folder(eload)} >  {log_file}'
    if not os.path.exists(log_file):
        run_command_with_output(f'run du for eload {eload} ', command_sh, return_process_output=True)
        sleep(1)
    with open(log_file, 'r') as f:
        text=f.readline().strip()
        sp_txt = text.split()
        if len(sp_txt[0]) > 1 and sp_txt[0].isdigit():
            return sp_txt[0]
        else:
            logger.error(f'Could not determine size of eload {eload}')
            return '0'


def main():
    if not submission_folder:
        print('Provide the submission folder')
        sys.exit(1)
    accepted_status = ['Done', 'Cancelled']
    for eload, status in load_eloads_from_jira(sys.argv[1]):

        if status in accepted_status and os.path.isdir(get_eload_folder(eload)):
            results = [
                eload,
                status,
                eload_size(eload),
                run_submission_status(eload),
                run_qc_submission(eload)
            ]
        elif not os.path.isdir(get_eload_folder(eload)) or not os.path.isfile(get_eload_config(eload)):
            results = [
                eload,
                status,
                '0',
                'NO DIR',
                'NO DIR'
            ]
        else:
            results = [
                eload,
                status,
                eload_size(eload),
                'Not completed',
                'Not completed'
            ]
        print('\t'.join(results))
    return


if __name__ == '__main__':
    sys.exit(main())
