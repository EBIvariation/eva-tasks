import argparse
import os
from functools import cached_property

import requests
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

logger = logging_config.get_logger(__name__)
logging_config.add_stderr_handler()


class DBMissingContig:

    def __init__(self, maven_config, maven_profile, dbname, reference_genome_dir=None):
        self.reference_genome_dir = reference_genome_dir
        self.maven_config = maven_config
        self.maven_profile = maven_profile
        self.dbname = dbname

    @cached_property
    def metadata_conn(self):
        return get_metadata_connection_handle(self.maven_profile, self.maven_config)

    @cached_property
    def mongo_conn(self):
        return get_mongo_connection_handle(self.maven_profile, self.maven_config)

    @cached_property
    def mongo_database(self):
        return self.mongo_conn[self.dbname]

    def mongo_database_size(self):
        return self.mongo_database.command('dbstats').get('dataSize')


    @cached_property
    def _project_and_analysis(self):
        projects = set()
        analyses = set()
        for document in self.mongo_database['files_2_0'].find({}, {'fid': 1, 'sid': 1}):
            projects.add(document['sid'])
            analyses.add(document['fid'])
        if not analyses:
            document = self.mongo_database['variants_2_0'].find_one({}, {'files.fid': 1, 'files.sid': 1})
            if document:
                projects.add(document.get('files')[0].get('sid'))
                analyses.add(document.get('files')[0].get('fid'))
        return projects, analyses

    @cached_property
    def projects(self):
        projects, analyses = self._project_and_analysis
        return projects

    @cached_property
    def analyses(self):
        projects, analyses = self._project_and_analysis
        return analyses

    @cached_property
    def scientific_name_and_assembly_accession(self):
        sp_db_name = self.dbname.split('_')
        taxonomy_code = sp_db_name[1]
        assembly_code = '_'.join(sp_db_name[2:])
        response = requests.get('https://www.ebi.ac.uk/eva/webservices/rest/v1/meta/species/list')
        response.raise_for_status()
        results = response.json()['response'][0]['result']
        for species_dict in results:
            if species_dict['assemblyCode'] == assembly_code and species_dict['taxonomyCode'] == taxonomy_code:
                return species_dict['taxonomyScientificName'], species_dict['assemblyAccession']
        return None, None

    @cached_property
    def assembly_report(self):
        scientific_name, assembly_accession = self.scientific_name_and_assembly_accession
        if not scientific_name or not assembly_accession:
            raise ValueError(f'Cannot resolve scientific name and assembly accession from {self.dbname}')
        assembly_report_path = os.path.join(self.reference_genome_dir, scientific_name.lower().replace(' ', '_'),
                                            assembly_accession, assembly_accession + '_assembly_report.txt')
        return assembly_report_path

    @cached_property
    def assembly_fasta(self):
        scientific_name, assembly_accession = self.scientific_name_and_assembly_accession
        if not scientific_name or not assembly_accession:
            raise ValueError(f'Cannot resolve scientific name and assembly accession from {self.dbname}')
        assembly_fasta_path = os.path.join(self.reference_genome_dir, scientific_name.lower().replace(' ', '_'),
                                            assembly_accession, assembly_accession + '.fa')
        return assembly_fasta_path

    def load_assembly_report(self):
        assembly_report_map = {}
        with open(self.assembly_report) as assembly_report_file:
            for line in assembly_report_file:
                if line.startswith('#'):
                    continue
                sp_line = line.strip().split('\t')
                accepted_names = {sp_line[0], sp_line[4], sp_line[6], sp_line[9]}
                # Add any other columns as valid alias
                if len(sp_line) > 9:
                    for val in sp_line:
                        if val:
                            accepted_names.add(val)
                if 'na' in accepted_names:
                    accepted_names.remove('na')
                for name in accepted_names:
                    if name in assembly_report_map:
                        raise ValueError(f'Duplicate assembly report entry for {name} in line {line.strip()}')
                    assembly_report_map[name] = sp_line[4]
        return assembly_report_map

    def load_assembly_fasta(self):
        name_in_fasta = set()
        with open(self.assembly_fasta) as assembly_fasta_file:
            for line in assembly_fasta_file:
                if line.startswith('>'):
                    name = line.strip().split()[0]
                    name_in_fasta.add(name.lstrip('>'))
        return name_in_fasta

    def get_chromosome_names(self):
        chromosome_names = {}
        pipeline = [
            {"$group": {'_id':'$chr', "projects": {"$addToSet": "$files.sid"}}},
            {"$project": {"projects": { "$reduce": {"input": '$projects', "initialValue": [], "in": {"$concatArrays": ['$$value', '$$this']}}}}}
        ]
        for chr_dict in self.mongo_database['variants_2_0'].aggregate(pipeline):
            chromosome_names[chr_dict.get('_id')] = chr_dict.get('projects')
        return chromosome_names

    def find_missing_contigs(self):
        assembly_report_map = self.load_assembly_report()
        assembly_sequence_name_set = self.load_assembly_fasta()
        chromosome2projects = self.get_chromosome_names()

        for chromosome in chromosome2projects:
            if chromosome not in assembly_report_map:
                projects = ", ".join(set(chromosome2projects.get(chromosome)))
                logger.error(f'Missing chromosome {chromosome} used in projects {projects} in assembly report file {self.assembly_report}')
                continue
            insdc_accession = assembly_report_map.get(chromosome)
            if insdc_accession not in assembly_sequence_name_set:
                logger.error(f'Missing chromosome {insdc_accession} originally {chromosome} in assembly fasta file {self.assembly_fasta}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='investigate missing contig dbs')
    parser.add_argument("--databases",  nargs='*', default=None, help="list of database to investigate", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", choices=('localhost', 'development', 'production_processing'),
                        help="Profile to look for information", required=True)
    parser.add_argument("--reference_genome_dir", help="parent directory for the reference genomes", required=True)
    args = parser.parse_args()
    for database_name in args.databases:
        DBMissingContig(args.private_config_xml_file, args.profile, database_name, reference_genome_dir=args.reference_genome_dir).find_missing_contigs()
