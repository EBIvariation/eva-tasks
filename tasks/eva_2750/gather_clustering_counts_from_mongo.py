import argparse
import glob
import os
import psycopg2
from collections import defaultdict
from datetime import datetime

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from ebi_eva_common_pyutils.pg_utils import execute_query, get_all_results_for_query


logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def gather_count_from_mongo(clustering_dir, mongo_source, private_config_xml_file):
    # Assume the directory structure:
    # clustering_dir --> <scientific_name_taxonomy_id> --> <assembly_accession> --> cluster_<date>.log_dict

    all_log_pattern = os.path.join(clustering_dir, '*', 'GCA_*', 'cluster_*.log')
    all_log_files = glob.glob(all_log_pattern)
    ranges_per_assembly = get_assembly_info_and_date_ranges(all_log_files)
    metrics_per_assembly = get_metrics_per_assembly(mongo_source, ranges_per_assembly)
    insert_counts_in_db(private_config_xml_file, metrics_per_assembly, ranges_per_assembly)


def get_assembly_info_and_date_ranges(all_log_files):
    """
    Parse all the log files and retrieve assembly basic information (taxonomy, scientific name) and the date ranges
    where all jobs and steps were run during the clustering process
    """
    ranges_per_assembly = defaultdict(dict)
    for log_file in all_log_files:
        logger.info('Parse log_dict file: ' + log_file)
        scientific_name, taxid, assembly_accession, log_date = parse_log_file_path(log_file)
        log_metric_date_range = parse_one_log(log_file)

        if assembly_accession not in ranges_per_assembly:
            ranges_per_assembly[assembly_accession] = defaultdict(dict)
            ranges_per_assembly[assembly_accession]['metrics'] = defaultdict(dict)

        # assembly info
        ranges_per_assembly[assembly_accession]['taxid'] = taxid
        ranges_per_assembly[assembly_accession]['scientific_name'] = scientific_name

        # new_remapped_current_rs
        if 'CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in log_metric_date_range:
            ranges_per_assembly[assembly_accession]['metrics']['new_remapped_current_rs'][log_file] = {
                'from': log_metric_date_range['CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': log_metric_date_range['CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP']
                if "CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP" in log_metric_date_range
                else log_metric_date_range["last_timestamp"]
            }
        # new_clustered_current_rs
        if 'CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in log_metric_date_range:
            ranges_per_assembly[assembly_accession]['metrics']['new_clustered_current_rs'][log_file] = {
                'from': log_metric_date_range['CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': log_metric_date_range['CLUSTER_UNCLUSTERED_VARIANTS_JOB']["completed"]
                if "completed" in log_metric_date_range['CLUSTER_UNCLUSTERED_VARIANTS_JOB']
                else log_metric_date_range["last_timestamp"]
            }
        # merged_rs
        if 'PROCESS_RS_MERGE_CANDIDATES_STEP' in log_metric_date_range:
            ranges_per_assembly[assembly_accession]['metrics']['merged_rs'][log_file] = {
                'from': log_metric_date_range['PROCESS_RS_MERGE_CANDIDATES_STEP'],
                'to': log_metric_date_range['PROCESS_RS_SPLIT_CANDIDATES_STEP']
                if "PROCESS_RS_SPLIT_CANDIDATES_STEP" in log_metric_date_range
                else log_metric_date_range["last_timestamp"]
            }
        # split_rs
        if 'PROCESS_RS_SPLIT_CANDIDATES_STEP' in log_metric_date_range:
            ranges_per_assembly[assembly_accession]['metrics']['split_rs'][log_file] = {
                'from': log_metric_date_range['PROCESS_RS_SPLIT_CANDIDATES_STEP'],
                'to': log_metric_date_range['CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP']
                if "CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP" in log_metric_date_range
                else log_metric_date_range["last_timestamp"]
            }
        # new_ss_clustered
        if 'CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in log_metric_date_range:
            ranges_per_assembly[assembly_accession]['metrics']['new_ss_clustered'][log_file] = {
                'from': log_metric_date_range['CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': log_metric_date_range['CLUSTER_UNCLUSTERED_VARIANTS_JOB']["completed"]
                if "completed" in log_metric_date_range['CLUSTER_UNCLUSTERED_VARIANTS_JOB']
                else log_metric_date_range["last_timestamp"]
            }
    return ranges_per_assembly


def get_metrics_per_assembly(mongo_source, ranges_per_assembly):
    """
    Perform queries to mongodb to get counts based on the date ranges for the different metrics
    """
    metrics_per_assembly = defaultdict(dict)
    for asm, asm_dict in ranges_per_assembly.items():
        new_remapped_current_rs, new_clustered_current_rs, merged_rs, split_rs, new_ss_clustered = 0, 0, 0, 0, 0
        for metric, log_dict in asm_dict['metrics'].items():
            expressions = []
            for log_name, query_range in log_dict.items():
                expressions.append({"createdDate": {"$gt": query_range["from"], "$lt": query_range["to"]}})

            date_range_filter = expressions
            if metric == 'new_remapped_current_rs':
                filter_criteria = {'asm': asm, '$or': date_range_filter}
                new_remapped_current_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_remapped_current_rs}')
            elif metric == 'new_clustered_current_rs':
                filter_criteria = {'asm': asm, '$or': date_range_filter}
                new_clustered_current_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_clustered_current_rs}')
            elif metric == 'merged_rs':
                filter_criteria = {'inactiveObjects.asm': asm, 'eventType': 'MERGED',
                                   '$or': date_range_filter}
                merged_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {merged_rs}')
            elif metric == 'split_rs':
                filter_criteria = {'inactiveObjects.asm': asm, 'eventType': 'RS_SPLIT',
                                   '$or': date_range_filter}
                split_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {split_rs}')
            elif metric == 'new_ss_clustered':
                filter_criteria = {'inactiveObjects.seq': asm, 'eventType': 'UPDATED',
                                   '$or': date_range_filter}
                new_ss_clustered = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_ss_clustered}')

        metrics_per_assembly[asm]["assembly_accession"] = asm
        metrics_per_assembly[asm]["new_remapped_current_rs"] = new_remapped_current_rs
        metrics_per_assembly[asm]["new_clustered_current_rs"] = new_clustered_current_rs
        metrics_per_assembly[asm]["merged_rs"] = merged_rs
        metrics_per_assembly[asm]["split_rs"] = split_rs
        metrics_per_assembly[asm]["new_ss_clustered"] = new_ss_clustered
    return metrics_per_assembly


def query_mongo(mongo_source, filter_criteria, metric):
    total_count = 0
    for collection_name in collections[metric]:
        logger.info(f'Querying mongo: db.{collection_name}.countDocuments({filter_criteria})')
        collection = mongo_source.mongo_handle[mongo_source.db_name][collection_name]
        count = collection.count_documents(filter_criteria)
        total_count += count
        logger.info(f'{count}')
    return total_count


def insert_counts_in_db(private_config_xml_file, metrics_per_assembly, ranges_per_assembly):
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("development", private_config_xml_file), user="evadev") \
            as metadata_connection_handle:
        for asm in metrics_per_assembly:
            # get last release data for assembly
            query_release2 = f"select * from dbsnp_ensembl_species.release_rs_statistics_per_assembly "\
                             f"where assembly_accession = '{asm}' and release_version = 2"
            logger.info(query_release2)
            asm_last_release_data = get_all_results_for_query(metadata_connection_handle, query_release2)

            # insert data for release 3
            taxid = ranges_per_assembly[asm]['taxid']
            scientific_name = ranges_per_assembly[asm]['scientific_name'].capitalize().replace('_', ' ')
            folder = f"{ranges_per_assembly[asm]['scientific_name']}/{asm}"
            release_version = 3

            release3_new_remapped_current_rs = metrics_per_assembly[asm]['new_remapped_current_rs']

            release3_new_clustered_current_rs = metrics_per_assembly[asm]['new_clustered_current_rs']
            release3_new_current_rs = release3_new_clustered_current_rs + release3_new_remapped_current_rs

            release3_new_merged_rs = metrics_per_assembly[asm]['merged_rs']
            release3_new_split_rs = metrics_per_assembly[asm]['split_rs']
            release3_new_ss_clustered = metrics_per_assembly[asm]['new_ss_clustered']

            insert_query = f"insert into dbsnp_ensembl_species.release_rs_statistics_per_assembly "\
                           f"(taxonomy_id, scientific_name, assembly_accession, release_folder, release_version, " \
                           f"current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs, " \
                           f"new_current_rs, new_multi_mapped_rs, new_merged_rs, new_deprecated_rs, " \
                           f"new_merged_deprecated_rs, new_ss_clustered, remapped_current_rs, " \
                           f"new_remapped_current_rs, split_rs, new_split_rs, ss_clustered, clustered_current_rs," \
                           f"new_clustered_current_rs) " \
                           f"values ({taxid}, '{scientific_name}', '{asm}', '{folder}', {release_version}, " \

            if asm_last_release_data:
                release2_current_rs = asm_last_release_data[0][5]
                release2_merged_rs = asm_last_release_data[0][7]
                release2_multi_mapped_rs = asm_last_release_data[0][6]
                release2_deprecated_rs = asm_last_release_data[0][8]
                release2_merged_deprecated_rs = asm_last_release_data[0][9]

                # get ss clustered
                query_ss_clustered = f"select sum(new_ss_clustered) " \
                                     f"from dbsnp_ensembl_species.release_rs_statistics_per_assembly " \
                                     f"where assembly_accession = '{asm}'"
                logger.info(query_ss_clustered)
                ss_clustered_previous_releases = get_all_results_for_query(metadata_connection_handle,
                                                                           query_ss_clustered)
                release3_ss_clustered = ss_clustered_previous_releases[0][0] + release3_new_ss_clustered

                # if assembly already existed -> add counts
                release3_current_rs = release2_current_rs + release3_new_current_rs
                release3_merged_rs = release2_merged_rs + release3_new_merged_rs
                # current_rs in previous releases (1 and 2) were all new clustered
                release3_clustered_current_rs = release2_current_rs + release3_new_clustered_current_rs

                insert_query_values = f"{release3_current_rs}, " \
                                      f"{release2_multi_mapped_rs}, " \
                                      f"{release3_merged_rs}, " \
                                      f"{release2_deprecated_rs}, " \
                                      f"{release2_merged_deprecated_rs}, " \
                                      f"{release3_new_current_rs}, " \
                                      f"0, " \
                                      f"{release3_new_merged_rs}, " \
                                      f"0, " \
                                      f"0, " \
                                      f"{release3_new_ss_clustered}, " \
                                      f"{release3_new_remapped_current_rs}, " \
                                      f"{release3_new_remapped_current_rs}, " \
                                      f"{release3_new_split_rs}, " \
                                      f"{release3_new_split_rs}, " \
                                      f"{release3_ss_clustered}," \
                                      f"{release3_clustered_current_rs}," \
                                      f"{release3_new_clustered_current_rs})"
            else:
                # if new assembly
                insert_query_values = f"{release3_new_current_rs}, " \
                                      f"0, " \
                                      f"{release3_new_merged_rs}, " \
                                      f"0, " \
                                      f"0, " \
                                      f"{release3_new_current_rs}, " \
                                      f"0, " \
                                      f"{release3_new_merged_rs}, " \
                                      f"0, " \
                                      f"0, " \
                                      f"{release3_new_ss_clustered}, " \
                                      f"{release3_new_remapped_current_rs}, " \
                                      f"{release3_new_remapped_current_rs}, " \
                                      f"{release3_new_split_rs}, " \
                                      f"{release3_new_split_rs}, " \
                                      f"{release3_new_ss_clustered}," \
                                      f"{release3_new_clustered_current_rs}," \
                                      f"{release3_new_clustered_current_rs})"
            insert_query = f"{insert_query} {insert_query_values}"
            logger.info(insert_query)
            execute_query(metadata_connection_handle, insert_query)

        # get assemblies in from release 1 and 2 not in release 3
        assemblies_in_logs = ",".join(f"'{a}'" for a in ranges_per_assembly.keys())
        query_missing_assemblies_stats = f"select * " \
                                         f"from dbsnp_ensembl_species.release_rs_statistics_per_assembly " \
                                         f"where release_version = 2 " \
                                         f"and assembly_accession not in ({assemblies_in_logs});"
        logger.info(query_missing_assemblies_stats)
        missing_assemblies_stats = get_all_results_for_query(metadata_connection_handle, query_missing_assemblies_stats)
        for assembly_stats in missing_assemblies_stats:
            taxonomy_id = assembly_stats[0]
            scientific_name = assembly_stats[1]
            assembly_accession = assembly_stats[2]
            release_folder = assembly_stats[3]
            current_rs = assembly_stats[5]
            multi_mapped_rs = assembly_stats[6]
            merged_rs = assembly_stats[7]
            deprecated_rs = assembly_stats[8]
            merged_deprecated_rs = assembly_stats[9]

            # get ss clustered
            query_ss_clustered = f"select sum(new_ss_clustered) " \
                                 f"from dbsnp_ensembl_species.release_rs_statistics_per_assembly " \
                                 f"where assembly_accession = '{assembly_accession}'"
            logger.info(query_ss_clustered)
            ss_clustered_previous_releases = get_all_results_for_query(metadata_connection_handle,
                                                                       query_ss_clustered)
            ss_clustered = ss_clustered_previous_releases[0][0]

            insert_query = f"insert into dbsnp_ensembl_species.release_rs_statistics_per_assembly "\
                           f"(taxonomy_id, scientific_name, assembly_accession, release_folder, release_version, " \
                           f"current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs, " \
                           f"new_current_rs, new_multi_mapped_rs, new_merged_rs, new_deprecated_rs, " \
                           f"new_merged_deprecated_rs, new_ss_clustered, remapped_current_rs, " \
                           f"new_remapped_current_rs, split_rs, new_split_rs, ss_clustered, clustered_current_rs, " \
                           f"new_clustered_current_rs) " \
                           f"values ({taxonomy_id}, '{scientific_name}', '{assembly_accession}', '{release_folder}', " \
                           f"3, {current_rs}, {multi_mapped_rs}, {merged_rs}, {deprecated_rs}, " \
                           f"{merged_deprecated_rs}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {ss_clustered}, 0, 0);"
            logger.info(insert_query)
            execute_query(metadata_connection_handle, insert_query)


collections = {
    "new_remapped_current_rs": [
        "clusteredVariantEntity",
        "dbsnpClusteredVariantEntity"
    ],
    "new_clustered_current_rs": [
        "clusteredVariantEntity"
    ],
    "merged_rs": [
        "clusteredVariantOperationEntity",
        "dbsnpClusteredVariantOperationEntity"
    ],
    "split_rs": [
        "clusteredVariantOperationEntity",
        "dbsnpClusteredVariantOperationEntity"
    ],
    "new_ss_clustered": [
        "submittedVariantOperationEntity",
        "dbsnpSubmittedVariantOperationEntity"
    ]
}


def parse_log_file_path(log_file_path):
    scientific_name_taxonomy_id, assembly_accession, file_name = log_file_path.split('/')[-3:]
    scientific_name = '_'.join(scientific_name_taxonomy_id.split('_')[:-1])
    taxid = scientific_name_taxonomy_id.split('_')[-1]
    date = datetime.strptime(file_name.split('.')[0].split('_')[-1], '%Y%m%d%H%M%S')  # 20220112170519
    return scientific_name, taxid, assembly_accession, date


def parse_one_log(log_file):
    # identify the clustering job/step
    # identify the start end of run
    # find the count lines and extract metrics
    results = {}
    with open(log_file) as open_file:
        for line in open_file:
            sp_line = line.strip().split()
            if len(sp_line) < 8:
                continue

            # get last timestamp
            try:
                timestamp = datetime.strptime(f"{sp_line[0]}T{sp_line[1]}Z", '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                pass

            # Jobs
            if sp_line[7] == 'o.s.b.c.l.support.SimpleJobLauncher':
                if sp_line[12] == "launched" or sp_line[12] == "completed":
                    current_job = sp_line[11].rstrip(']').lstrip('[name=')
                    job_status = sp_line[12]
                    if current_job not in results:
                        results[current_job] = {}
                    if job_status not in results[current_job]:
                        results[current_job][job_status] = {}
                    results[current_job][job_status] = timestamp
            # Steps
            if sp_line[7] == 'o.s.batch.core.job.SimpleStepHandler':
                current_step = sp_line[11].rstrip(']').lstrip('[')
                if current_step not in results:
                    results[current_step] = {}
                results[current_step] = timestamp

    results["last_timestamp"] = timestamp
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Parse all the clustering logs to get date ranges and query mongo to get metrics counts')
    parser.add_argument("--clustering_root_path", type=str,
                        help="base directory where all the clustering was run.", required=True)
    parser.add_argument("--mongo-source-uri",
                        help="Mongo Source URI (ex: mongodb://user:@mongos-source-host:27017/admin)", required=True)
    parser.add_argument("--mongo-source-secrets-file",
                        help="Full path to the Mongo Source secrets file (ex: /path/to/mongo/source/secret)",
                        required=True)
    parser.add_argument('--private_config_xml_file', help='Path to the file containing the ', required=True)

    args = parser.parse_args()
    mongo_source = MongoDatabase(uri=args.mongo_source_uri, secrets_file=args.mongo_source_secrets_file,
                                 db_name="eva_accession_sharded")
    gather_count_from_mongo(args.clustering_root_path, mongo_source, args.private_config_xml_file)


if __name__ == '__main__':
    main()
