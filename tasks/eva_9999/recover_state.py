import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.config_utils import get_mongo_creds_for_profile, get_properties_from_xml_file
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query, execute_query

logger = logging_config.get_logger(__name__)


def get_accession_job_tracker_creds_for_profile(profile_name: str, settings_xml_file: str):
    """
    Gets host, username, and password for variant load job tracker database.
    Useful for filling properties files.
    """
    properties = get_properties_from_xml_file(profile_name, settings_xml_file)
    accession_url = properties['eva.accession.jdbc.url']
    accession_user = properties['eva.accession.user']
    accession_pass = properties['eva.accession.password']
    return accession_url, accession_user, accession_pass


def get_incomplete_blocks(profile_name, settings_xml_file, instance=None):
    query = (
        f"SELECT id, application_instance_id, first_value, last_value FROM contiguous_id_blocks "
        f"WHERE category_id='ss' AND last_committed<>last_value "
    )
    if instance:
        query += f"AND application_instance_id ='instance-{instance}'"
    with get_accession_job_tracker_creds_for_profile(profile_name, settings_xml_file) as pg_conn:
        return get_all_results_for_query(pg_conn, query)


def count_submitted_variant_in_range(start, end, profile_name, settings_xml_file):
    mongo_host, mongo_user, mongo_password = get_mongo_creds_for_profile(profile_name, settings_xml_file)
    with get_mongo_connection_handle(
            username=mongo_user,
            password=mongo_password,
            host=mongo_host
    ) as accessioning_mongo_handle:
        aggregate_pipeline = [
            {"$match": {"accession": {"$gt": start - 1, "$lt": end + 1}}},
            {"$group": {"_id": "$accession"}}, {"$count": "count"}
        ]
        sve_collection = accessioning_mongo_handle["eva_accession_sharded"]["submittedVariantEntity"]
        count_eva = sve_collection.aggregate(aggregate_pipeline, {"allowDiskUse": True})
        sve_collection = accessioning_mongo_handle["eva_accession_sharded"]["dbsnpSubmittedVariantEntity"]
        count_dbsnp = sve_collection.aggregate(aggregate_pipeline, {"allowDiskUse": True})
        print(count_eva, count_dbsnp)
        return count_eva + count_dbsnp


def update_block(profile_name, settings_xml_file, block_id):
    with get_accession_job_tracker_creds_for_profile(profile_name, settings_xml_file) as pg_conn:
        query = f"UPDATE public.contiguous_id_blocks SET last_committed = last_value WHERE id={block_id}"
        execute_query(pg_conn, query)


def check_block(profile_name, settings_xml_file, instance, update=False):
    for block in get_incomplete_blocks(profile_name, settings_xml_file, instance):
        block_id, application_instance_id, first_value, last_value = block
        count_accession = count_submitted_variant_in_range(first_value, last_value, profile_name, settings_xml_file)
        if count_accession == last_value - first_value + 1:
            if update:
                logger.info(f'Block id {block_id} starting at {first_value} and ending at {last_value} has all {count_accession} used')
                update_block(block_id)
        else:
            logger.info(f'Block id {block_id} starting at {first_value} and ending at {last_value} only has {count_accession} used')


def main():
    argparser = ArgumentParser(description='Check the existing blocks in the accessioning tracker and confirm if the '
                                           'accessions have been committed in mongo.')
    argparser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    argparser.add_argument("--profile", help="The profile, in the config file, that should be used", required=True)
    argparser.add_argument("--instance", help="Only search this instance", required=False, default=None)
    args = argparser.parse_args()

    logging_config.add_stdout_handler(level=logging.INFO)
    check_block(args.private_config_xml_file, args.profile, args.instance)


if __name__ == "__main__":
    main()
