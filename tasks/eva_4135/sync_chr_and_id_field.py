import argparse
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_common_pyutils.logger import logging_config
from pymongo import UpdateOne

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)

BATCH_SIZE = 1000
ANNOTATION_COLL_NAME = 'annotations_2_0'


def process_annotations(db_name, annotation_coll):
    modified_count = 0
    cursor = annotation_coll.find({}, {'_id': 1, 'chr': 1}).batch_size(BATCH_SIZE).allow_disk_use(True)

    batch = []
    for variant in cursor:
        batch.append(variant)
        if len(batch) < BATCH_SIZE:
            continue

        # process variants in batch
        modified_count += process_batch(db_name, batch, annotation_coll)
        batch = []

    # process remaining variants
    if batch:
        modified_count += process_batch(db_name, batch, annotation_coll)

    logger.info(f"[{db_name}] Done. Total modified: {modified_count}")


def process_batch(db_name, batch, annotation_coll):
    ids_modified = []
    bulk_ops = []
    for variant in batch:
        chr_from_id = variant.get('_id').split("_")[0]
        chr_from_field = variant.get('chr')
        if chr_from_id != chr_from_field:
            bulk_ops.append(UpdateOne({'_id': variant['_id']}, {'$set': {'chr': chr_from_id}}))
            ids_modified.append(variant['_id'])

    if bulk_ops:
        result = annotation_coll.bulk_write(bulk_ops, ordered=False)
        logger.info(f"[{db_name}] Modified in batch: {result.modified_count}. Ids modified: {ids_modified}")
        return result.modified_count
    else:
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Find and update annotations which have different chromosome name in Id and chr field',
        add_help=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument('--profile', default='localhost')
    parser.add_argument('--db-name', help='Database name to run the script on', required=False)
    args = parser.parse_args()

    with get_mongo_connection_handle(args.profile, args.private_config_xml_file) as mongo_conn:
        db_list = []
        if args.db_name:
            db_list.append(args.db_name)
        else:
            db_list = mongo_conn.list_database_names()

        for db_name in db_list:
            if db_name in ['admin', 'config', 'local']:
                continue
            db = mongo_conn[db_name]
            if ANNOTATION_COLL_NAME in db.list_collection_names():
                logger.info(f"Processing Database: {db_name}")
                annotation_coll = db[ANNOTATION_COLL_NAME]
                # Process mis-matched annotations
                process_annotations(db_name, annotation_coll)


if __name__ == '__main__':
    main()
