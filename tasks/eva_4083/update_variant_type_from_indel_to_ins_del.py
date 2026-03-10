import argparse
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_common_pyutils.logger import logging_config
from pymongo import UpdateOne

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)

BATCH_SIZE = 1000
VARIANT_COLL_NAME = 'variants_2_0'
QUERY = {
    'type': 'INDEL',
    '$or': [
        {'ref': {'$in': ['', None]}},
        {'alt': {'$in': ['', None]}}
    ]
}


def get_variant_type(ref, alt):
    if ref is None or ref == '':
        return 'INS'
    elif alt is None or alt == '':
        return 'DEL'
    else:
        return None


def process_indel_variants(db_name, variant_coll):
    modified_count = 0
    cursor = variant_coll.find(QUERY, {'_id': 1, 'ref': 1, 'alt': 1}).batch_size(BATCH_SIZE)

    batch = []
    for variant in cursor:
        batch.append(variant)
        if len(batch) < BATCH_SIZE:
            continue

        # process variants in batch
        modified_count += process_batch(db_name, batch, variant_coll)
        batch = []

    # process remaining variants
    if batch:
        modified_count += process_batch(db_name, batch, variant_coll)

    logger.info(f"[{db_name}] Done. Total modified: {modified_count}")


def process_batch(db_name, batch, variant_coll):
    ids_modified = []
    bulk_ops = []
    for variant in batch:
        new_type = get_variant_type(variant.get('ref'), variant.get('alt'))
        if new_type is not None:
            bulk_ops.append(
                UpdateOne({'_id': variant['_id']}, {'$set': {'type': new_type}})
            )
            ids_modified.append(variant['_id'])

    if bulk_ops:
        result = variant_coll.bulk_write(bulk_ops, ordered=False)
        logger.info(f"[{db_name}] Modified in batch: {result.modified_count}. Ids modified: {ids_modified}")
        return result.modified_count
    else:
        logger.info(f"[{db_name}] No modification in batch")
        return 0


def main():
    parser = argparse.ArgumentParser(description='Find and update variants with type INDEL to INS/DEL', add_help=True)
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
            if VARIANT_COLL_NAME in db.list_collection_names():
                logger.info(f"Processing Database: {db_name}")
                variant_coll = db[VARIANT_COLL_NAME]
                # Process indel variants
                process_indel_variants(db_name, variant_coll)


if __name__ == '__main__':
    main()
