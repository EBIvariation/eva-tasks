import argparse

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)

BATCH_SIZE = 5000
ANNOTATION_COLL_NAME = 'annotations_2_0'

# Matches docs where at least one consequence type has a null in its `so` array or `so` itself is null.
NULL_SO_FILTER = {"ct": {"$elemMatch": {"so": {"$type": "null"}}}}


def clean_collection(collection, batch_size, apply):
    total_to_fix = collection.count_documents(NULL_SO_FILTER)
    logger.info("%s.%s: %d documents contain null SO terms",
                collection.database.name, collection.name, total_to_fix)
    if total_to_fix == 0 or not apply:
        return 0

    fixed = 0
    cursor = collection.find(NULL_SO_FILTER, {"_id": 1}, no_cursor_timeout=True)
    try:
        batch = []
        for doc in cursor:
            batch.append(doc["_id"])
            if len(batch) >= batch_size:
                fixed += _pull_nulls(collection, batch)
                logger.info("  modified %d / %d", fixed, total_to_fix)
                batch = []
        if batch:
            fixed += _pull_nulls(collection, batch)
    finally:
        cursor.close()

    return fixed


def _pull_nulls(collection, ids):
    result = collection.update_many(
        {"_id": {"$in": ids}},
        {"$pull": {"ct.$[].so": None}}
    )
    return result.modified_count


def main():
    parser = argparse.ArgumentParser(
        description='Find and remove null SO values in consequenceType in annotation collections of the db',
        add_help=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument('--profile', default='localhost')
    parser.add_argument('--db-name', help='Database name to run the script on', required=False)
    parser.add_argument("--apply", action="store_true", help="Actually modify data (default is dry run)")
    args = parser.parse_args()

    if not args.apply:
        logger.info("DRY RUN: no data will be modified. Use --apply to fix.")

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

                clean_collection(annotation_coll, BATCH_SIZE, args.apply)


if __name__ == '__main__':
    main()
