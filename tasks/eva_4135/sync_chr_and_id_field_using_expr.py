import argparse
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_common_pyutils.logger import logging_config

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)

ANNOTATION_COLL_NAME = 'annotations_2_0'

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
                # update mis-matched annotations
                result = annotation_coll.update_many(
                    {
                        "$expr": {
                            "$ne": [
                                "$chr",
                                {"$arrayElemAt": [{"$split": ["$_id", "_"]}, 0]}
                            ]
                        }
                    },
                    [
                        {
                            "$set": {
                                "chr": {
                                    "$arrayElemAt": [
                                        {"$split": ["$_id", "_"]},
                                        0
                                    ]
                                }
                            }
                        }
                    ]
                )

            logger.info(f"[{db_name}] Done. Total matched/modified : {result.matched_count} / {result.modified_count}")


if __name__ == '__main__':
    main()
