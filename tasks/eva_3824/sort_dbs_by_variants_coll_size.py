import argparse
import csv

from ebi_eva_common_pyutils.logger import logging_config
from pymongo import MongoClient

logging_config.add_stdout_handler()
logger = logging_config.get_logger(__name__)


def get_variants_collection_size(db):
    # Returns the size of the collection 'variants_2_0' if it exists, else 0.
    if 'variants_2_0' in db.list_collection_names():
        stats = db.command("collstats", "variants_2_0")
        return stats.get("size", 0)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Sort the MongoDB databases based on the size of the collection variants_2_0")
    parser.add_argument("--mongouri", help="MongoDB URI")
    parser.add_argument("--output_file", help="Output CSV file to save the results")
    args = parser.parse_args()

    client = MongoClient(args.mongouri)
    db_sizes = []

    for db_name in client.list_database_names():
        if db_name in ['config']:
            continue
        db = client[db_name]
        size = get_variants_collection_size(db)
        if size > 0:
            db_sizes.append((db_name, size))
        else:
            logger.warn(f"DB {db_name} does not have collection variants_2_0 or it does not contains any variants")

    sorted_dbs = sorted(db_sizes, key=lambda x: x[1])

    # Write the results to file
    with open(args.output_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        for db_name, size in sorted_dbs:
            writer.writerow([db_name, size])


if __name__ == "__main__":
    main()
