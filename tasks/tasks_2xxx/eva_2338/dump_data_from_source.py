# Copyright 2021 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import sys
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.logger import logging_config

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def dump_data_from_source(mongo_source: MongoDatabase, top_level_dump_dir):
    try:
        logger.info("Running mongodump from source...")

        # Force table scan is performant for many workloads avoids cursor timeout issues
        # See https://jira.mongodb.org/browse/TOOLS-845?focusedCommentId=988298&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-988298
        mongo_source.dump_data(dump_dir=os.path.join(top_level_dump_dir, mongo_source.db_name),
                               mongodump_args={"forceTableScan": "", "numParallelCollections": "1"})
    except Exception as ex:
        logger.error(f"Error while dumping data from source!\n{ex.__str__()}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Dump data from a given MongoDB source',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--mongo-source-uri",
                        help="Mongo Source URI (ex: mongodb://user:@mongos-source-host:27017/admin)", required=True)
    parser.add_argument("--mongo-source-secrets-file",
                        help="Full path to the Mongo Source secrets file (ex: /path/to/mongo/source/secret)",
                        required=True)
    parser.add_argument("--db-name", help="Database to migrate (ex: eva_hsapiens_grch37)", required=True)
    parser.add_argument("--dump-dir", help="Top-level directory where all dumps reside (ex: /path/to/dumps)",
                        required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = parser.parse_args()
    dump_data_from_source(MongoDatabase(uri=args.mongo_source_uri, secrets_file= args.mongo_source_secrets_file,
                                        db_name=args.db_name), top_level_dump_dir=args.dump_dir)


if __name__ == "__main__":
    main()
