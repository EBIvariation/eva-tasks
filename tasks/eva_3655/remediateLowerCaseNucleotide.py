import argparse
import logging
import os

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

logger = log_cfg.get_logger(__name__)
log_cfg.add_stdout_handler()
log_cfg.set_log_level(logging.INFO)


def remediate_lower_case_nucleotide(working_dir, private_config_xml_file, profile, db_name):
    with get_mongo_connection_handle(profile, private_config_xml_file) as mongo_conn:
        variants_coll = mongo_conn[db_name]['variants_2_0']
        files_coll = mongo_conn[db_name]['files_2_0']
        # regex for finding candidates with lowercase in ref or alt
        regex_pattern = r'^[a-zA-Z0-9_.]+_[0-9]+_([a-zA-Z]*[a-z]+[a-zA-Z]*_?[a-zA-Z]*|[a-zA-Z]*_?[a-zA-Z]*[a-z]+[a-zA-Z]*)$'
        cursor = variants_coll.find({'_id': {'$regex': regex_pattern}})
        if cursor:
            for variant in cursor:
                # double check if the candidate variant actually has lowercase ref or alt
                if has_lowercase_ref_or_alt(variant):
                    # create new id by making ref and alt uppercase
                    new_id = f"{variant['chr']}_{variant['start']}_{variant['ref'].upper()}_{variant['alt'].upper()}"

                    # check if new id is present in db and get the corresponding variant
                    variant_in_db = variants_coll.find_one({'_id': new_id})

                    if variant_in_db:
                        # variant with new db present, needs to check for merging
                        variant_files_list = variant.get("files", [])
                        variant_in_db_files_list = variant_in_db.get("files", [])
                        variant_sid_fid_tuple_set = set(
                            (file_obj["sid"], file_obj["fid"]) for file_obj in variant_files_list)
                        variant_in_db_sid_fid_tuple_set = set(
                            (file_obj["sid"], file_obj["fid"]) for file_obj in variant_in_db_files_list)

                        common_pairs = variant_sid_fid_tuple_set.intersection(variant_in_db_sid_fid_tuple_set)

                        if common_pairs:
                            # check if there is any combination of sid and fid for which there are more than one entry in files collection
                            result = get_sid_fid_number_of_documents(files_coll, common_pairs)
                            sid_fid_gt_one = [k for k, v in result.items() if len(v) > 1]
                            if sid_fid_gt_one:
                                remediate_case_cant_merge(working_dir, sid_fid_gt_one, db_name)
                            else:
                                sid_fid_not_present_in_db = variant_sid_fid_tuple_set.difference(common_pairs)
                                remediate_case_merge_all_common_sid_fid_has_one_file(variants_coll, variant,
                                                                                     sid_fid_not_present_in_db, new_id)
                        else:
                            remediate_case_merge_all_sid_fid_different(variants_coll, variant, new_id)
                    else:
                        # no merge required as new id is not present
                        remediate_case_no_id_collision(variants_coll, variant, new_id)
                else:
                    # if the variant ref or alt does not contain any lowercase letters, flag it as an invalid candidate
                    logger.error(f"Variant with Id {variant['_id']} is not a valid candidate")
        else:
            logger.info(f"No candidate found in database: {db_name}")


def has_lowercase_ref_or_alt(variant):
    if any(char.islower() for char in variant['ref']) or any(char.islower() for char in variant['alt']):
        return True
    else:
        return False


def remediate_case_no_id_collision(variants_coll, variant, new_id):
    logger.info(f"variant to be deleted (case no id collision):  {variant}")

    variant_old_id = variant['_id']

    # update all the relevant fields uppercase
    variant['_id'] = new_id
    variant['ref'] = variant['ref'].upper()
    variant['alt'] = variant['alt'].upper()
    variant['files'] = uppercase_variant_files(variant)
    variant['hgvs'] = uppercase_variant_hgvs(variant)
    variant['st'] = uppercase_variant_st(variant)

    # insert updated variant and delete the existing one with lowercase
    logger.info(f"Inserting variant (case no id collision):  {variant}")
    variants_coll.insert_one(variant)
    variants_coll.delete_one({'_id': variant_old_id})


def remediate_case_merge_all_sid_fid_different(variants_coll, variant, new_id):
    update = {
        "$push": {
            "files": {"$each": uppercase_variant_files(variant)},
            "hgvs": {"$each": uppercase_variant_hgvs(variant)},
            "st": {"$each": uppercase_variant_st(variant)},
        }
    }
    # update the existing uppercase variant with values from lowercase variant and delete the lowercase variant thereafter
    logger.info(f"Inserting variant (case merge all sid fid diff):  {variant}")
    variants_coll.update_one({"_id": new_id}, update)
    logger.info(f"Variant to be deleted (case merge all sid fid diff):  {variant}")
    variants_coll.delete_one({'_id': variant['_id']})


def remediate_case_cant_merge(working_dir, sid_fid_gt_one, db_name):
    nmc_dir_path = os.path.join(working_dir, 'non_merged_candidates')
    os.makedirs(nmc_dir_path, exist_ok=True)
    nmc_file_path = os.path.join(nmc_dir_path, f"{db_name}.txt")

    with open(nmc_file_path, 'a') as nmc_file:
        for p in sid_fid_gt_one:
            nmc_file.write(f"{p}\n")


def remediate_case_merge_all_common_sid_fid_has_one_file(variants_coll, variant, sid_fid_not_present_in_db, new_id):
    candidate_files = [file_obj for file_obj in variant.get('files', []) if
                       (file_obj['sid'], file_obj['fid']) in sid_fid_not_present_in_db]
    candidate_st = [st_obj for st_obj in variant.get('st', []) if
                    (st_obj['sid'], st_obj['fid']) in sid_fid_not_present_in_db]
    # TODO:hgvs

    update = {
        "$push": {
            "files": {"$each": uppercase_variant_files({'files': candidate_files})},
            "hgvs": {"$each": uppercase_variant_hgvs({'hgvs': []})},
            "st": {"$each": uppercase_variant_st({'st': candidate_st})},
        }
    }

    # update the existing uppercase variant with values from lowercase variant and delete the lowercase variant thereafter
    logger.info(f"Inserting variant (case merge sid fid have only one file):  {variant}")
    variants_coll.update_one({"_id": new_id}, update)
    logger.info(f"Variant to be deleted (case merge sid fid have only one file):  {variant}")
    variants_coll.delete_one({'_id': variant['_id']})


def uppercase_variant_files(variant):
    return [
        {**file_obj, 'alts': file_obj['alts'].upper()} if 'alts' in file_obj else file_obj
        for file_obj in variant.get("files", [])
    ]


def uppercase_variant_hgvs(variant):
    start = str(variant.get('start', ''))

    def uppercase_after_start(name):
        start_index = name.find(start)
        if start_index == -1:
            return name
        return name[:start_index + len(start)] + name[start_index + len(start):].upper()

    return [
        {**hgvs_obj, 'name': uppercase_after_start(hgvs_obj['name'])} if 'name' in hgvs_obj else hgvs_obj
        for hgvs_obj in variant.get("hgvs", [])
    ]


def uppercase_variant_st(variant):
    return [
        {**st_obj, 'mafAl': st_obj['mafAl'].upper()} if 'mafAl' in st_obj else st_obj
        for st_obj in variant.get("st", [])
    ]


def get_documents_for_each_sid_fid_combination(files_coll, common_pairs):
    query = {"$or": [{"sid": sid, "fid": fid} for sid, fid in common_pairs]}
    documents = list(files_coll.find(query))
    result = {pair: [] for pair in common_pairs}
    for doc in documents:
        result[(doc['sid'], doc['fid'])].append(doc)

    return result


def get_sid_fid_number_of_documents(files_coll, common_pairs):
    query = {"$or": [{"sid": sid, "fid": fid} for sid, fid in common_pairs]}
    documents = list(files_coll.find(query))
    result = {pair: [] for pair in common_pairs}
    for doc in documents:
        result[(doc['sid'], doc['fid'])].append(doc)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find and remediate variants with lowercase nucleotide',
                                     add_help=False)
    parser.add_argument("--working-dir", help="/path/to/dir where all the logs and other files will be stored",
                        required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile",
                        help="ex: environment in which to run the script e.g. localhost, development, production_processing",
                        required=True)
    parser.add_argument("--db-list", help="List of variant warehouse DBs space-separated",
                        required=True, nargs='+')
    args = parser.parse_args()

    for db_name in args.db_list:
        remediate_lower_case_nucleotide(args.working_dir, args.private_config_xml_file, args.profile, db_name)
