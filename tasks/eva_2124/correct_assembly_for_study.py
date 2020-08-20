#!/usr/bin/env python
import hashlib
from argparse import ArgumentParser

import pymongo
from ebi_eva_common_pyutils.mongo_utils import get_mongo_connection_handle
from tasks.eva_2124.load_synonyms import load_synonyms_for_assembly

def get_SHA1(variant_rec):
    """Calculate the SHA1 digest from the ref, study, contig, start, ref, and alt attributes of the variant"""
    h = hashlib.sha1()
    keys = ['seq', 'study', 'contig', 'start', 'ref', 'alt']
    h.update('_'.join([str(variant_rec[key]) for key in keys]).encode())
    return h.hexdigest().upper()


def get_genbank(synonym_dictionaries, contig):
    """
    returns a tuple (genbank, was_already_genbank) or raises an exception if the contig was not found
    """
    by_name, by_assigned_molecule, by_genbank, by_refseq, by_ucsc = synonym_dictionaries
    if contig in by_name:
        return by_name[contig]['genbank'], False

    if contig in by_assigned_molecule:
        return by_assigned_molecule[contig]['genbank'], False

    if contig in by_ucsc:
        return by_ucsc[contig]['genbank'], False

    if contig in by_refseq and by_refseq[contig]['is_genbank_refseq_identical']:
        return by_refseq[contig]['genbank'], False

    if contig in by_genbank:
        return contig, True

    raise Exception('could not find synonym for contig {}'.format(contig))


def correct(mongo_user, mongo_password, mongo_host, studies, assembly_accession, chunk_size = 1000):
    """
    Connect to mongodb and retrieve all variants the should be updated, check their key and update them in bulk.
    """
    with get_mongo_connection_handle(
            username=mongo_user,
            password=mongo_password,
            host=mongo_host
    ) as accessioning_mongo_handle:
        sve_collection = accessioning_mongo_handle["eva_accession_sharded"]["submittedVariantEntity"]
        cursor = sve_collection.find({'study': {'$in': studies}, 'seq': assembly_accession})
        synonym_dictionaries = load_synonyms_for_assembly(assembly_accession)

        insert_statements = []
        drop_statements = []
        record_checked = 0
        already_genbanks = 0
        total_inserted = 0
        total_dropped = 0
        for variant in cursor:
            # Ensure that the variant we are changing has the expected SHA1
            original_id = get_SHA1(variant)
            assert variant['_id'] == original_id, "Original id is different from the one calculated %s != %s" % (variant['_id'], original_id)
            genbank, was_already_genbank = get_genbank(synonym_dictionaries, variant['contig'])
            if was_already_genbank:
                already_genbanks += 1
            else:
                variant['contig'] = genbank
                variant['_id'] = get_SHA1(variant)
                insert_statements.append(pymongo.InsertOne(variant))
                drop_statements.append(pymongo.DeleteOne({'_id': original_id}))
            record_checked += 1
            if len(insert_statements) >= chunk_size:
                total_inserted, total_dropped = execute_bulk(drop_statements, insert_statements, sve_collection,
                                                             total_dropped, total_inserted)

        if len(insert_statements) > 0:
            total_inserted, total_dropped = execute_bulk(drop_statements, insert_statements, sve_collection,
                                                         total_dropped, total_inserted)
        print('Retrieved %s documents and checked matching Sha1 hash' % record_checked)
        print('{} of those documents had already a genbank contig. If the projects were all affected, '
              'that number should be 0, but even if it is not, there is nothing else to fix'.format(already_genbanks))

        print('There was %s new documents inserted' % total_inserted)
        print('There was %s old documents dropped' % total_dropped)
        return total_inserted


def execute_bulk(drop_statements, insert_statements, sve_collection, total_dropped, total_inserted):
    result_insert = sve_collection.bulk_write(requests=insert_statements, ordered=False)
    total_inserted += result_insert.inserted_count
    result_drop = sve_collection.bulk_write(requests=drop_statements, ordered=False)
    total_dropped += result_drop.deleted_count
    insert_statements.clear()
    drop_statements.clear()
    return total_inserted, total_dropped


def main():
    argparse = ArgumentParser()
    argparse.add_argument('--mongo_user', help='user to connect to mongodb', required=True)
    argparse.add_argument('--mongo_password', help='password to connect to mongodb', required=True)
    argparse.add_argument('--mongo_host', help='host to connect to mongodb', required=True)
    argparse.add_argument('--studies', help='The studies in the assembly to correct', required=True)
    argparse.add_argument('--assembly', help='the assembly accession of the entities that needs to be changed',
                          required=True)

    args = argparse.parse_args()
    correct(args.mongo_user, args.mongo_password, args.mongo_host, args.studies.split(','), args.assembly)


if __name__ == "__main__":
    main()