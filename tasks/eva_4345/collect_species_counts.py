#!/usr/bin/env python
"""
Collect, for every species (taxonomy id) EVA holds data for, the number of studies, analyses, files, samples,
submitted variants (SS), clustered variants (RS), the assemblies with RS IDs and the currently supported assembly.

Sources:
  - EVAPRO: public EVA studies, analyses, files, samples and the supported assembly.
  - Accessioning warehouse (MongoDB): SS counts and dbSNP studies per taxonomy (remapped copies excluded).
  - Release web services: current RS counts per species and assemblies with RS IDs for the release.

One row per taxonomy id, annotated with the species, kingdom and superkingdom names from NCBI taxonomy (ete3).
"""
import argparse
import csv
from collections import defaultdict
from functools import lru_cache

import requests
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query
from pymongo import ReadPreference

logger = log_cfg.get_logger(__name__)

RELEASE_WS = 'https://www.ebi.ac.uk/eva/webservices/release'
LATEST_RELEASE_URL = f'{RELEASE_WS}/v1/info/latest'
PER_SPECIES_URL = RELEASE_WS + '/v2/stats/per-species?releaseVersion={release_version}'
PER_ASSEMBLY_URL = RELEASE_WS + '/v2/stats/per-assembly?releaseVersion={release_version}'

ACCESSION_DB = 'eva_accession_sharded'
EVA_SS_COLLECTION = 'submittedVariantEntity'
DBSNP_SS_COLLECTION = 'dbsnpSubmittedVariantEntity'

# Test projects excluded from the counts (same as eva_3205/study_metrics.ipynb)
EXCLUDED_PROJECTS = ('PRJEBZ0001', 'PRJX00001')
PUBLIC_PROJECT_FILTER = 'p.ena_status=4 AND p.eva_status=1 AND p.project_accession NOT IN ({})'.format(
    ', '.join(f"'{p}'" for p in EXCLUDED_PROJECTS)
)
REQUIRED_RANKS = ['superkingdom', 'kingdom', 'species']
SEPARATOR = ';'

OUTPUT_HEADER = [
    'taxonomy_id', 'scientific_name', 'common_name', 'species', 'kingdom', 'superkingdom',
    'eva_studies', 'dbsnp_studies', 'total_studies', 'analyses', 'files', 'samples',
    'eva_submitted_variants', 'dbsnp_submitted_variants', 'submitted_variants',
    'clustered_variants', 'assemblies_with_rs', 'supported_assembly'
]


def _taxid_for_analysis_cte():
    """
    Public, non-hidden analyses with the taxonomy of their assembly set, falling back to the taxonomy of
    the project when the analysis has no assembly set.
    """
    return (
        'WITH public_analysis AS ('
        ' SELECT DISTINCT a.analysis_accession, COALESCE(asm.taxonomy_id, pt.taxonomy_id) AS taxonomy_id'
        ' FROM evapro.project p'
        ' JOIN evapro.project_analysis pa ON pa.project_accession = p.project_accession'
        ' JOIN evapro.analysis a ON a.analysis_accession = pa.analysis_accession'
        ' LEFT JOIN evapro.assembly_set asm ON asm.assembly_set_id = a.assembly_set_id'
        ' LEFT JOIN evapro.project_taxonomy pt ON pt.project_accession = p.project_accession'
        f' WHERE {PUBLIC_PROJECT_FILTER} AND (a.hidden_in_eva IS NULL OR a.hidden_in_eva = 0)'
        '), public_file AS ('
        ' SELECT DISTINCT pa.taxonomy_id, f.file_id'
        ' FROM public_analysis pa'
        ' JOIN evapro.analysis_file af ON af.analysis_accession = pa.analysis_accession'
        ' JOIN evapro.file f ON f.file_id = af.file_id'
        " WHERE f.is_current = 1 AND upper(f.file_type) LIKE 'VCF%'"
        ') '
    )


def get_evapro_counts(metadata_conn):
    """Return taxonomy_id -> dict of EVA study set, analysis, file and sample counts, plus taxonomy names."""
    counts = defaultdict(lambda: {'eva_study_set': set(), 'analyses': 0, 'files': 0, 'samples': 0})

    query = (
        'SELECT pt.taxonomy_id, p.project_accession FROM evapro.project p'
        ' JOIN evapro.project_taxonomy pt ON pt.project_accession = p.project_accession'
        f' WHERE {PUBLIC_PROJECT_FILTER}'
    )
    for taxonomy_id, project_accession in get_all_results_for_query(metadata_conn, query):
        counts[taxonomy_id]['eva_study_set'].add(project_accession)

    query = _taxid_for_analysis_cte() + (
        'SELECT taxonomy_id, count(DISTINCT analysis_accession) FROM public_analysis GROUP BY taxonomy_id'
    )
    for taxonomy_id, nb_analysis in get_all_results_for_query(metadata_conn, query):
        counts[taxonomy_id]['analyses'] = nb_analysis

    query = _taxid_for_analysis_cte() + (
        'SELECT taxonomy_id, count(DISTINCT file_id) FROM public_file GROUP BY taxonomy_id'
    )
    for taxonomy_id, nb_file in get_all_results_for_query(metadata_conn, query):
        counts[taxonomy_id]['files'] = nb_file

    query = _taxid_for_analysis_cte() + (
        'SELECT pf.taxonomy_id, count(DISTINCT fs.sample_id) FROM public_file pf'
        ' JOIN evapro.file_sample fs ON fs.file_id = pf.file_id GROUP BY pf.taxonomy_id'
    )
    for taxonomy_id, nb_sample in get_all_results_for_query(metadata_conn, query):
        counts[taxonomy_id]['samples'] = nb_sample

    # Drop the analyses/files with no taxonomy at all
    counts.pop(None, None)
    return counts


def get_evapro_taxonomy_names(metadata_conn):
    query = 'SELECT taxonomy_id, scientific_name, common_name FROM evapro.taxonomy'
    return {t: (s, c) for t, s, c in get_all_results_for_query(metadata_conn, query)}


def get_supported_assemblies(metadata_conn):
    """Return taxonomy_id -> sorted list of currently supported assembly accessions."""
    query = 'SELECT DISTINCT taxonomy_id, assembly_id FROM evapro.supported_assembly_tracker WHERE current = true'
    supported = defaultdict(set)
    for taxonomy_id, assembly in get_all_results_for_query(metadata_conn, query):
        supported[taxonomy_id].add(assembly)
    return {t: sorted(a) for t, a in supported.items()}


def get_mongo_ss_counts(mongo_handle, collection_name):
    """
    Scan one submitted variant collection once and return taxonomy_id -> (ss_count, set of studies).
    Remapped copies of submitted variants are excluded so each submitted variant is counted once.
    """
    logger.info(f'Aggregating SS counts per taxonomy and study in {ACCESSION_DB}.{collection_name}')
    collection = mongo_handle[ACCESSION_DB][collection_name]
    pipeline = [
        {'$match': {'remappedFrom': {'$exists': False}}},
        {'$group': {'_id': {'tax': '$tax', 'study': '$study'}, 'n': {'$sum': 1}}},
    ]
    ss_counts = defaultdict(int)
    studies = defaultdict(set)
    for doc in collection.aggregate(pipeline, allowDiskUse=True):
        taxonomy_id = doc['_id'].get('tax')
        ss_counts[taxonomy_id] += doc['n']
        if doc['_id'].get('study'):
            studies[taxonomy_id].add(doc['_id']['study'])
    logger.info(f'Found {sum(ss_counts.values())} SS across {len(ss_counts)} taxonomies in {collection_name}')
    return {t: (ss_counts[t], studies[t]) for t in ss_counts}


def _get_json(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_latest_release_version():
    return _get_json(LATEST_RELEASE_URL)['releaseVersion']


def get_release_counts(release_version):
    """Return taxonomy_id -> {'clustered_variants': currentRs, 'assemblies_with_rs': [assembly accessions]}."""
    release_counts = defaultdict(lambda: {'clustered_variants': 0, 'assemblies_with_rs': set()})
    for species in _get_json(PER_SPECIES_URL.format(release_version=release_version)):
        release_counts[species['taxonomyId']]['clustered_variants'] += species.get('currentRs') or 0
    for assembly in _get_json(PER_ASSEMBLY_URL.format(release_version=release_version)):
        if (assembly.get('currentRs') or 0) > 0:
            for taxonomy_id in assembly.get('taxonomyIds') or []:
                release_counts[taxonomy_id]['assemblies_with_rs'].add(assembly['assemblyAccession'])
    return release_counts


@lru_cache(maxsize=None)
def _ncbi_taxa():
    from ete3 import NCBITaxa
    return NCBITaxa()


@lru_cache(maxsize=None)
def annotate_taxonomy(taxonomy_id):
    """Return a dict with the scientific name of the taxonomy and of its species, kingdom and superkingdom."""
    annotation = dict.fromkeys(REQUIRED_RANKS + ['scientific_name'])
    ete_db = _ncbi_taxa()
    try:
        lineage = ete_db.get_lineage(int(taxonomy_id))
    except ValueError:
        logger.warning(f'Taxonomy {taxonomy_id} not found in the local NCBI taxonomy database')
        return annotation
    ranks = ete_db.get_rank(lineage)
    names = ete_db.get_taxid_translator(lineage)
    for taxid, rank in ranks.items():
        if rank in REQUIRED_RANKS:
            annotation[rank] = names[taxid]
    # NCBI renamed "superkingdom" to "domain" in 2025: accept either
    if not annotation['superkingdom']:
        annotation['superkingdom'] = next((names[t] for t, r in ranks.items() if r == 'domain'), None)
    annotation['scientific_name'] = names.get(int(taxonomy_id))
    return annotation


def collect_species_counts(private_config_xml_file, profile, release_version, skip_mongo):
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_conn:
        logger.info('Collecting counts from EVAPRO')
        evapro_counts = get_evapro_counts(metadata_conn)
        taxonomy_names = get_evapro_taxonomy_names(metadata_conn)
        supported_assemblies = get_supported_assemblies(metadata_conn)

    if release_version is None:
        release_version = get_latest_release_version()
    logger.info(f'Collecting counts from the release web services for release {release_version}')
    release_counts = get_release_counts(release_version)

    eva_ss, dbsnp_ss = {}, {}
    if not skip_mongo:
        with get_mongo_connection_handle(profile, private_config_xml_file,
                                         read_preference=ReadPreference.SECONDARY_PREFERRED) as mongo_handle:
            eva_ss = get_mongo_ss_counts(mongo_handle, EVA_SS_COLLECTION)
            dbsnp_ss = get_mongo_ss_counts(mongo_handle, DBSNP_SS_COLLECTION)

    all_taxonomies = set(evapro_counts) | set(release_counts) | set(eva_ss) | set(dbsnp_ss)
    all_taxonomies.discard(None)
    rows = []
    for taxonomy_id in all_taxonomies:
        annotation = annotate_taxonomy(taxonomy_id)
        evapro = evapro_counts.get(taxonomy_id, {})
        eva_studies = evapro.get('eva_study_set', set())
        eva_ss_count, eva_ss_studies = eva_ss.get(taxonomy_id, (0, set()))
        dbsnp_ss_count, dbsnp_studies = dbsnp_ss.get(taxonomy_id, (0, set()))
        release = release_counts.get(taxonomy_id, {})
        scientific_name, common_name = taxonomy_names.get(taxonomy_id, (annotation['scientific_name'], None))
        rows.append({
            'taxonomy_id': taxonomy_id,
            'scientific_name': scientific_name,
            'common_name': common_name,
            'species': annotation['species'],
            'kingdom': annotation['kingdom'],
            'superkingdom': annotation['superkingdom'],
            'eva_studies': len(eva_studies),
            'dbsnp_studies': len(dbsnp_studies),
            # EVA studies registered in EVAPRO, plus any other study found in the accessioning warehouse
            'total_studies': len(eva_studies | eva_ss_studies | dbsnp_studies),
            'analyses': evapro.get('analyses', 0),
            'files': evapro.get('files', 0),
            'samples': evapro.get('samples', 0),
            'eva_submitted_variants': eva_ss_count,
            'dbsnp_submitted_variants': dbsnp_ss_count,
            'submitted_variants': eva_ss_count + dbsnp_ss_count,
            'clustered_variants': release.get('clustered_variants', 0),
            'assemblies_with_rs': SEPARATOR.join(sorted(release.get('assemblies_with_rs', []))),
            'supported_assembly': SEPARATOR.join(supported_assemblies.get(taxonomy_id, [])),
        })
    rows.sort(key=lambda r: ((r['scientific_name'] or '').lower(), r['taxonomy_id']))
    return rows


def write_tsv(rows, output_file):
    with open(output_file, 'w', newline='') as open_output:
        writer = csv.DictWriter(open_output, fieldnames=OUTPUT_HEADER, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f'Wrote {len(rows)} taxonomies to {output_file}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--private-config-xml-file', required=True,
                        help='Maven settings XML file with the database credentials')
    parser.add_argument('--profile', default='production_processing',
                        help='Maven profile to use for EVAPRO and MongoDB')
    parser.add_argument('--output', required=True, help='Path of the output TSV file')
    parser.add_argument('--release-version', type=int, default=None,
                        help='RS release version to use for RS counts (default: latest release)')
    parser.add_argument('--skip-mongo', action='store_true', default=False,
                        help='Do not count submitted variants in the accessioning warehouse (fast, for testing)')
    args = parser.parse_args()
    log_cfg.add_stdout_handler()

    rows = collect_species_counts(args.private_config_xml_file, args.profile, args.release_version, args.skip_mongo)
    write_tsv(rows, args.output)


if __name__ == '__main__':
    main()
