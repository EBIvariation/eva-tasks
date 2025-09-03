import requests
username = 'ebi-seqcol'
password = 'XfpEnMjuZlG8QnALQGTq'

files_to_download = [
    'base.fa',
    'different_names.fa',
    'different_order.fa',
    'pair_swap.fa',
    'subset.fa',
    'swap_wo_coords.fa'
]
test_fasta_url = 'https://raw.githubusercontent.com/refgenie/refget/refs/heads/dev/test_fasta/'
seqcol_server = 'http://45.88.81.158:8081/eva/webservices/seqcol/admin/seqcols/'
accessions = [
    'GCA_000001405.15',  # 	GRCh38
    'GCA_000001405.29',  # 	GRCh38.p14
    'GCA_000001405.1',   # GRCh37
    'GCA_000001405.14'   # GRCh37.p13
]


def download_file(file_name):
    url_for_file = test_fasta_url + file_name
    r = requests.get(url_for_file)
    r.raise_for_status()
    return r.text

def put_fasta_to_eva_seqcol_server(file_name, file_content):
    url = seqcol_server + 'fasta/' + file_name
    r = requests.put(url, data=file_content, auth=(username, password))
    r.raise_for_status()
    print(f'Loaded {file_name} to eva seqcol server')

def put_accession_to_eva_seqcol_server(accession):
    url = seqcol_server + accession
    r = requests.put(url, auth=(username, password))
    if r.status_code is requests.codes.ok:
        print(f'Loaded {accession} to eva seqcol server')
    else:
        print(r.text)


def load_test_fasta():
    for file_name in files_to_download:
        file_content = download_file(file_name)
        put_fasta_to_eva_seqcol_server(file_name, file_content)


def load_accession():
    for accession in accessions:
        put_accession_to_eva_seqcol_server(accession)

load_accession()