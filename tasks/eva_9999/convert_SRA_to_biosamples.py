import requests


def get_biosample_accession(sra_accession):
    """
    Given an SRA accession (e.g., SRR or SRS), return the associated BioSample accession using the ENA API.
    """
    # Define the API endpoint
    url = "https://www.ebi.ac.uk/ena/portal/api/filereport"

    # Parameters for the request
    params = {
        "accession": sra_accession,
        "result": "sample",  # read_run returns metadata about sequencing runs
        "fields": "sample_accession",
        "format": "json"
    }

    # Send GET request to ENA
    response = requests.get(url, params=params)
    response.raise_for_status()

    results = response.json()

    if not results:
        raise Exception(f"No results found for accession {sra_accession}")

    biosample = results[0].get("sample_accession")

    if not biosample:
        raise Exception(f"Biosample accession not found for {sra_accession}")

    return biosample

with open('input_sample.txt') as open_file, open('output_sample.tsv', 'w') as output_file:
    for line in open_file:
        sra_sample = line.strip().split()[0]
        biosample = get_biosample_accession(sra_sample)
        output_file.write(f"{sra_sample}\t{biosample}\n")
        print(sra_sample, biosample)
