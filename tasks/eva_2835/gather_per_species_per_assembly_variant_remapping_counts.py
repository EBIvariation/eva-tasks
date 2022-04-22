# Copyright 2022 EMBL - European Bioinformatics Institute
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

# Importing relevant packages
import os
import yaml
import argparse
import csv

# Initializing the final output column
all_columns = []

# Initializing the list to store all the keys and values for all the files
all_keys = []
all_values = []

# Initializing the list to store the values of all taxonomy ids and assembly accessions
all_tax_id =[]
all_assembly = []


def collect_taxid_assembly(remapping_root_path, output_path):
    """
    This function is used to store the ful list of taxonomy ids and assembly accessions
    Input: It accepts the remapping full path from the user along with the output path
           for storing the results for subsequent analysis
    Output: It calls gather_counts_per_tax_per_assembly() function
           for a particular taxonomy id and a particular assembly of that taxonomy
    """

    # Collecting the tax_ids from the input path
    taxids = [name for name in os.listdir(remapping_root_path) if
              os.path.isdir(os.path.join(remapping_root_path, name))]

    # Defining a dictionary to store the taxonomy and its corresponding assemblies
    tax_assembly = {}

    for tax_id in taxids:
        assembly_accession = [name for name in os.listdir(os.path.join(remapping_root_path, tax_id)) if
                              os.path.isdir(os.path.join(os.path.join(remapping_root_path, tax_id), name))]
        tax_assembly[tax_id] = assembly_accession

    # Generating statistics for each taxonomy and each assembly
    for key, value in tax_assembly.items():
        for val in range(len(value)):
            gather_counts_per_tax_per_assembly(remapping_root_path, key, value[val])


# In Progress


def gather_counts_per_tax_per_assembly(path, taxid, assembly_accession):
    """
    This function is used to store the counts of the remapped variants along wih the reason for failures
    at different rounds (i.e. for different lengths of the flanking region)
    Input: The taxonomy id and the assembly accession from collect_taxid_assembly()
    Output: A spreadsheet outlining all the relevant counts for each taxonomy and each assembly
            per taxonomy for both EVA and dbSNP data
    """

    # Setting the filename for the eva counts
    filename_eva = str(assembly_accession) + "_eva_remapped_counts.yml"
    filename_eva = os.path.join(path, str(taxid), str(assembly_accession), "eva", filename_eva)

    # Setting the filename for the dbsnp counts
    filename_dbsnp = str(assembly_accession) + "_dbsnp_remapped_counts.yml"
    filename_dbsnp = os.path.join(path, str(taxid), str(assembly_accession), "dbsnp", filename_dbsnp)

    with open(filename_eva, 'r') as file:

        # Loading the data from the yaml file
        data = yaml.safe_load(file)

        # Storing the contents for a particular yaml file in a linear dictionary format for eva
        keys = []
        values = []
        keys_values = {}

        for key, value in data.items():

            # Checking if the value is dictionary
            if isinstance(value, dict):
                for key2, value2 in value.items():
                    keys.append(key + "_" + key2)
                    values.append(value2)
            else:
                keys.append(key.capitalize())
                values.append(value)

        # Creating dictionary from two lists
        zip_iterator = zip(keys, values)
        keys_values = dict(zip_iterator)

    with open(filename_dbsnp, 'r') as file:

        # Loading the data from the yaml file
        data = yaml.safe_load(file)

        # Storing the contents for a particular yaml file in a linear dictionary format for dbsnp
        keys = []
        values = []
        keys_values_dbsnp = {}

        for key, value in data.items():

            # Checking if the value is dictionary
            if isinstance(value, dict):
                for key2, value2 in value.items():
                    keys.append(key + "_" + key2)
                    values.append(value2)
            else:
                keys.append(key.capitalize())
                values.append(value)

        # Creating dictionary from two lists
        zip_iterator = zip(keys, values)
        keys_values_dbsnp = dict(zip_iterator)

    # Adding the values of bdsnp and eva with the common keys
    for key in keys_values_dbsnp:
        if key in keys_values:
            keys_values_dbsnp[key] = keys_values_dbsnp[key] + keys_values[key]

    keys_values_dbsnp = {**keys_values, **keys_values_dbsnp}

    # Sorting the dictionary using the keys
    keys_values_dbsnp = {key: value for key, value in sorted(keys_values_dbsnp.items())}

    # Splitting the dictionary into two lists - one for keys and one for values
    keys, values = zip(*keys_values_dbsnp.items())
    keys = list(keys)
    values = list(values)

    # Adding the keys and values list to the master list for each taxononmy and assembly
    all_keys.append(keys)
    all_values.append(values)

    # Adding the taxonomy and assembly accession for a particular file
    all_tax_id.append(taxid)
    all_assembly.append(assembly_accession)

    # Updating the final output columns
    global all_columns
    all_columns.extend(keys)
    all_columns = list(set(all_columns))

# Defining the main function to accept the inputs from the user


def main():
    parser = argparse.ArgumentParser(
        description='Collecting statistics per taxonomy per assembly for variant remapping')

    # Allowing the user to enter the full path for the taxonomy folders which contain
    # the corresponding old assemblies which in turn contain the yaml files containing
    # the remapped statistics
    parser.add_argument("--remapping_root_path", type=str,
                        help="Path where the remapping directories are present", required=True)

    # Taking the output path from the user where the visualization files and output csv
    # files are present
    parser.add_argument("--output_file", type=str,
                        help="Path to the output .", required=True)

    args = parser.parse_args()

    # Calling the primary function responsible for generating the statistics
    collect_taxid_assembly(args.remapping_root_path, args.output_file)


if __name__ == "__main__":
    main()
