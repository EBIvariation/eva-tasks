#Copyright 2022 EMBL - European Bioinformatics Institute
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
import yaml

# Defining the function to store the counts of the remapped variants along wih the reason for failure
# in different rounds


def gather_counts_per_tax_per_assembly(taxid, assembly_accession):

    filename = assembly_accession + "_eva_remapped_counts.yml"

    with open(filename, 'r') as file:

        # Loading the data from the yaml file
        data = yaml.safe_load(file)

        # Recording the total number of varints
        all_taxid_assembly_accession = data['all']
        filtered_taxid_assembly_accession = data['filtered']

        # Recording the remapping statistics for flanking region of length 50
        flank_50_taxid_assembly_accession = data['Flank_50']

        # Recording the remapping statistics for flanking region of length 2000
        flank_2000_taxid_assembly_accession = data['Flank_2000']

        # Recording the remapping statistics for flanking region of length 50000
        flank_50000_taxid_assembly_accession = data['Flank_50000']

        # Collecting failure statistics for flanking region of length 50
        for k, v in flank_50_taxid_assembly_accession.items():
            k = k.replace(" ", "")
            temp = "flank_50_" + k + "_taxid_assembly_accession"
            locals()[temp] = v

        # Collecting failure statistics for flanking region of length 2000
        for k, v in flank_2000_taxid_assembly_accession.items():
            k = k.replace(" ", "")
            temp = "flank_2000_" + k + "_taxid_assembly_accession"
            locals()[temp] = v

        # Collecting failure statistics for flanking region of length 50000
        for k, v in flank_50000_taxid_assembly_accession.items():
            k = k.replace(" ", "")
            temp = "flank_50_" + k + "_taxid_assembly_accession"
            locals()[temp] = v


# Defining the main function --In Progress

def main():
    gather_counts_per_tax_per_assembly(9265, "GCA_000298735.2")


if __name__ == "__main__":
    main()
