import os.path
import unittest

from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

from tasks.eva_3655.remediateLowerCaseNucleotide import remediate_lower_case_nucleotide


class MyTestCase(unittest.TestCase):
    def test_remediate_lower_case_nucleotide(self):
        working_dir = os.path.dirname(os.path.abspath(__file__))
        private_config_xml_file = os.path.join(working_dir, 'eva-maven-settings.xml')
        test_db_name = "test_lowercase_remediation_db"

        with get_mongo_connection_handle('localhost', private_config_xml_file) as mongo_conn:
            # remove existing data
            mongo_conn.drop_database(test_db_name)
            os.remove(os.path.join(working_dir, "non_merged_candidates", f"{test_db_name}.txt"))

            variants_coll = mongo_conn[test_db_name]['variants_2_0']
            files_coll = mongo_conn[test_db_name]['files_2_0']

            # setup test data
            self.setup_test_data(variants_coll, files_coll)
            # run remediation
            remediate_lower_case_nucleotide(working_dir, private_config_xml_file, 'localhost', test_db_name)
            # qc results
            self.qc_test_result_case_no_id_collision(variants_coll)
            self.qc_test_result_case_id_collision_all_sid_fid_diff(variants_coll)
            self.qc_test_result_case_id_collision_sid_fid_has_more_than_one_file(variants_coll, working_dir,
                                                                                 test_db_name)
            self.qc_test_result_case_id_collision_sid_fid_has_one_file(variants_coll)

    def setup_test_data(self, variants_coll, files_coll):
        variants_coll.insert_many([
            # case no id collision after remediation
            {
                "_id": "chr1_11111111_a_g", "chr": "chr1", "start": 11111111, "end": 11111111, "ref": "a", "alt": "g",
                "files": [{"sid": "sid11", "fid": "fid11"}],
                "hgvs": [{"type": "genomic", "name": "chr1:g.11111111a>g"}],
                "st": [{"maf": 0.11, "mgf": 0.11, "mafAl": "a", "mgfGt": "0/0", "sid": "sid11", "fid": "fid11"}]
            },

            # case id collision - all sid and fid are different
            # variant with uppercase ref and alt
            {
                "_id": "chr2_22222222_A_G", "chr": "chr2", "start": 22222222, "end": 22222222, "ref": "A", "alt": "G",
                "files": [{"sid": "sid21", "fid": "fid21"},
                          {"sid": "sid211", "fid": "fid211"}],
                "hgvs": [{"type": "genomic", "name": "chr2:g.22222222A>G"},
                         {"type": "genomic", "name": "chr2:g.22222222A>G"}],
                "st": [{"maf": 0.21, "mgf": 0.21, "mafAl": "A", "mgfGt": "0/0", "sid": "sid21", "fid": "fid21"},
                       {"maf": 0.211, "mgf": 0.211, "mafAl": "A", "mgfGt": "0/0", "sid": "sid211", "fid": "fid211"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr2_22222222_a_g", "chr": "chr2", "start": 22222222, "end": 22222222, "ref": "a", "alt": "g",
                "files": [{"sid": "sid22", "fid": "fid22"},
                          {"sid": "sid222", "fid": "fid222"}],
                "hgvs": [{"type": "genomic", "name": "chr2:g.22222222a>g"},
                         {"type": "genomic", "name": "chr2:g.22222222a>g"}],
                "st": [{"maf": 0.22, "mgf": 0.22, "mafAl": "a", "mgfGt": "0/0", "sid": "sid22", "fid": "fid22"},
                       {"maf": 0.222, "mgf": 0.222, "mafAl": "a", "mgfGt": "0/0", "sid": "sid222", "fid": "fid222"}]
            },

            # case id collision - fid has more than one file
            # variant with uppercase ref and alt
            {
                "_id": "chr3_33333333_A_G", "chr": "chr3", "start": 33333333, "end": 33333333, "ref": "A", "alt": "G",
                "files": [{"sid": "sid31", "fid": "fid31"},
                          {"sid": "sid311", "fid": "fid311"}],
                "hgvs": [{"type": "genomic", "name": "chr3:g.33333333A>G"},
                         {"type": "genomic", "name": "chr3:g.33333333A>G"}],
                "st": [{"maf": 0.31, "mgf": 0.31, "mafAl": "A", "mgfGt": "0/0", "sid": "sid31", "fid": "fid31"},
                       {"maf": 0.311, "mgf": 0.311, "mafAl": "A", "mgfGt": "0/0", "sid": "sid311", "fid": "fid311"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr3_33333333_a_g", "chr": "chr3", "start": 33333333, "end": 33333333, "ref": "a", "alt": "g",
                "files": [{"sid": "sid32", "fid": "fid32"},
                          {"sid": "sid322", "fid": "fid322"},
                          {"sid": "sid31", "fid": "fid31"}],
                "hgvs": [{"type": "genomic", "name": "chr3:g.33333333a>g"},
                         {"type": "genomic", "name": "chr3:g.33333333a>g"},
                         {"type": "genomic", "name": "chr3:g.33333333a>g"}],
                "st": [
                    {"maf": 0.32, "mgf": 0.32, "mafAl": "a", "mgfGt": "0/0", "sid": "sid32", "fid": "fid32"},
                    {"maf": 0.322, "mgf": 0.322, "mafAl": "a", "mgfGt": "0/0", "sid": "sid322", "fid": "fid322"},
                    {"maf": 0.31, "mgf": 0.31, "mafAl": "a", "mgfGt": "0/0", "sid": "sid31", "fid": "fid31"}]
            },

            # case id collision - fid has just one file
            # variant with uppercase ref and alt
            {
                "_id": "chr4_44444444_A_G", "chr": "chr4", "start": 44444444, "end": 44444444, "ref": "A", "alt": "G",
                "files": [{"sid": "sid41", "fid": "fid41"},
                          {"sid": "sid411", "fid": "fid411"}],
                "hgvs": [{"type": "genomic", "name": "chr4:g.44444444A>G"},
                         {"type": "genomic", "name": "chr4:g.44444444A>G"}],
                "st": [{"maf": 0.41, "mgf": 0.41, "mafAl": "A", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"},
                       {"maf": 0.411, "mgf": 0.411, "mafAl": "A", "mgfGt": "0/0", "sid": "sid411", "fid": "fid411"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr4_44444444_a_g", "chr": "chr4", "start": 44444444, "end": 44444444, "ref": "a", "alt": "g",
                "files": [{"sid": "sid42", "fid": "fid42"},
                          {"sid": "sid422", "fid": "fid422"},
                          {"sid": "sid41", "fid": "fid41"}],
                "hgvs": [{"type": "genomic", "name": "chr4:g.44444444a>g"},
                         {"type": "genomic", "name": "chr4:g.44444444a>g"},
                         {"type": "genomic", "name": "chr4:g.44444444a>g"}],
                "st": [
                    {"maf": 0.42, "mgf": 0.42, "mafAl": "a", "mgfGt": "0/0", "sid": "sid42", "fid": "fid42"},
                    {"maf": 0.422, "mgf": 0.422, "mafAl": "a", "mgfGt": "0/0", "sid": "sid422", "fid": "fid422"},
                    {"maf": 0.41, "mgf": 0.41, "mafAl": "a", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"}]
            },

        ])

        files_coll.insert_many([
            # case id collision - fid has more than one file
            {"_id": "file_31", "sid": "sid31", "fid": "fid31", "fname": "file_name_31.vcf.gz"},
            {"_id": "file_31_1", "sid": "sid31", "fid": "fid31", "fname": "file_name_31_1.vcf.gz"},
            # case id collision - fid has just one file
            {"_id": "file_41", "sid": "sid41", "fid": "fid41", "fname": "file_name_41.vcf.gz"}
        ])

    def qc_test_result_case_no_id_collision(self, variants_coll):
        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': 'chr1_11111111_a_g'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': 'chr1_11111111_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))
        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual('A', uppercase_variant['ref'])
        self.assertEqual('G', uppercase_variant['alt'])
        self.assertEqual('chr1:g.11111111A>G', uppercase_variant['hgvs'][0]['name'])
        self.assertEqual('A', uppercase_variant['st'][0]['mafAl'])

    def qc_test_result_case_id_collision_all_sid_fid_diff(self, variants_coll):
        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': 'chr2_22222222_a_g'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': 'chr2_22222222_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual('A', uppercase_variant['ref'])
        self.assertEqual('G', uppercase_variant['alt'])
        self.assertEqual([{"sid": "sid21", "fid": "fid21"},
                          {"sid": "sid211", "fid": "fid211"},
                          {"sid": "sid22", "fid": "fid22"},
                          {"sid": "sid222", "fid": "fid222"}],
                         uppercase_variant['files'])
        self.assertEqual([{"type": "genomic", "name": "chr2:g.22222222A>G"},
                          {"type": "genomic", "name": "chr2:g.22222222A>G"},
                          {"type": "genomic", "name": "chr2:g.22222222A>G"},
                          {"type": "genomic", "name": "chr2:g.22222222A>G"}],
                         uppercase_variant['hgvs'])
        self.assertEqual([{"maf": 0.21, "mgf": 0.21, "mafAl": "A", "mgfGt": "0/0", "sid": "sid21", "fid": "fid21"},
                          {"maf": 0.211, "mgf": 0.211, "mafAl": "A", "mgfGt": "0/0", "sid": "sid211", "fid": "fid211"},
                          {"maf": 0.22, "mgf": 0.22, "mafAl": "A", "mgfGt": "0/0", "sid": "sid22", "fid": "fid22"},
                          {"maf": 0.222, "mgf": 0.222, "mafAl": "A", "mgfGt": "0/0", "sid": "sid222", "fid": "fid222"}],
                         uppercase_variant['st'])

    def qc_test_result_case_id_collision_sid_fid_has_more_than_one_file(self, variants_coll, working_dir, db_name):
        # assert lowercase variant is not deleted
        lowercase_variant_list = list(variants_coll.find({'_id': 'chr3_33333333_a_g'}))
        self.assertEqual(1, len(lowercase_variant_list))
        uppercase_variant_list = list(variants_coll.find({'_id': 'chr3_33333333_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert the issue is logged in the file
        non_merged_candidate_file = os.path.join(working_dir, "non_merged_candidates", f"{db_name}.txt")
        self.assertTrue(os.path.exists(non_merged_candidate_file))
        with open(non_merged_candidate_file, 'r') as nmc_file:
            self.assertEqual("('sid31', 'fid31')\n", nmc_file.readline())

    def qc_test_result_case_id_collision_sid_fid_has_one_file(self, variants_coll):
        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': 'chr4_44444444_a_g'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': 'chr4_44444444_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual('A', uppercase_variant['ref'])
        self.assertEqual('G', uppercase_variant['alt'])
        self.assertEqual([{"sid": "sid41", "fid": "fid41"},
                          {"sid": "sid411", "fid": "fid411"},
                          {"sid": "sid42", "fid": "fid42"},
                          {"sid": "sid422", "fid": "fid422"}],
                         uppercase_variant['files'])
        self.assertEqual([{"maf": 0.41, "mgf": 0.41, "mafAl": "A", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"},
                          {"maf": 0.411, "mgf": 0.411, "mafAl": "A", "mgfGt": "0/0", "sid": "sid411", "fid": "fid411"},
                          {"maf": 0.42, "mgf": 0.42, "mafAl": "A", "mgfGt": "0/0", "sid": "sid42", "fid": "fid42"},
                          {"maf": 0.422, "mgf": 0.422, "mafAl": "A", "mgfGt": "0/0", "sid": "sid422", "fid": "fid422"}],
                         uppercase_variant['st'])
        # TODO: uncomment after fixing hgvs inclusion in the case
        # self.assertEqual([{"type": "genomic", "name": "chr2:g.44444444A>G"},
        #                   {"type": "genomic", "name": "chr2:g.44444444A>G"},
        #                   {"type": "genomic", "name": "chr2:g.44444444A>G"},
        #                   {"type": "genomic", "name": "chr2:g.44444444A>G"}],
        #                  uppercase_variant['hgvs'])


if __name__ == '__main__':
    unittest.main()
