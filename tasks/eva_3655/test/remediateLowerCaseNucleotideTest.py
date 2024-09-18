import os.path
import re
import unittest

from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle

from tasks.eva_3655.remediateLowerCaseNucleotide import remediate_lower_case_nucleotide, regex_pattern, encrypt_sha1

lowercase_large_ref = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
uppercase_large_ref = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
lowercase_large_alt = "gggggggggggggggggggggggggggggggggggggggggggggggggg"
uppercase_large_alt = "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"
lowercase_id_large_ref_alt = encrypt_sha1(f"{lowercase_large_ref}_{lowercase_large_alt}")
uppercase_id_large_ref_alt = encrypt_sha1(f"{uppercase_large_ref}_{uppercase_large_alt}")
lowercase_hgvs_large_ref_alt = f"{lowercase_large_ref}>{lowercase_large_alt}"
uppercase_hgvs_large_ref_alt = f"{uppercase_large_ref}>{uppercase_large_alt}"


class MyTestCase(unittest.TestCase):

    def setup_env_and_run_remediation_with_qc(self, db_name, files_data, varints_data, qc_func):
        working_dir = os.path.dirname(os.path.abspath(__file__))
        private_config_xml_file = os.path.join(working_dir, 'eva-maven-settings.xml')

        with get_mongo_connection_handle('localhost', private_config_xml_file) as mongo_conn:
            # remove existing data
            mongo_conn.drop_database(db_name)
            nmc_file_path = os.path.join(working_dir, "non_merged_candidates", f"{db_name}.txt")
            if os.path.exists(nmc_file_path):
                os.remove(nmc_file_path)

            variants_coll = mongo_conn[db_name]['variants_2_0']
            files_coll = mongo_conn[db_name]['files_2_0']
            if files_data:
                files_coll.insert_many(files_data)
            if varints_data:
                variants_coll.insert_many(varints_data)

            # run remediation
            remediate_lower_case_nucleotide(working_dir, private_config_xml_file, 'localhost', db_name)

            # qc results
            qc_func(variants_coll, working_dir, db_name)

    def test_remediate_lower_case_nucleotide_with_different_cases(self):
        test_db_name = "test_lowercase_remediation_db_different_cases"
        files_data, variants_data = self.get_test_data_for_different_cases()
        self.setup_env_and_run_remediation_with_qc(test_db_name, files_data, variants_data,
                                                   self.qc_test_result_for_different_cases)

    def test_remediate_lower_case_nucleotide_hgvs_not_present(self):
        test_db_name = "test_lowercase_remediation_db_hgvs_not_present"
        files_data, variants_data = self.get_test_data_for_hgvs_not_present()
        self.setup_env_and_run_remediation_with_qc(test_db_name, files_data, variants_data,
                                                   self.qc_test_result_for_hgvs_not_present)

    def test_remediate_lower_case_nucleotide_large_ref_alt(self):
        test_db_name = "test_lowercase_remediation_db_large_ref_alt"
        files_data, variants_data = self.get_test_data_for_large_ref_alt()
        self.setup_env_and_run_remediation_with_qc(test_db_name, files_data, variants_data,
                                                   self.qc_test_result_for_large_ref_alt)

    def test_regex(self):
        test_ids = [
            ("chr1_77777777_a_g", True),
            ("chr1_77777777_a_", True),
            ("chr1_77777777__g", True),
            ("chr1_77777777_AcG_AgT", True),
            ("chr1_77777777__AgT", True),
            ("chr1_77777777_AcG_", True),
            ("chr1_77777777_cAG_gAT", True),
            ("chr1_77777777_AGc_ATg", True),
            (f"chr1_77777777_{encrypt_sha1(lowercase_large_ref)}_{encrypt_sha1(lowercase_large_alt)}", True),
            (f"chr1_77777777_{encrypt_sha1(uppercase_large_ref)}_{encrypt_sha1(uppercase_large_alt)}", True),
            ("chr1_77777777_AG5_AT5", True),
            ("chr1_77777777_555_555", True),
            ("chr1_77777777_A_G", False),
            ("chr1_77777777_A_", False),
            ("chr1_77777777__G", False),
            ("chr1_77777777_ACT_CTG", False),
        ]

        for string, expected in test_ids:
            match = bool(re.search(regex_pattern, string))
            assert match == expected, f"Test failed for string: {string}"

    def get_test_data_for_different_cases(self):
        return [
            # case id collision - fid has more than one file
            {"_id": "file_31", "sid": "sid31", "fid": "fid31", "fname": "file_name_31.vcf.gz"},
            {"_id": "file_31_1", "sid": "sid31", "fid": "fid31", "fname": "file_name_31_1.vcf.gz"},
            # case id collision - fid has just one file
            {"_id": "file_41", "sid": "sid41", "fid": "fid41", "fname": "file_name_41.vcf.gz"}
        ], [
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
                "hgvs": [{"type": "genomic", "name": "chr2:g.22222222A>G"}],
                "st": [{"maf": 0.21, "mgf": 0.21, "mafAl": "A", "mgfGt": "0/0", "sid": "sid21", "fid": "fid21"},
                       {"maf": 0.211, "mgf": 0.211, "mafAl": "A", "mgfGt": "0/0", "sid": "sid211", "fid": "fid211"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr2_22222222_a_g", "chr": "chr2", "start": 22222222, "end": 22222222, "ref": "a", "alt": "g",
                "files": [{"sid": "sid22", "fid": "fid22"},
                          {"sid": "sid222", "fid": "fid222"}],
                "hgvs": [{"type": "genomic", "name": "chr2:g.22222222a>g"}],
                "st": [{"maf": 0.22, "mgf": 0.22, "mafAl": "a", "mgfGt": "0/0", "sid": "sid22", "fid": "fid22"},
                       {"maf": 0.222, "mgf": 0.222, "mafAl": "a", "mgfGt": "0/0", "sid": "sid222", "fid": "fid222"}]
            },

            # case id collision - fid has more than one file
            # variant with uppercase ref and alt
            {
                "_id": "chr3_33333333_A_G", "chr": "chr3", "start": 33333333, "end": 33333333, "ref": "A", "alt": "G",
                "files": [{"sid": "sid31", "fid": "fid31"},
                          {"sid": "sid311", "fid": "fid311"}],
                "hgvs": [{"type": "genomic", "name": "chr3:g.33333333A>G"}],
                "st": [{"maf": 0.31, "mgf": 0.31, "mafAl": "A", "mgfGt": "0/0", "sid": "sid31", "fid": "fid31"},
                       {"maf": 0.311, "mgf": 0.311, "mafAl": "A", "mgfGt": "0/0", "sid": "sid311", "fid": "fid311"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr3_33333333_a_g", "chr": "chr3", "start": 33333333, "end": 33333333, "ref": "a", "alt": "g",
                "files": [{"sid": "sid32", "fid": "fid32"},
                          {"sid": "sid322", "fid": "fid322"},
                          {"sid": "sid31", "fid": "fid31"}],
                "hgvs": [{"type": "genomic", "name": "chr3:g.33333333a>g"}],
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
                "hgvs": [{"type": "genomic", "name": "chr4:g.44444444A>G"}],
                "st": [{"maf": 0.41, "mgf": 0.41, "mafAl": "A", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"},
                       {"maf": 0.411, "mgf": 0.411, "mafAl": "A", "mgfGt": "0/0", "sid": "sid411", "fid": "fid411"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr4_44444444_a_g", "chr": "chr4", "start": 44444444, "end": 44444444, "ref": "a", "alt": "g",
                "files": [{"sid": "sid42", "fid": "fid42"},
                          {"sid": "sid422", "fid": "fid422"},
                          {"sid": "sid41", "fid": "fid41"}],
                "hgvs": [{"type": "genomic", "name": "chr4:g.44444444a>g"}],
                "st": [
                    {"maf": 0.42, "mgf": 0.42, "mafAl": "a", "mgfGt": "0/0", "sid": "sid42", "fid": "fid42"},
                    {"maf": 0.422, "mgf": 0.422, "mafAl": "a", "mgfGt": "0/0", "sid": "sid422", "fid": "fid422"},
                    {"maf": 0.41, "mgf": 0.41, "mafAl": "a", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"}]
            }
        ]

    def qc_test_result_for_different_cases(self, variants_coll, working_dir, db_name):
        # case_no_id_collision

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

        # case_id_collision_all_sid_fid_diff

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
        self.assertEqual([{"type": "genomic", "name": "chr2:g.22222222A>G"}], uppercase_variant['hgvs'])
        self.assertEqual([{"maf": 0.21, "mgf": 0.21, "mafAl": "A", "mgfGt": "0/0", "sid": "sid21", "fid": "fid21"},
                          {"maf": 0.211, "mgf": 0.211, "mafAl": "A", "mgfGt": "0/0", "sid": "sid211", "fid": "fid211"},
                          {"maf": 0.22, "mgf": 0.22, "mafAl": "A", "mgfGt": "0/0", "sid": "sid22", "fid": "fid22"},
                          {"maf": 0.222, "mgf": 0.222, "mafAl": "A", "mgfGt": "0/0", "sid": "sid222", "fid": "fid222"}],
                         uppercase_variant['st'])

        # case_id_collision_sid_fid_has_more_than_one_file

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

        # case_id_collision_sid_fid_has_one_file
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
        self.assertEqual([{"type": "genomic", "name": "chr4:g.44444444A>G"}], uppercase_variant['hgvs'])
        self.assertEqual([{"maf": 0.41, "mgf": 0.41, "mafAl": "A", "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"},
                          {"maf": 0.411, "mgf": 0.411, "mafAl": "A", "mgfGt": "0/0", "sid": "sid411", "fid": "fid411"},
                          {"maf": 0.42, "mgf": 0.42, "mafAl": "A", "mgfGt": "0/0", "sid": "sid42", "fid": "fid42"},
                          {"maf": 0.422, "mgf": 0.422, "mafAl": "A", "mgfGt": "0/0", "sid": "sid422", "fid": "fid422"}],
                         uppercase_variant['st'])

    def get_test_data_for_hgvs_not_present(self):
        return [], [
            # case id collision - hgvs not already present
            # variant with uppercase ref and alt
            {
                "_id": "chr5_55555555_A_G", "chr": "chr5", "start": 55555555, "end": 55555555, "ref": "A", "alt": "G",
                "files": [{"sid": "sid51", "fid": "fid51"}],
                "hgvs": [{"type": "genomic", "name": "chr4:g.44444444A>G"}],
                "st": [{"maf": 0.51, "mgf": 0.51, "mafAl": "A", "mgfGt": "0/0", "sid": "sid51", "fid": "fid51"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr5_55555555_a_g", "chr": "chr5", "start": 55555555, "end": 55555555, "ref": "a", "alt": "g",
                "files": [{"sid": "sid52", "fid": "fid52"},
                          {"sid": "sid51", "fid": "fid51"}],
                "hgvs": [{"type": "genomic", "name": "chr5:g.55555555a>g"}],
                "st": [{"maf": 0.52, "mgf": 0.52, "mafAl": "a", "mgfGt": "0/0", "sid": "sid52", "fid": "fid52"},
                       {"maf": 0.51, "mgf": 0.51, "mafAl": "A", "mgfGt": "0/0", "sid": "sid51", "fid": "fid51"}]
            },

            # variant with uppercase ref and alt
            {
                "_id": "chr6_66666666_A_G", "chr": "chr6", "start": 66666666, "end": 66666666, "ref": "A", "alt": "G",
                "files": [{"sid": "sid61", "fid": "fid61"}],
                "hgvs": [{"type": "genomic", "name": "chr5:g.55555555A>G"}],
                "st": [{"maf": 0.61, "mgf": 0.61, "mafAl": "A", "mgfGt": "0/0", "sid": "sid61", "fid": "fid61"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": "chr6_66666666_a_g", "chr": "chr6", "start": 66666666, "end": 66666666, "ref": "a", "alt": "g",
                "files": [{"sid": "sid62", "fid": "fid62"}],
                "hgvs": [{"type": "genomic", "name": "chr6:g.66666666a>g"}],
                "st": [{"maf": 0.62, "mgf": 0.62, "mafAl": "a", "mgfGt": "0/0", "sid": "sid62", "fid": "fid62"}]
            }
        ]

    def qc_test_result_for_hgvs_not_present(self, variants_coll, working_dir, db_name):
        uppercase_variant_list = list(variants_coll.find({'_id': 'chr5_55555555_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual([{"type": "genomic", "name": "chr4:g.44444444A>G"},
                          {"type": "genomic", "name": "chr5:g.55555555A>G"}],
                         uppercase_variant['hgvs'])

        uppercase_variant_list = list(variants_coll.find({'_id': 'chr6_66666666_A_G'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual([{"type": "genomic", "name": "chr5:g.55555555A>G"},
                          {"type": "genomic", "name": "chr6:g.66666666A>G"}],
                         uppercase_variant['hgvs'])

    def get_test_data_for_large_ref_alt(self):
        return [
            # case id collision - fid has more than one file
            {"_id": "file_31", "sid": "sid31", "fid": "fid31", "fname": "file_name_31.vcf.gz"},
            {"_id": "file_31_1", "sid": "sid31", "fid": "fid31", "fname": "file_name_31_1.vcf.gz"},
            # case id collision - fid has just one file
            {"_id": "file_41", "sid": "sid41", "fid": "fid41", "fname": "file_name_41.vcf.gz"}
        ], [
            # case no id collision after remediation
            {
                "_id": f"chr1_11111111_{lowercase_id_large_ref_alt}", "chr": "chr1", "start": 11111111, "end": 11111191,
                "ref": lowercase_large_ref, "alt": lowercase_large_alt,
                "files": [{"sid": "sid11", "fid": "fid11"}],
                "hgvs": [{"type": "genomic", "name": f"chr1:g.11111111{lowercase_hgvs_large_ref_alt}"}],
                "st": [{"maf": 0.11, "mgf": 0.11, "mafAl": "a", "mgfGt": "0/0", "sid": "sid11", "fid": "fid11"}]
            },

            # case id collision - all sid and fid are different
            # variant with uppercase ref and alt
            {
                "_id": f"chr2_22222222_{uppercase_id_large_ref_alt}", "chr": "chr2", "start": 22222222, "end": 22222292,
                "ref": uppercase_large_ref, "alt": uppercase_large_alt,
                "files": [{"sid": "sid21", "fid": "fid21"},
                          {"sid": "sid211", "fid": "fid211"}],
                "hgvs": [],
                "st": [
                    {"maf": 0.21, "mgf": 0.21, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid21",
                     "fid": "fid21"},
                    {"maf": 0.211, "mgf": 0.211, "mafAl": uppercase_large_alt, "mgfGt": "0/0", "sid": "sid211",
                     "fid": "fid211"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": f"chr2_22222222_{lowercase_id_large_ref_alt}", "chr": "chr2", "start": 22222222, "end": 22222292,
                "ref": lowercase_large_ref, "alt": lowercase_large_alt,
                "files": [{"sid": "sid22", "fid": "fid22"},
                          {"sid": "sid222", "fid": "fid222"}],
                "hgvs": [{"type": "genomic", "name": f"chr2:g.22222222{lowercase_hgvs_large_ref_alt}"}],
                "st": [
                    {"maf": 0.22, "mgf": 0.22, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid22",
                     "fid": "fid22"},
                    {"maf": 0.222, "mgf": 0.222, "mafAl": lowercase_large_alt, "mgfGt": "0/0", "sid": "sid222",
                     "fid": "fid222"}]
            },

            # case id collision - fid has more than one file
            # variant with uppercase ref and alt
            {
                "_id": f"chr3_33333333_{uppercase_id_large_ref_alt}", "chr": "chr3", "start": 33333333, "end": 33333392,
                "ref": uppercase_large_ref, "alt": uppercase_large_alt,
                "files": [{"sid": "sid31", "fid": "fid31"},
                          {"sid": "sid311", "fid": "fid311"}],
                "hgvs": [{"type": "genomic", "name": f"chr3:g.33333333{uppercase_hgvs_large_ref_alt}"}],
                "st": [
                    {"maf": 0.31, "mgf": 0.31, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid31",
                     "fid": "fid31"},
                    {"maf": 0.311, "mgf": 0.311, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid311",
                     "fid": "fid311"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": f"chr3_33333333_{lowercase_id_large_ref_alt}", "chr": "chr3", "start": 33333333, "end": 33333392,
                "ref": lowercase_large_ref, "alt": lowercase_large_alt,
                "files": [{"sid": "sid32", "fid": "fid32"},
                          {"sid": "sid322", "fid": "fid322"},
                          {"sid": "sid31", "fid": "fid31"}],
                "hgvs": [{"type": "genomic", "name": f"chr3:g.33333333{lowercase_hgvs_large_ref_alt}"}],
                "st": [
                    {"maf": 0.32, "mgf": 0.32, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid32",
                     "fid": "fid32"},
                    {"maf": 0.322, "mgf": 0.322, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid322",
                     "fid": "fid322"},
                    {"maf": 0.31, "mgf": 0.31, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid31",
                     "fid": "fid31"}]
            },

            # case id collision - fid has just one file
            # variant with uppercase ref and alt
            {
                "_id": f"chr4_44444444_{uppercase_id_large_ref_alt}", "chr": "chr4", "start": 44444444, "end": 44444493,
                "ref": uppercase_large_ref, "alt": uppercase_large_alt,
                "files": [{"sid": "sid41", "fid": "fid41"},
                          {"sid": "sid411", "fid": "fid411"}],
                "hgvs": [],
                "st": [
                    {"maf": 0.41, "mgf": 0.41, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid41",
                     "fid": "fid41"},
                    {"maf": 0.411, "mgf": 0.411, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid411",
                     "fid": "fid411"}]
            },
            # variant with lowercase ref and alt
            {
                "_id": f"chr4_44444444_{lowercase_id_large_ref_alt}", "chr": "chr4", "start": 44444444, "end": 44444493,
                "ref": lowercase_large_ref, "alt": lowercase_large_alt,
                "files": [{"sid": "sid42", "fid": "fid42"},
                          {"sid": "sid422", "fid": "fid422"},
                          {"sid": "sid41", "fid": "fid41"}],
                "hgvs": [{"type": "genomic", "name": f"chr4:g.44444444{lowercase_hgvs_large_ref_alt}"}],
                "st": [
                    {"maf": 0.42, "mgf": 0.42, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid42",
                     "fid": "fid42"},
                    {"maf": 0.422, "mgf": 0.422, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid422",
                     "fid": "fid422"},
                    {"maf": 0.41, "mgf": 0.41, "mafAl": lowercase_large_ref, "mgfGt": "0/0", "sid": "sid41",
                     "fid": "fid41"}]
            }
        ]

    def qc_test_result_for_large_ref_alt(self, variants_coll, working_dir, db_name):
        # case_no_id_collision

        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': f'chr1_11111111_{lowercase_id_large_ref_alt}'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': f'chr1_11111111_{uppercase_id_large_ref_alt}'}))
        self.assertEqual(1, len(uppercase_variant_list))
        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual(uppercase_large_ref, uppercase_variant['ref'])
        self.assertEqual(uppercase_large_alt, uppercase_variant['alt'])
        self.assertEqual(f'chr1:g.11111111{uppercase_hgvs_large_ref_alt}', uppercase_variant['hgvs'][0]['name'])
        self.assertEqual(uppercase_large_ref, uppercase_variant['st'][0]['mafAl'])

        # case_id_collision_all_sid_fid_diff

        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': f'chr2_22222222_{lowercase_id_large_ref_alt}'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': f'chr2_22222222_{uppercase_id_large_ref_alt}'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual(uppercase_large_ref, uppercase_variant['ref'])
        self.assertEqual(uppercase_large_alt, uppercase_variant['alt'])
        self.assertEqual([{"sid": "sid21", "fid": "fid21"},
                          {"sid": "sid211", "fid": "fid211"},
                          {"sid": "sid22", "fid": "fid22"},
                          {"sid": "sid222", "fid": "fid222"}],
                         uppercase_variant['files'])
        self.assertEqual([{"type": "genomic", "name": f"chr2:g.22222222{uppercase_hgvs_large_ref_alt}"}],
                         uppercase_variant['hgvs'])
        self.assertEqual(
            [{"maf": 0.21, "mgf": 0.21, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid21", "fid": "fid21"},
             {"maf": 0.211, "mgf": 0.211, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid211",
              "fid": "fid211"},
             {"maf": 0.22, "mgf": 0.22, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid22", "fid": "fid22"},
             {"maf": 0.222, "mgf": 0.222, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid222",
              "fid": "fid222"}],
            uppercase_variant['st'])

        # case_id_collision_sid_fid_has_more_than_one_file

        # assert lowercase variant is not deleted
        lowercase_variant_list = list(variants_coll.find({'_id': f'chr3_33333333_{lowercase_id_large_ref_alt}'}))
        self.assertEqual(1, len(lowercase_variant_list))
        uppercase_variant_list = list(variants_coll.find({'_id': f'chr3_33333333_{lowercase_id_large_ref_alt}'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert the issue is logged in the file
        non_merged_candidate_file = os.path.join(working_dir, "non_merged_candidates", f"{db_name}.txt")
        self.assertTrue(os.path.exists(non_merged_candidate_file))
        with open(non_merged_candidate_file, 'r') as nmc_file:
            self.assertEqual("('sid31', 'fid31')\n", nmc_file.readline())

        # case_id_collision_sid_fid_has_one_file
        # assert lowercase variant deleted
        lowercase_variant_list = list(variants_coll.find({'_id': f'chr4_44444444_{lowercase_id_large_ref_alt}'}))
        self.assertEqual(0, len(lowercase_variant_list))
        # assert uppercase variant inserted
        uppercase_variant_list = list(variants_coll.find({'_id': f'chr4_44444444_{uppercase_id_large_ref_alt}'}))
        self.assertEqual(1, len(uppercase_variant_list))

        # assert all things updated to uppercase in the updated variant
        uppercase_variant = uppercase_variant_list[0]
        self.assertEqual(uppercase_large_ref, uppercase_variant['ref'])
        self.assertEqual(uppercase_large_alt, uppercase_variant['alt'])
        self.assertEqual([{"sid": "sid41", "fid": "fid41"},
                          {"sid": "sid411", "fid": "fid411"},
                          {"sid": "sid42", "fid": "fid42"},
                          {"sid": "sid422", "fid": "fid422"}],
                         uppercase_variant['files'])
        self.assertEqual([{"type": "genomic", "name": f"chr4:g.44444444{uppercase_hgvs_large_ref_alt}"}],
                         uppercase_variant['hgvs'])
        self.assertEqual(
            [{"maf": 0.41, "mgf": 0.41, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid41", "fid": "fid41"},
             {"maf": 0.411, "mgf": 0.411, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid411",
              "fid": "fid411"},
             {"maf": 0.42, "mgf": 0.42, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid42", "fid": "fid42"},
             {"maf": 0.422, "mgf": 0.422, "mafAl": uppercase_large_ref, "mgfGt": "0/0", "sid": "sid422",
              "fid": "fid422"}],
            uppercase_variant['st'])


if __name__ == '__main__':
    unittest.main()
