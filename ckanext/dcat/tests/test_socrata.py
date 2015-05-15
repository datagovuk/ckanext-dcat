from ckanext.dcat import converters
from ckanext.dcat.tests import (poor_mans_dict_diff, assert_equal2,
                                get_sample_file_as_dict,
                                get_sample_file_content)


class TestConvertersForSocrata(object):

    def test_socrata1_to_ckan(self):
        dcat_dict = get_sample_file_as_dict('socrata_dataset1.json')
        expected_ckan_dict = get_sample_file_as_dict('socrata_dataset1.ckan.json')

        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        assert_equal2(ckan_dict, expected_ckan_dict)
        #assert ckan_dict == expected_ckan_dict, poor_mans_dict_diff(
        #    expected_ckan_dict, ckan_dict)

    def test_socrata2_to_ckan(self):
        dcat_dict = get_sample_file_as_dict('socrata_dataset2.json')
        expected_ckan_dict = get_sample_file_as_dict('socrata_dataset2.ckan.json')

        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        assert_equal2(expected_ckan_dict, ckan_dict)
        #assert ckan_dict == expected_ckan_dict, poor_mans_dict_diff(
        #    expected_ckan_dict, ckan_dict)
