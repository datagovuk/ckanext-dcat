from ckanext.dcat import converters
from ckanext.dcat.tests import (poor_mans_dict_diff, get_example_file_as_dict,
                                change_extra_value)


class TestConverters(object):

    def test_ckan_to_dcat(self):
        ckan_dict = get_example_file_as_dict('full_ckan_dataset.json')
        expected_dcat_dict = get_example_file_as_dict('dataset.json')

        dcat_dict = converters.ckan_to_dcat(ckan_dict)

        assert dcat_dict == expected_dcat_dict, poor_mans_dict_diff(
            expected_dcat_dict, dcat_dict)

    def test_dcat_to_ckan(self):
        dcat_dict = get_example_file_as_dict('dataset.json')
        expected_ckan_dict = get_example_file_as_dict('ckan_dataset.json')

        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # dataset.json doesn't store a URI, so ckan_dataset.json will not have one
        change_extra_value(expected_ckan_dict, 'metadata_uri', None)

        assert ckan_dict == expected_ckan_dict, poor_mans_dict_diff(
            expected_ckan_dict, ckan_dict)

    # This roundtrip doesn't work - the mapping to DCAT doesn't store lots of
    # CKAN info, particularly:
    #   ID, extras, maintainer, metadata_created/modified
    #def test_ckan_to_dcat_to_ckan(self):
    #    ckan_dict = get_example_file_as_dict('full_ckan_dataset.json')
    #
    #    dcat_dict = converters.ckan_to_dcat(ckan_dict)
    #    new_ckan_dict = converters.dcat_to_ckan(dcat_dict)
    #
    #    assert ckan_dict == new_ckan_dict, poor_mans_dict_diff(
    #        ckan_dict, new_ckan_dict)

    # This roundtrip works, but no doubt you could add DCAT predicates that are
    # not converted to CKAN properties.
    def test_dcat_to_ckan_to_dcat(self):
        dcat_dict = get_example_file_as_dict('dataset.json')

        ckan_dict = converters.dcat_to_ckan(dcat_dict)
        new_dcat_dict = converters.ckan_to_dcat(ckan_dict)

        assert dcat_dict == new_dcat_dict, poor_mans_dict_diff(
            dcat_dict, new_dcat_dict)
