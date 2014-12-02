from nose.tools import assert_raises, assert_equal
import json

from ckanext.dcat.harvesters import DCATJSONHarvester
from ckanext.dcat.formats import ParseError
from ckanext.dcat import converters
from ckanext.dcat.tests import (assert_equal2,
                                get_sample_file_as_dict,
                                get_sample_file_content,
                                change_extra_value,
                                )

_get_guids_and_datasets = DCATJSONHarvester._get_guids_and_datasets


class TestParse:
    def test_non_json(self):
        json_str = 'fdfd'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_list_with_bad_item(self):
        json_str = '["not a dataset"]'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_list_with_bad_dataset(self):
        json_str = '[{}]'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_dict_but_not_catalog(self):
        json_str = '{}'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_catalog_with_invalid_dataset(self):
        json_str = '{"dataset": {"not": "a dataset"}}'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_catalog_with_list_of_invalid_datasets(self):
        json_str = '{"dataset": [{}]}'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_dataset_with_no_id(self):
        json_str = '[{"publisher": "CO"}]'

        assert_raises(ParseError, _get_guids_and_datasets, json_str)

    def test_dataset(self):
        json_str = '[{"identifier": "test", "publisher": "CO"}]'

        guid, dataset = _get_guids_and_datasets(json_str)[0]

        assert_equal(guid, 'test')

    def test_catalog(self):
        json_str = '{"dataset": [{"identifier": "test", "publisher": "CO"}]}'

        guid, dataset = _get_guids_and_datasets(json_str)[0]

        assert_equal(guid, 'test')


class TestSamples:

    def test_eddc1_dataset1(self):
        catalog_json = get_sample_file_as_dict('eddc1.json')
        expected_ckan_dict = get_sample_file_as_dict('eddc1.ckan.json')

        guid, dataset_json_str = _get_guids_and_datasets(json.dumps(catalog_json))[0]
        dataset_json = json.loads(dataset_json_str)
        ckan_dict = converters.dcat_to_ckan(dataset_json)

        assert_equal2(ckan_dict, expected_ckan_dict)

    def test_eddc1_dataset2(self):
        catalog_json = get_sample_file_as_dict('eddc1.json')
        expected_ckan_dict = get_sample_file_as_dict('eddc1.d2.ckan.json')

        guid, dataset_json_str = _get_guids_and_datasets(json.dumps(catalog_json))[1]
        dataset_json = json.loads(dataset_json_str)
        ckan_dict = converters.dcat_to_ckan(dataset_json)

        assert_equal2(ckan_dict, expected_ckan_dict)

    def test_dft1(self):
        json_str = get_sample_file_as_dict('dft1.json')
        expected_ckan_dict = get_sample_file_as_dict('dft1.ckan.json')

        ckan_dict = converters.dcat_to_ckan(json_str)

        # dataset.json doesn't store a URI, so ckan_dataset.json will not have one
        #change_extra_value(expected_ckan_dict, 'metadata_uri', None)

        assert_equal2(ckan_dict, expected_ckan_dict)

