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


def assert_raises2(error, error_message, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except error, e:
        assert error_message and error_message in str(e), \
            'Raised error but "%s" was not in the error message "%s"' \
            % (error_message, str(e))
        return
    except Exception, e:
        assert 0, 'Wrong exception raised: %s not %s' % (e, error)
    assert 0, 'Did not raise exception'

class TestParse:
    def test_non_json(self):
        json_str = 'fdfd'

        assert_raises2(ParseError, 'No JSON object could be decoded', _get_guids_and_datasets, json_str)

    def test_list_with_bad_item(self):
        json_str = '["not a dataset"]'

        assert_raises2(ParseError, 'With a JSON array "[ ... ]" at the top level, each item should be a dataset object. Did not find an object "{ ... }"', _get_guids_and_datasets, json_str)

    def test_list_with_bad_dataset(self):
        json_str = '[{}]'

        assert_raises2(ParseError, 'With a JSON array "[ ... ]" at the top level, each item should be a dataset object. The first item appeared not to be a dataset because it did not have the "publisher" key.', _get_guids_and_datasets, json_str)

    def test_dict_but_not_catalog(self):
        json_str = '{}'

        assert_raises2(ParseError, 'With a JSON object "{ ... }" at the top level, it should be a catalogue object containing an array of datasets, but the "dataset" key was not found.', _get_guids_and_datasets, json_str)

    def test_catalog_with_invalid_dataset(self):
        json_str = '{"dataset": {"not": "a dataset"}}'

        assert_raises2(ParseError, 'With a Catalogue at the top level, the "dataset" key should contain a JSON array of datasets \'[ { ... }, { ... }, ... ]\', but did not find a JSON array: \'{"not": "a dataset"}\'', _get_guids_and_datasets, json_str)

    def test_catalog_with_list_of_invalid_datasets(self):
        json_str = '{"dataset": [{}]}'

        assert_raises2(ParseError, 'No title found for dataset 0 of 1', _get_guids_and_datasets, json_str)

    def test_dataset_with_no_id(self):
        json_str = '[{"publisher": "CO"}]'

        assert_raises2(ParseError, 'No title found for dataset 0 of 1', _get_guids_and_datasets, json_str)

    def test_dataset(self):
        json_str = '[{"title": "test1", "identifier": "test", "publisher": "CO"}]'

        guid, dataset = _get_guids_and_datasets(json_str)[0]

        assert_equal(guid, 'test')

    def test_catalog(self):
        json_str = '{"dataset": [{"title": "test1", "identifier": "test", "publisher": "CO"}]}'

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

    def test_eddc2(self):
        catalog_json = get_sample_file_as_dict('eddc2.json')
        expected_ckan_dict = get_sample_file_as_dict('eddc2.ckan.json')

        guid, dataset_json_str = _get_guids_and_datasets(json.dumps(catalog_json))[0]
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

