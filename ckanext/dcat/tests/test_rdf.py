from ckanext.dcat.formats import rdf
from ckanext.dcat.tests import (assert_equal2,
                                get_example_file_as_dict,
                                get_example_file_content,
                                get_sample_file_as_dict,
                                get_sample_file_content,
                                poor_mans_dict_diff
                                )
from ckanext.dcat import converters


class TestXmlToDict:
    '''This is a unit test formats/rdf.py which converts the DCAT in RDF/XML (etc)
    into a DCAT dictionary. (This would then get converted to a CKAN dict using
    converters.py).'''
    def test_dataset(self):
        dcat = get_example_file_content('dataset.rdf')
        expected_dict = get_example_file_as_dict('dataset.json')

        dcat_dict = rdf.DCATDataset(dcat).read_values()

        # the URI is optional for the json serialization - this is ok
        expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'
        # seconds get added on unnecessarily
        expected_dict['modified'] = expected_dict['modified'].replace('21:04', '21:04:00')

        assert_equal2(expected_dict, dcat_dict, ignore_keys_with_blank_values=True,
                      ignore_order=True)

    def test_odc_dataset1(self):
        dcat = get_sample_file_content('odc_dataset1.rdf')
        expected_dict = get_sample_file_as_dict('odc_dataset1.json')

        dcat_dict = rdf.DCATDataset(dcat).read_values()

        # this title has no text in the XML, but is empty string in the JSON - this is ok
        #del expected_dict['distribution'][0]['title']
        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_dict, dcat_dict, ignore_keys_with_blank_values=True)

class TestXmlToCkan:
    '''This is a test converting DCAT in RDF/XML to CKAN dictionary.'''
    def test_dataset(self):
        dcat = get_example_file_content('dataset.rdf')
        expected_ckan_dict = get_example_file_as_dict('ckan_dataset.json')

        dcat_dict = rdf.DCATDataset(dcat).read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # the title has no text in the XML, but is empty string in the CKAN dict - this is ok
        del expected_ckan_dict['resources'][0]['name']
        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_ckan_dict, ckan_dict)

