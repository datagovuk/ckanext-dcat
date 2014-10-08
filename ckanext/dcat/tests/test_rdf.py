from nose.tools import assert_equal

from ckanext.dcat.formats import rdf
from ckanext.dcat.tests import (assert_equal2,
                                get_example_file_as_dict,
                                get_example_file_content,
                                get_sample_file_as_dict,
                                get_sample_file_content,
                                poor_mans_dict_diff
                                )
from ckanext.dcat import converters


class TestRdfToDict:
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

        assert_equal2(expected_dict, dcat_dict, ignore_keys_with_blank_values=True)


def get_extra(ckan_dict, key):
    for extra in ckan_dict['extras']:
        if extra['key'] == key:
            return extra['value']

def update_extra(ckan_dict, key, existing_value, new_value):
    for extra in ckan_dict['extras']:
        if extra['key'] == key:
            if existing_value is not None:
                assert_equal(existing_value, extra['value'])
            extra['value'] = new_value
            return
    raise 'Could not find key %s' % key


class TestRdfToCkan:
    '''This is a test converting DCAT RDF/XML -> DCAT dict -> CKAN dict'''
    def test_dataset(self):
        dcat = get_example_file_content('dataset.rdf')
        expected_ckan_dict = get_example_file_as_dict('ckan_dataset.json')

        dcat_dict = rdf.DCATDataset(dcat).read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # seconds get added on unnecessarily
        update_extra(expected_ckan_dict, 'dcat_modified',
                     '2012-05-10T21:04', '2012-05-10T21:04:00')
        # languages end up in another order
        langs = get_extra(ckan_dict, 'language')
        if 'en' in langs:
            update_extra(expected_ckan_dict, 'language', None, langs)
        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_ckan_dict, ckan_dict, ignore_order=True)

    def test_odc_dataset1(self):
        dcat = get_sample_file_content('odc_dataset1.rdf')
        expected_ckan_dict = get_sample_file_as_dict('odc_dataset1.ckan.json')

        dcat_dict = rdf.DCATDataset(dcat).read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_ckan_dict, ckan_dict, ignore_order=True)


class TestRdfDatasets:
    def test_split(self):
        dcat = get_sample_file_content('datasets.rdf')
        datasets = list(rdf.DCATDatasets(dcat).split_into_datasets())
        assert_equal(len(datasets), 2)
        # can come in any order
        uri, triples = datasets[0] if str(datasets[0]).startswith('http://opendatacommunities.org/data/test2') else datasets[1]
        assert_equal(uri, 'http://opendatacommunities.org/data/test1')
        print triples
        assert_equal(triples.strip(), '''@prefix ns1: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xml: <http://www.w3.org/XML/1998/namespace> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://opendatacommunities.org/data/test1> a <http://publishmydata.com/def/dataset#Dataset> ;
    ns1:description "Test1" .''')
