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

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()

        # the URI is optional for the json serialization - this is ok
        expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'
        # seconds get added on unnecessarily
        expected_dict['modified'] = expected_dict['modified'].replace('21:04', '21:04:00')

        assert_equal2(expected_dict, dcat_dict, ignore_keys_with_blank_values=True,
                      ignore_order=True)

    def test_odc_dataset1(self):
        dcat = get_sample_file_content('odc_dataset1.rdf')
        expected_dict = get_sample_file_as_dict('odc_dataset1.json')

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()

        assert_equal2(expected_dict, dcat_dict, ignore_keys_with_blank_values=True)

    def test_fsa_dataset1(self):
        dcat = get_sample_file_content('fsa_dataset1.rdf')
        expected_dict = get_sample_file_as_dict('fsa_dataset1.json')

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()

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
    raise Exception('Could not find key %s' % key)


class TestRdfToCkan:
    '''This is a test converting DCAT RDF/XML -> DCAT dict -> CKAN dict'''
    def test_dataset(self):
        dcat = get_example_file_content('dataset.rdf')
        expected_ckan_dict = get_example_file_as_dict('ckan_dataset.json')

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # seconds get added on unnecessarily
        update_extra(expected_ckan_dict, 'data_modified',
                     '2012-05-10T21:04', '2012-05-10T21:04:00')
        # URL is carried through from the DCAT
        expected_ckan_dict['url'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'
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

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_ckan_dict, ckan_dict, ignore_order=True)

    def test_fsa_dataset1(self):
        dcat = get_sample_file_content('fsa_dataset1.rdf')
        expected_ckan_dict = get_sample_file_as_dict('fsa_dataset1.ckan.json')

        dcat_dict = rdf.DCATDataset(dcat, format='xml').read_values()
        ckan_dict = converters.dcat_to_ckan(dcat_dict)

        # the URI is optional for the json serialization - this is ok
        #expected_dict['uri'] = 'https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98'

        assert_equal2(expected_ckan_dict, ckan_dict, ignore_order=True)

class TestRdfDatasets:
    def test_split(self):
        dcat = get_sample_file_content('datasets.rdf')
        datasets = list(rdf.DCATDatasets(dcat).split_into_datasets())
        assert_equal(len(datasets), 2)
        # datasets can come in any order
        print [str(d)[:50] for d in datasets]
        if 'http://opendatacommunities.org/data/test1' in str(datasets[0])[:50]:
            uri1, triples1 = datasets[0]
            triples2 = datasets[1][1]
        elif 'http://opendatacommunities.org/data/test2' in str(datasets[0])[:50]:
            uri1, triples1 = datasets[1]
            triples2 = datasets[0][1]
        else:
            assert 0, str(datasets[0])[:50]
        assert_equal(uri1, 'http://opendatacommunities.org/data/test1')
        print triples1
        assert_equal(triples1.strip(), '''@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix ns1: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xml: <http://www.w3.org/XML/1998/namespace> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://opendatacommunities.org/data/test1> a <http://publishmydata.com/def/dataset#Dataset> ;
    ns1:description "Test1" .''')
        print triples2
        assert_equal(triples2.strip(), '''@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix ns1: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xml: <http://www.w3.org/XML/1998/namespace> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://opendatacommunities.org/data/test2> a dcat:Dataset ;
    ns1:description "Test2" ;
    dcat:distribution <http://data.food.gov.uk/catalog/data/distribution/f1ad63d1-909e-4d97-a1ba-4f0f6037772b> .

<http://data.food.gov.uk/catalog/data/distribution/f1ad63d1-909e-4d97-a1ba-4f0f6037772b> a dcat:Distribution ;
    dcat:mediaType "text/csv" .''')
