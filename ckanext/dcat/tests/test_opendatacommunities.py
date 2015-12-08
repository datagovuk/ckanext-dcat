from nose.tools import assert_equal

try:
    from ckan.tests.helpers import reset_db
except ImportError:
    from ckan.new_tests.helpers import reset_db

import ckanext.harvest.model as harvest_model
from ckanext.harvest.tests.lib import run_harvest

from ckanext.dcat.harvesters import DCATRDFHarvester

import mock_odc

# Start ODC-alike server we can test harvesting against it
mock_odc.serve()


class TestRdfHarvest(object):
    @classmethod
    def setup_class(cls):
        reset_db()
        harvest_model.setup()

    def test_simple_harvest(self):
        test_name = 'dataset1'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/administrative-geography'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'added')
        assert_equal(result['errors'], [])

    def test_ignore_developers_corner_by_subject(self):
        test_name = 'developers-corner-subject'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/dev-local-authorities'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])

    def test_ignore_developers_corner_by_theme(self):
        test_name = 'developers-corner-theme'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/dev-local-authorities'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])

    def test_ignore_developers_corner_by_folder(self):
        test_name = 'developers-corner-folder'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/ssd-test'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])

    def test_ignore_geography_by_subject(self):
        test_name = 'geography-subject'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/geography/greenbelt'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])

    def test_ignore_geography_by_theme(self):
        test_name = 'geography-theme'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/geography/greenbelt'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])

    def test_ignore_geography_by_folder(self):
        test_name = 'geography-folder'

        results_by_guid = run_harvest(
            url='http://localhost:%s/%s/data.rdf' % (mock_odc.PORT, test_name),
            harvester=DCATRDFHarvester())

        assert_equal(results_by_guid.keys(), ['http://opendatacommunities.org/data/geography/greenbelt'])
        result = results_by_guid.values()[0]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])
