#coding: utf-8

import os
import uuid
import json
import logging
from hashlib import sha1

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from lxml import etree
import requests

from ckan import plugins as p
from ckan import logic
from ckan import model

from ckanext.harvest.harvesters.base import HarvesterBase, PackageDictError
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

from ckanext.dcat import converters
from ckanext.dcat.formats import ParseError, rdf, xml_

log = logging.getLogger(__name__)

class DCATHarvester(HarvesterBase):

    p.implements(IHarvester)

    MAX_FILE_SIZE = 1024 * 1024 * 50 # 50 Mb
    CHUNK_SIZE = 1024


    force_import = False

    _user_name = None

    def _get_content(self, url, harvest_job, page=1):
        if not url.lower().startswith('http'):
            # Check local file
            if os.path.exists(url):
                with open(url, 'r') as f:
                    content = f.read()
                return content
            else:
                self._save_gather_error('Could not get content for this url', harvest_job)
                return None

        try:
            if page > 1:
                url = url + '&' if '?' in url else url + '?'
                url = url + 'page={0}'.format(page)

            log.debug('Getting file %s', url)

            # first we try a HEAD request which may not be supported
            did_get = False
            r = requests.head(url)
            if r.status_code in (405, 400):
                # HEAD request isn't support (405 Not Supported or 400 more
                # general error e.g. Socrata) so fall back to GET
                r = requests.get(url, stream=True)
                did_get = True
            r.raise_for_status()

            cl = r.headers['content-length']
            if cl and int(cl) > self.MAX_FILE_SIZE:
                msg = '''Remote file is too big. Allowed
                    file size: {allowed}, Content-Length: {actual}.'''.format(
                    allowed=self.MAX_FILE_SIZE, actual=cl)
                self._save_gather_error(msg, harvest_job)
                return None

            if not did_get:
                r = requests.get(url, stream=True)

            length = 0
            content = ''
            for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                content = content + chunk
                length += len(chunk)

                if length >= self.MAX_FILE_SIZE:
                    self._save_gather_error('Remote file is too big.', harvest_job)
                    return None

            try:
                import chardet
                encoding_dict = chardet.detect(content)
                log.debug('Encoding detected: %r', encoding_dict)
                allowed_encodings = set(('ascii', 'utf-8'))
                if encoding_dict['confidence'] > 0.8 and \
                        encoding_dict['encoding'].lower() not in allowed_encodings:
                    self._save_gather_error('File encoding is detected as "%s" when it should be one of: "%s"' % encoding_dict['encoding'], '" "'.join(allowed_encodings), harvest_job)
                    return None
            except ImportError:
                log.debug('Skipping encoding check as chardet is not installed')

            return content

        except requests.exceptions.HTTPError, error:
            if page > 1 and error.response.status_code == 404:
                # We want to catch these ones later on
                raise

            msg = 'Could not get content. Server responded with %s %s' % (
                error.response.status_code, error.response.reason)
            self._save_gather_error(msg, harvest_job)
            return None
        except requests.exceptions.ConnectionError, error:
            msg = '''Could not get content because a
                                connection error occurred. %s''' % error
            self._save_gather_error(msg, harvest_job)
            return None
        except requests.exceptions.Timeout, error:
            msg = 'Could not get content because the connection timed out.'
            self._save_gather_error(msg, harvest_job)
            return None


    def _get_user_name(self):
        if self._user_name:
            return self._user_name

        user = p.toolkit.get_action('get_site_user')({'ignore_auth': True}, {})
        self._user_name = user['name']

        return self._user_name

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_package_name(self, harvest_object, title):

        package = harvest_object.package
        if package is None or package.title != title:
            name = self._gen_new_name(title)
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
        else:
            name = package.name

        return name

    def get_original_url(self, harvest_object_id):
        obj = model.Session.query(HarvestObject).\
                                    filter(HarvestObject.id==harvest_object_id).\
                                    first()
        if obj:
            return obj.source.url
        return None

    ## Start hooks

    def modify_package_dict(self, package_dict, dcat_dict, harvest_object):
        '''
            Allows custom harvesters to modify the package dict before
            creating or updating the actual package.
        '''
        return package_dict

    ## End hooks

    def gather_stage(self,harvest_job):
        log.debug('In DCATHarvester gather_stage')


        ids = []

        # Get the previous guids for this source
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                                    filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = guid_to_package_id.keys()
        guids_in_source = []


        # Get file contents
        url = harvest_job.source.url

        previous_content = ''
        page = 1
        while True:

            try:
                content = self._get_content(url, harvest_job, page)
            except requests.exceptions.HTTPError, error:
                if error.response.status_code == 404:
                    if page > 1:
                        # Server returned a 404 after the first page, no more
                        # records
                        log.debug('404 after first page, no more pages')
                        break
                    else:
                        # Proper 404
                        msg = 'Could not get content. Server responded with 404 Not Found'
                        self._save_gather_error(msg, harvest_job)
                        return None
                else:
                    # This should never happen. Raising just in case.
                    raise

            if not content:
                return None

            if previous_content == content:
                # Server does not support pagination or no more pages
                log.debug('Same content, no more pages')
                break

            try:
                batch_guids = []
                for guid, as_string in self._get_guids_and_datasets(content):

                    log.debug('Got identifier: {0}'.format(guid))
                    batch_guids.append(guid)

                    if guid in guids_in_db:
                        # Dataset needs to be updated
                        obj = HarvestObject(guid=guid, job=harvest_job,
                                        package_id=guid_to_package_id[guid],
                                        content=as_string,
                                        extras=[HarvestObjectExtra(key='status', value='changed')])
                    else:
                        # Dataset needs to be created
                        obj = HarvestObject(guid=guid, job=harvest_job,
                                        content=as_string,
                                        extras=[HarvestObjectExtra(key='status', value='new')])

                    obj.save()
                    ids.append(obj.id)

                if len(batch_guids) > 0:
                    guids_in_source.extend(batch_guids)
                else:
                    log.debug('Empty document, no more records')
                    # Empty document, no more ids
                    break

            except ParseError, e:
                msg = 'Error parsing: {0}'.format(str(e))
                self._save_gather_error(msg, harvest_job)
                return None
            except ValueError, e:
                msg = 'Error parsing file: {0}'.format(str(e))
                self._save_gather_error(msg, harvest_job)
                return None



            page = page + 1
            previous_content = content

        # Check datasets that need to be deleted
        guids_to_delete = set(guids_in_db) - set(guids_in_source)
        for guid in guids_to_delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status', value='delete')])
            model.Session.flush()  # give the obj an ID
            log.debug('To delete GUID="%s" id=%r', guid, obj.id)
            ids.append(obj.id)
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()


        return ids

    def fetch_stage(self,harvest_object):
        return True

    def import_stage(self,harvest_object):
        log.debug('In DCATHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if self.force_import:
            status = 'change'
        else:
            status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # Delete package
            context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}

            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))

            return True


        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()


        package_dict, dcat_dict = self._get_package_dict(harvest_object)
        if not package_dict.get('name'):
            package_dict['name'] = self._get_package_name(harvest_object, package_dict['title'])

        # Allow custom harvesters to modify the package dict before creating
        # or updating the package
        package_dict = self.modify_package_dict(package_dict,
                                                dcat_dict,
                                                harvest_object)

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        context = {
            'user': self._get_user_name(),
            'return_id_only': True,
            'ignore_auth': True,
        }

        if status == 'new':


            package_schema = logic.schema.default_create_package_schema()
            context['schema'] = package_schema

            # We need to explicitly provide a package ID
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()

            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            package_id = p.toolkit.get_action('package_create')(context, package_dict)
            log.info('Created dataset with id %s', package_id)
        elif status == 'change':

            package_dict['id'] = harvest_object.package_id
            package_id = p.toolkit.get_action('package_update')(context, package_dict)
            log.info('Updated dataset with id %s', package_id)

        model.Session.commit()

        return True


class DCATXMLHarvester(DCATHarvester):

    DCAT_NS = 'http://www.w3.org/ns/dcat#'
    DCT_NS = 'http://purl.org/dc/terms/'
    RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

    def info(self):
        return {
            'name': 'dcat_xml',
            'title': 'DCAT XML-RDF Harvester',
            'description': 'Harvester for DCAT dataset descriptions serialized as XML-RDF'
        }


    def _get_guids_and_datasets(self, content):

        doc = etree.fromstring(content)

        for dataset_element in doc.xpath('//dcat:Dataset',namespaces={'dcat': self.DCAT_NS}) :

            as_string = etree.tostring(dataset_element)

            # Get identifier
            guid = dataset_element.get('{{{ns}}}about'.format(ns=self.RDF_NS))
            if not guid:
                id_element = dataset_element.find('{{{ns}}}identifier'.format(ns=self.DCT_NS))
                if id_element:
                    guid = id_element.strip()
                else:
                    # This is bad, any ideas welcomed
                    guid = sha1(as_string).hexdigest()

            yield guid, as_string


    def _get_package_dict(self, harvest_object):

        content = harvest_object.content

        dataset = xml_.DCATDataset(content)
        dcat_dict = dataset.read_values()

        package_dict = converters.dcat_to_ckan(dcat_dict)

        return package_dict, dcat_dict

class DCATJSONHarvester(DCATHarvester):

    # Monkey patch in the base import_stage
    import_stage = HarvesterBase.import_stage

    def info(self):
        return {
            'name': 'data_json',
            'title': 'data.json',
            'description': 'data.json (UK-variant) - simple JSON serialization of DCAT dataset'
        }

    @classmethod
    def _get_guids_and_datasets(cls, content):

        try:
            doc = json.loads(content)
        except ValueError, e:
            raise ParseError('Error parsing JSON: %s' % e)

        if isinstance(doc, list):
            # Assume a list of datasets
            datasets = doc
            if datasets:
                if not isinstance(datasets[0], dict):
                    raise ParseError('Could not recognize JSON structure. With a JSON array "[ ... ]" at the top level, each item should be a dataset object. Did not find an object "{ ... }"')
                if 'publisher' not in datasets[0]:
                    raise ParseError('Could not recognize JSON structure. With a JSON array "[ ... ]" at the top level, each item should be a dataset object. The first item appeared not to be a dataset because it did not have the "publisher" key.')
        elif isinstance(doc, dict):
            if 'dataset' in doc:
                # It is a Catalog
                datasets = doc['dataset']
                if not isinstance(datasets, list):
                    raise ParseError('Could not recognize JSON structure. With a Catalogue at the top level, the "dataset" key should contain a JSON array of datasets \'[ { ... }, { ... }, ... ]\', but did not find a JSON array: \'%s\'' % json.dumps(datasets))
            elif 'publisher' in doc:
                # It is a single dataset
                datasets = [doc]
            else:
                raise ParseError('Could not recognize JSON structure. With a JSON object "{ ... }" at the top level, it should be a catalogue object containing an array of datasets, but the "dataset" key was not found.')
        else:
            raise ParseError('Could not recognize JSON structure as either a catalogue or an array of datasets.')

        guid_and_dataset_json_str = []
        for i, dataset in enumerate(datasets):

            if not isinstance(dataset, dict):
                raise ParseError('Expected dataset JSON object, but got \'%s\' '
                                 '(dataset list %s of %s)' %
                                 (json.dumps(dataset), i, len(datasets)))
            as_string = json.dumps(dataset)

            # Get identifier
            if not dataset.get('title'):
                raise ParseError('No title found for dataset %s of %s' %
                                 (i, len(datasets)))
            guid = dataset.get('identifier') or dataset.get('uri')
            if not guid:
                raise ParseError('No identifier found for dataset "%s" '
                                 '(%s of %s)' %
                                 (dataset.get('title'), i, len(datasets)))

            guid_and_dataset_json_str.append((guid, as_string))
            # don't yield it because a generator cannot raise exceptions (parse errors)
        return guid_and_dataset_json_str

    # DCATHarvester version
    def _get_package_dict(self, harvest_object):

        content = harvest_object.content

        dcat_dict = json.loads(content)

        package_dict = converters.dcat_to_ckan(dcat_dict)

        return package_dict, dcat_dict

    # HarvesterBase version
    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config, existing_dataset):

        content = harvest_object.content

        dcat_dict = json.loads(content)

        try:
            package_dict_harvested = converters.dcat_to_ckan(dcat_dict)
        except converters.ConvertError, e:
            raise PackageDictError(str(e))

        # convert extras to a dict for the defaults merge
        package_dict_harvested['extras'] = \
            dict((extra['key'], extra['value'])
                 for extra in package_dict_harvested['extras'])
        # convert tags to a list for the defaults merge
        package_dict_harvested['tags'] = \
            [tag_dict['name'] for tag_dict in package_dict_harvested['tags']]

        package_dict = package_dict_defaults.merge(package_dict_harvested)

        if not existing_dataset:
            package_dict['name'] = self.munge_title_to_name(package_dict['title'])
            package_dict['name'] = self.check_name(package_dict['name'])

        # Harvest GUID needs setting manually as DCAT has a clashing 'GUID'
        # extra that comes from the dct:identifier
        package_dict['extras']['guid'] = package_dict_defaults['extras']['guid']

        # set publisher according the harvest source publisher
        # and any value for dcat_publisher gets stored in an extra

        # DGU Theme
        try:
            from ckanext.dgu.lib.theme import categorize_package, PRIMARY_THEME, SECONDARY_THEMES
            # Guess theme from other metadata
            themes = categorize_package(package_dict)
            if themes:
                package_dict['extras'][PRIMARY_THEME] = themes[0]
                package_dict['extras'][SECONDARY_THEMES] = json.dumps(themes[1:])
        except ImportError:
            pass
        log.debug('Theme: %s', package_dict['extras'].get('theme-primary'))

        # DGU-specific license field
        if not package_dict.get('license_id') and package_dict['extras'].get('license_url'):
            # abuses the license_id field, but that's what DGU does
            package_dict['license_id'] = package_dict['extras']['license_url']

        # DGU contact details
        if package_dict['extras'].get('contact_email'):
            package_dict['extras']['contact-email'] = package_dict['extras']['contact_email']

        # convert for package_update
        package_dict['extras'] = self.extras_from_dict(package_dict['extras'])
        package_dict['tags'] = [{'name': tag} for tag in package_dict['tags']]

        return package_dict

    def get_dataset_validator(self):
        # potential JSON validator
        # You can do: validator.iter_errors(instance)
        from jsonschema import Draft4Validator, FormatChecker
        schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'dataset.json')

        with open(schema_path, 'r') as file:
            schema = json.loads(file.read())
        return Draft4Validator(schema, format_checker=FormatChecker())

class DCATRDFHarvester(DCATHarvester):

    # Monkey patch in the base import_stage
    import_stage = HarvesterBase.import_stage

    DCAT_NS = 'http://www.w3.org/ns/dcat#'
    DCT_NS = 'http://purl.org/dc/terms/'
    RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

    def info(self):
        return {
            'name': 'dcat_rdf',
            'title': 'DCAT RDF/XML Harvester',
            'description': 'DCAT dataset descriptions serialized as RDF/XML'
        }

    def _get_guids_and_datasets(self, content):

        rdf_doc = rdf.DCATDatasets(content)
        for uri, dataset_rdf_str in rdf_doc.split_into_datasets():

            guid = uri
            if not guid:
                raise AssertionError('No guid')

            yield guid, dataset_rdf_str

    # DCATHarvester version
    def _get_package_dict(self, harvest_object):

        content = harvest_object.content

        dataset = rdf.DCATDataset(content)
        try:
            dcat_dict = dataset.read_values()
        except rdf.ReadValueError, e:
            raise PackageDictError(str(e))

        package_dict = converters.dcat_to_ckan(dcat_dict)

        return package_dict, dcat_dict

    # HarvesterBase version
    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config, existing_dataset):

        content = harvest_object.content

        dataset = rdf.DCATDataset(content)
        try:
            dcat_dict = dataset.read_values()
        except rdf.ReadValueError, e:
            raise PackageDictError(str(e))

        package_dict_harvested = converters.dcat_to_ckan(dcat_dict)

        # convert extras to a dict for the defaults merge
        package_dict_harvested['extras'] = \
            dict((extra['key'], extra['value'])
                 for extra in package_dict_harvested['extras'])
        # convert tags to a list for the defaults merge
        package_dict_harvested['tags'] = \
            [tag_dict['name'] for tag_dict in package_dict_harvested['tags']]

        package_dict = package_dict_defaults.merge(package_dict_harvested)

        if not existing_dataset:
            package_dict['name'] = self.munge_title_to_name(package_dict['title'])
            package_dict['name'] = self.check_name(package_dict['name'])

        # ODC specific - discard datasets that are not ready
        if package_dict['extras'].get('dcat_subject') == u'http://opendatacommunities.org/def/concept/themes/developer-corner':
            log.info('Discarding dataset with theme "developer-corner": %s', harvest_object.guid)
            return None

        # Harvest GUID needs setting manually as DCAT has a clashing 'GUID'
        # extra that comes from the dct:identifier
        package_dict['extras']['guid'] = package_dict_defaults['extras']['guid']

        # set publisher according the harvest source publisher
        # and any value for dcat_publisher gets stored in an extra

        # DGU Theme
        try:
            from ckanext.dgu.lib.theme import categorize_package, PRIMARY_THEME, SECONDARY_THEMES
            # Guess theme from other metadata
            themes = categorize_package(package_dict)
            if themes:
                package_dict['extras'][PRIMARY_THEME] = themes[0]
                package_dict['extras'][SECONDARY_THEMES] = json.dumps(themes[1:])
        except ImportError:
            pass
        log.debug('Theme: %s', package_dict['extras'].get('theme-primary'))

        # DGU-specific license field
        if not package_dict.get('license_id') and package_dict['extras'].get('license_url'):
            # abuses the license_id field, but that's what DGU does
            package_dict['license_id'] = package_dict['extras']['license_url']

        # DGU contact details
        if package_dict['extras'].get('contact_email'):
            package_dict['extras']['contact-email'] = package_dict['extras']['contact_email']

        # DGU package.url as a resource
        if package_dict['url']:
            package_dict['resources'].append(
                {'name': 'Dataset home page',
                 'description': None,
                 'url': package_dict['url'],
                 'format': 'HTML',
                 'resource_type': 'documentation'
                })

        # convert for package_update
        package_dict['extras'] = self.extras_from_dict(package_dict['extras'])
        package_dict['tags'] = [{'name': tag} for tag in package_dict['tags']]

        return package_dict
