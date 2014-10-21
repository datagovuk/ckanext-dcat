import logging

log = logging.getLogger(__name__)


def dcat_to_ckan(dcat_dict):

    package_dict = {}

    package_dict['title'] = dcat_dict.get('title')
    package_dict['notes'] = dcat_dict.get('description')
    package_dict['url'] = dcat_dict.get('landingPage') or dcat_dict.get('uri')

    package_dict['tags'] = []
    for keyword in (dcat_dict.get('keyword') or []):
        package_dict['tags'].append({'name': keyword})

    package_dict['extras'] = []
    for key in ['issued', 'modified']:
        # NB these dates refer to when the data itself was changed, not the
        # metadata
        package_dict['extras'].append({'key': 'data_{0}'.format(key), 'value': dcat_dict.get(key)})

    # DR: I agree with keeping the URI and dct:identifier separate - the
    # dct:identifier might be some hex rather than a URI. However I'm not sure
    # about calling it 'guid' as the harvest_object.guid defaults to the URI.
    package_dict['extras'].append({'key': 'guid', 'value': dcat_dict.get('identifier')})
    package_dict['extras'].append({'key': 'metadata_uri', 'value': dcat_dict.get('uri')})

    # When harvested, the owner_org will be set according to the harvest
    # source. So the dcat.publisher is really a secondary organization, to
    # store in an extra for reference.
    dcat_publisher = dcat_dict.get('publisher')
    if isinstance(dcat_publisher, basestring):
        package_dict['extras'].append({'key': 'dcat_publisher_name', 'value': dcat_publisher})
    elif isinstance(dcat_publisher, dict):
        if dcat_publisher.get('name'):
            package_dict['extras'].append({'key': 'dcat_publisher_name', 'value': dcat_publisher.get('name')})
        if dcat_publisher.get('uri'):
            package_dict['extras'].append({'key': 'dcat_publisher_uri', 'value': dcat_publisher.get('uri')})
        # it's not normal for a harvester to edit the publisher's email
        # address, so just store this info in an extra
        if dcat_publisher.get('mbox'):
            package_dict['extras'].append({'key': 'dcat_publisher_email', 'value': dcat_publisher.get('mbox')})

    contact_email = dcat_dict.get('contactEmail')
    if contact_email:
        package_dict['extras'].append({'key': 'contact_email', 'value': contact_email})

    # subject is a URI, so although it is similar to a tag, it will need some
    # more work.  It is used to set the theme in DGU.
    subjects = dcat_dict.get('subject')
    if subjects:
        package_dict['extras'].append({'key': 'dcat_subject', 'value': ' '.join(subjects)})

    # The dcat licence URL will need matching to find the equivalent CKAN
    # licence_id if there is one. So alway store it in an extra, and if there
    # is a match, write the license_id.
    dcat_license = dcat_dict.get('license')
    if dcat_license == 'No license provided':
        # Socrata convention
        dcat_license = None
    if dcat_license:
        # Should it be a URL or textual title of the license?
        # NB DCAT gives you a URL, the data.json spec is not clear, and the
        # data.json examples appear to be textual. e.g. "Public Domain":
        # * http://eeoc.gov/data.json (?)
        # * https://nycopendata.socrata.com/data.json (Socrata)
        if dcat_license.startswith('http'):
            package_dict['extras'].append({'key': 'license_url', 'value': dcat_license})
            matched_ckan_license_id = find_license_by_uri(dcat_license)
            if matched_ckan_license_id:
                package_dict['license_id'] = matched_ckan_license_id
        else:
            package_dict['extras'].append({'key': 'license_name', 'value': dcat_license})
            matched_ckan_license_id = find_license_by_title(dcat_license)
            if matched_ckan_license_id:
                package_dict['license_id'] = matched_ckan_license_id

    #if dcat_dict.get('isReplacedBy'):
    #    # This means the dataset is obsolete and needs deleting in CKAN.
    #    # This is a suggestion, but not used yet, so is commented out.
    #    import pdb; pdb.set_trace()
    #    package_dict['state'] = 'deleted'

    package_dict['extras'].append({
        'key': 'language',
        'value': ','.join(dcat_dict.get('language') or [])
    })

    package_dict['resources'] = []
    for distribution in (dcat_dict.get('distribution') or []):
        resource = {
            'name': distribution.get('title'),
            'description': distribution.get('description'),
            'url': distribution.get('downloadURL') or distribution.get('accessURL'),
            'format': distribution.get('format'),
        }

        if distribution.get('byteSize'):
            try:
                resource['size'] = int(distribution.get('byteSize'))
            except ValueError:
                pass
        package_dict['resources'].append(resource)
    if dcat_dict.get('dataDump'):
        package_dict['resources'].append({
            'name': 'Data dump',
            'description': None,
            'url': dcat_dict.get('dataDump'),
            'format': 'RDF',
            'resource_type': 'file',
        })
    if dcat_dict.get('sparqlEndpoint'):
        package_dict['resources'].append({
            'name': 'SPARQL Endpoint',
            'description': None,
            'url': dcat_dict.get('sparqlEndpoint'),
            'format': 'SPARQL',
            'resource_type': 'api',
        })
    if dcat_dict.get('zippedShapefile'):
        package_dict['resources'].append({
            'name': 'Data as shapefile (zipped)',
            'description': None,
            'url': dcat_dict.get('zippedShapefile'),
            'format': 'SHP',
            'resource_type': 'file',
        })
    # ODC don't want this. Is there a better way to add docs?
    #for reference in (dcat_dict.get('references') or []):
    #    package_dict['resources'].append({
    #        'name': 'Reference',
    #        'description': None,
    #        'url': reference,
    #        'format': 'HTML',
    #        'resource_type': 'documentation',
    #    })

    return package_dict


def ckan_to_dcat(package_dict):

    dcat_dict = {}

    dcat_dict['title'] = package_dict.get('title')
    dcat_dict['description'] = package_dict.get('notes')
    dcat_dict['landingPage'] = package_dict.get('url')


    dcat_dict['keyword'] = []
    for tag in (package_dict.get('tags') or []):
        dcat_dict['keyword'].append(tag['name'])


    dcat_dict['publisher'] = {}

    for extra in (package_dict.get('extras') or []):
        if extra['key'] in ['data_issued', 'data_modified']:
            dcat_dict[extra['key'].replace('data_', '')] = extra['value']

        elif extra['key'] == 'language':
            dcat_dict['language'] = extra['value'].split(',')

        elif extra['key'] == 'dcat_publisher_name':
            dcat_dict['publisher']['name'] = extra['value']

        elif extra['key'] == 'dcat_publisher_email':
            dcat_dict['publisher']['mbox'] = extra['value']

        elif extra['key'] == 'guid':
            dcat_dict['identifier'] = extra['value']

        elif extra['key'] == 'license_url':
            dcat_dict['license'] = extra['value']

    if not dcat_dict['publisher'].get('name') and package_dict.get('maintainer'):
        dcat_dict['publisher']['name'] = package_dict.get('maintainer')
        if package_dict.get('maintainer_email'):
            dcat_dict['publisher']['mbox'] = package_dict.get('maintainer_email')

    dcat_dict['distribution'] = []
    for resource in (package_dict.get('resources') or []):
        distribution = {
            'title': resource.get('name'),
            'description': resource.get('description'),
            'format': resource.get('format'),
            'byteSize': resource.get('size'),
            # TODO: downloadURL or accessURL depending on resource type?
            'accessURL': resource.get('url'),
        }
        dcat_dict['distribution'].append(distribution)

    return dcat_dict


def find_license_by_uri(license_uri):
    from ckan import model
    for license in model.Package.get_license_register().values():
        if license.url == license_uri:
            return license.id
    # special cases
    if license_uri == 'http://www.nationalarchives.gov.uk/doc/open-government-licence/':
        return 'uk-ogl'

def find_license_by_title(license_title):
    from ckan import model
    license_title_lower = license_title.lower()
    for license in model.Package.get_license_register().values():
        if license.title.lower() == license_title_lower:
            return license.id
