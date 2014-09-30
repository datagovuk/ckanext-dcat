import rdflib
from rdflib.namespace import RDF, RDFS

DCAT = rdflib.Namespace('http://www.w3.org/ns/dcat#')
REG = rdflib.Namespace('http://purl.org/linked-data/registry#')
SKOS = rdflib.Namespace('http://www.w3.org/2004/02/skos/core#')
OWL = rdflib.Namespace('http://www.w3.org/2002/07/owl#')
LDP = rdflib.Namespace('http://www.w3.org/ns/ldp#')
DCT = rdflib.Namespace('http://purl.org/dc/terms/')
DC = rdflib.Namespace('http://purl.org/dc/elements/1.1/')
DATASET = rdflib.Namespace('http://publishmydata.com/def/dataset#')
FOAF = rdflib.Namespace('http://xmlns.com/foaf/0.1/')
VOID = rdflib.Namespace('http://rdfs.org/ns/void#')


class ParseError(Exception):
    pass


class DCATDataset(object):
    def __init__(self, dcat_rdf_string):
        self.dcat_rdf_string = dcat_rdf_string
        self.graph = rdflib.Graph()
        self.graph.parse(data=dcat_rdf_string)
        # format: is determined from source. Can be: rdf/xml, turtle, n3, nt, trix, rdfa

    def read_values(self):
        datasets = list(self.graph.subjects(RDF.type, DCAT.Dataset))
        if not datasets:
            raise ParseError('No dataset found')
        if len(datasets) > 1:
            raise ParseError('Multiple datasets found - expected 1')
        uri = datasets[0]
        dataset_resource = self.graph.resource(uri)
        dcat_dict = self.dataset_to_dict(dataset_resource, uri)
        return dcat_dict

    @staticmethod
    def dataset_to_dict(dataset_resource, uri):
        rdf_dataset = add_rdf_resource_operators(dataset_resource)

        d = dcat_dict = {}
        d['title'] = str_(rdf_dataset.first(RDFS.label) or \
                          rdf_dataset.first(DCT.title))
        d['description'] = '\n\n'.join(rdf_dataset.all(RDFS.comment) +
                                       rdf_dataset.all(DCT.description)) \
                           or None
        d['uri'] = str_(uri)
        contactEmail = rdf_dataset.first(DATASET.contactEmail)
        d['contactEmail'] = str_(contactEmail.identifier).replace('mailto:', '') \
                            if contactEmail else None
        d['issued'] = str_(rdf_dataset.first(DCT.issued))
        d['modified'] = str_(rdf_dataset.first(DCT.modified))
        d['license'] = str_(uri_(rdf_dataset.first(DCT.license)))
        #license_resource = rdf_dataset.first(DCT.license)
        #d['license'] = str_(license_resource.identifier) \
        #               if license_resource else None
        publisher = {}
        rdf_publisher = rdf_dataset.first(DCT.publisher)
        add_rdf_resource_operators(rdf_publisher)
        publisher['uri'] = str_(uri_(rdf_publisher))
        publisher['mbox'] = str_(rdf_publisher.first(FOAF.mbox))
        publisher['name'] = str_(rdf_publisher.first(FOAF.name))
        d['publisher'] = publisher or None
        d['references'] = [str_(uri_(ref))
                           for ref in rdf_dataset.all(DCT.references)] or None
        d['sparqlEndpoint'] = str_(uri_(rdf_dataset.first(VOID.sparqlEndpoint)))
        d['spatial'] = rdf_dataset.first(DCT.spatial)
        d['subject'] = deduplicate([
            str_(uri_(subject))
            for subject in (rdf_dataset.all(DCT.subject) +
                            rdf_dataset.all(DCAT.theme))]) or None
        d['language'] = rdf_dataset.all(DC.language) or None
        d['keyword'] = rdf_dataset.all(DCAT.keyword) or None
        d['identifier'] = rdf_dataset.first(DCT.identifier)
        d['distribution'] = []
        for rdf_distribution in rdf_dataset[DCAT.distribution]:
            dist = {}
            add_rdf_resource_operators(rdf_distribution)
            dist['accessURL'] = rdf_distribution.first(DCAT.accessURL)
            dist['title'] = rdf_distribution.first(DCT.title)
            dist['description'] = rdf_distribution.first(DCT.description)
            dist['format'] = rdf_distribution.first(DCAT.mediaType)
            if dist:
                d['distribution'].append(dist)
        return dcat_dict

def add_rdf_resource_operators(rdf_resource):
    '''Given an rdflib.Resource, monkey-patch in some operators to make it
    convenient to return values of different cardinalities. The operators
    `first` and `all` are named like sqlalchemy-query operators.'''
    def first(predicate):
        try:
            return rdf_resource[predicate].next()
        except StopIteration:
            return None
    rdf_resource.first = first
    def all(predicate):
        return [obj for obj in rdf_resource[predicate]]
    rdf_resource.all = all
    return rdf_resource

def str_(rdf_literal):
    if rdf_literal is None:
        return None
    return str(rdf_literal)

def uri_(resource):
    if resource:
        return resource.identifier

def deduplicate(list_):
    return list(set(list_))