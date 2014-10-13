import os
import json
import difflib
import collections
from pprint import pprint

import nose.tools


def get_example_file_content(file_name):
    path = os.path.join(os.path.dirname(__file__),
                        '..', '..', '..', 'examples',
                        file_name)
    with open(path, 'r') as f:
        return f.read()


def get_example_file_as_dict(file_name):
    return json.loads(get_example_file_content(file_name))


def get_sample_file_content(file_name):
    path = os.path.join(os.path.dirname(__file__),
                        'samples', file_name)
    with open(path, 'r') as f:
        return f.read()


def get_sample_file_as_dict(file_name):
    return json.loads(get_sample_file_content(file_name))


def poor_mans_dict_diff(d1, d2):
    def _get_lines(d):
        return sorted([l.strip().rstrip(',')
                       for l in json.dumps(d, indent=0).split('\n')
                       if not l.startswith(('{', '}', '[', ']'))])

    d1_lines = _get_lines(d1)
    d2_lines = _get_lines(d2)

    return '\n' + '\n'.join([l for l in difflib.ndiff(d1_lines, d2_lines)
                             if l.startswith(('-', '+'))])


# assert_equal2 is an improved assert_equal that doesn't insist on printing
# strings that vary only
# when one is unicode and the other is a str string.
def drop_keys_with_blank_values(data):
    if isinstance(data, collections.Mapping):
        return dict(drop_keys_with_blank_values(item) for item in data.iteritems()
                    if item[1] is not None)
    elif isinstance(data, collections.Iterable) and \
            not isinstance(data, basestring):
        return type(data)(map(drop_keys_with_blank_values, data))
    else:
        return data

def drop_unicodeness(data):
    if isinstance(data, basestring):
        try:
            return str(data)
        except:
            # return unicode if it really is!
            return data
    elif isinstance(data, collections.Mapping):
        return dict(map(drop_unicodeness, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(drop_unicodeness, data))
    else:
        return data

def sort_lists(data):
    if isinstance(data, collections.Mapping):
        return dict(map(sort_lists, data.iteritems()))
    elif isinstance(data, collections.Iterable) and \
            not isinstance(data, basestring):
        iterable = type(data)(map(sort_lists, data))
        if isinstance(iterable, list):
            return sorted(iterable)
        return iterable
    else:
        return data
nose.tools.assert_equal.__self__.maxDiff = None
def assert_equal2(a, b, ignore_keys_with_blank_values=False, ignore_order=False):
    '''Ignores unicode string vs string'''
    if ignore_keys_with_blank_values:
        a, b = drop_keys_with_blank_values(a), drop_keys_with_blank_values(b)
    a, b = drop_unicodeness(a), drop_unicodeness(b)
    if ignore_order:
        a, b = sort_lists(a), sort_lists(b)
    if a != b:
        def print_json(data):
            try:
                print json.dumps(data, sort_keys=True, indent=2)
            except:
                pprint(data)
        print "A"
        print_json(a)
        print 'B'
        print_json(b)
        nose.tools.assert_equal(drop_unicodeness(a), drop_unicodeness(b))
