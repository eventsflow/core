''' Tests for loading extra vars
'''
from eventsflow.utils import load_extra_vars


def test_load_empty_extra_vars():
    ''' test for loading empty extra vars
    '''
    assert load_extra_vars(None) == {}
    assert load_extra_vars([]) == {}
    assert load_extra_vars({}) == {}

def test_load_extra_vars_json_as_string():
    ''' test for loading extra vars as json from string
    '''
    assert load_extra_vars('[]') == {}
    assert load_extra_vars('{}') == {}

    assert load_extra_vars(['{ "k1": "v1", "k2": "v2"}',]) == {
        'k1': 'v1',
        'k2': 'v2'
    }

def test_load_extra_vars_missing_file():
    ''' test for loading exta vars from missing file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/missing.json',]) == {}

def test_load_extra_vars_empty_json_file():
    ''' test for loading extra vars from empty json file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/empty.json',]) == {}

def test_load_extra_vars_json_file():
    ''' test for loading extra vars from json file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/vars.json', ]) == {
        'k1': 'v1',
        'k2': 'v2'
    }

def test_load_extra_vars_corrupted_json_file():
    ''' test for loading extra vars from corrupted json file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/corrupted.json', ]) == {}

def test_load_extra_vars_empty_yaml_file():
    ''' test for loading extra vars from empty yaml file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/empty.yaml', ]) == {}

def test_load_extra_vars_yaml_file():
    ''' test for loading extra vars from yaml file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/vars.yaml', ]) == {
        'k1': 'v1',
        'k2': 'v2'
    }

def test_load_extra_vars_corrupted_yaml_file():
    ''' test for loading extra vars from corrupted yaml file
    '''
    assert load_extra_vars(['@tests/resources/extra_vars/corrupted.yaml', ]) == {}
