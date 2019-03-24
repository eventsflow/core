
import os
import pytest

from eventsflow.utils import load_extra_vars
from eventsflow.utils import parse_with_extra_vars
from eventsflow.utils import split_worker_uri

# ========================================================= 
# Loading extra variables
# 

def test_load_empty_extra_vars():

    assert load_extra_vars(None) == {}
    assert load_extra_vars([]) == {}
    assert load_extra_vars({}) == {}


def test_load_extra_vars_json_as_string():
    
    assert load_extra_vars('[]') == {}
    assert load_extra_vars('{}') == {}

    assert load_extra_vars(['{ "k1": "v1", "k2": "v2"}',]) == { 
        'k1': 'v1', 
        'k2': 'v2' 
    }


def test_load_extra_vars_missing_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/missing.json',]) == {}


def test_load_extra_vars_empty_json_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/empty.json',]) == {}


def test_load_extra_vars_json_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/vars.json', ]) == { 
        'k1': 'v1', 
        'k2': 'v2' 
    }


def test_load_extra_vars_corrupted_json_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/corrupted.json', ]) == {}


def test_load_extra_vars_empty_yaml_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/empty.yaml', ]) == {}


def test_load_extra_vars_yaml_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/vars.yaml', ]) == { 
        'k1': 'v1', 
        'k2': 'v2' 
    }


def test_load_extra_vars_corrupted_yaml_file():
    
    assert load_extra_vars(['@test/resources/extra_vars/corrupted.yaml', ]) == {}


# ========================================================= 
# Parsing extra variables
# 

def test_flow_simple_template():

    source = 'test/resources/flows/simple-template.yaml'
    target = 'test/resources/flows/simple-template-result.yaml'
            
    assert parse_with_extra_vars(
                source, {'filename': 'test.yaml'}
            ).strip() == open(target).read().strip()


def test_flow_unspecified_placeholder():

    source = 'test/resources/flows/unspecified-placeholder.yaml'
            
    assert parse_with_extra_vars(
                source, {'filename': 'test.yaml'}
            ) == None

# ========================================================= 
# Worker settings
# 

def test_split_worker_uri():

    assert split_worker_uri('') == (None, None)
    assert split_worker_uri(None) == (None, None)

    assert split_worker_uri('eventsflow.workers.process.ProcessingWorker') == ('eventsflow.workers.process', 'ProcessingWorker')

