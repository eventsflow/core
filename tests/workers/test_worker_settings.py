''' Tests for Worker Settings
'''
import pytest

from eventsflow.workers.settings import Settings

def test_worker_settings_no_name():
    ''' test for worker settings without worker name
    '''
    with pytest.raises(TypeError):
        Settings(**{
            'name': None,
        })

def test_worker_settings_no_type():
    ''' test for worker settings without worker type
    '''
    with pytest.raises(TypeError):
        Settings(**{
            'name': 'TestWorker',
            'type': None,
        })

def test_worker_settings_incorrect_parameters():
    ''' test for worker settings with incorrect parameters
    '''
    with pytest.raises(TypeError):
        Settings(**{
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker',
            'parameters': [],
        })

def test_worker_settings_no_parameters():
    ''' test for worker settings without paramters
    '''
    with pytest.raises(TypeError):
        Settings(**{
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker',
            'parameters': { 'batch_size': 100 },
            'instances': 0,
        })

def test_worker_settings_none_queue():
    ''' test for worker settings without queue
    '''
    settings = Settings(**{
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker',
            'parameters': { 'batch_size': 100 },
            'instances': 1,
            'inputs': None, })
    assert settings.inputs ==  { 'default': { 'refs': None, 'events': [], }, }

def test_worker_settings_incorrect_queue_defs():
    ''' test for worker settings with incorrect queue definition
    '''
    with pytest.raises(TypeError):
        Settings(**{
                'name': 'TestWorker',
                'type': 'eventsflow.workers.DummyWorker',
                'parameters': { 'batch_size': 100 },
                'instances': 1,
                'inputs': ['input-queue', ], })

def test_worker_settings_simple():
    ''' tests for simple worker settings
    '''
    worker_config = {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'description': 'Test worker',
        'instances': 1,
        'parameters': {
            'param1': 'values1',
            'param2': 'values2',
        },
        'inputs': 'input-queue',
        'outputs': 'output-queue',
    }
    settings = Settings(**worker_config)
    assert settings.name == 'TestWorker'
    assert settings.type == 'eventsflow.workers.DummyWorker'
    assert settings.instances == 1
    assert settings.parameters == {
                        'param1': 'values1',
                        'param2': 'values2', }
    assert settings.inputs ==  { 'default': { 'refs': 'input-queue', 'events': [], }, }
    assert settings.outputs == { 'default': { 'refs': 'output-queue', 'events': [], }, }

def test_worker_settings_simple_as_dict():
    ''' test for simple worker settings specified as dict
    '''
    worker_config = {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'description': 'Test worker',
        'instances': 1,
        'parameters': {
            'param1': 'values1',
            'param2': 'values2',
        },
        'inputs': 'input-queue',
        'outputs': 'output-queue',
    }
    settings = Settings(**worker_config)
    assert vars(settings) == {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'instances': 1,
        'parameters': { 'param1': 'values1', 'param2': 'values2', },
        'inputs':     { 'default': { 'refs': 'input-queue', 'events': [], }, },
        'outputs':    { 'default': { 'refs': 'output-queue', 'events': [], }, },
    }

def test_worker_settings_simple_list_of_queues():
    ''' test for simple worker settings with a list of queues
    '''
    worker_config = {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'description': 'Test worker',
        'instances': 1,
        'parameters': {
            'param1': 'values1',
            'param2': 'values2',
        },
        'inputs': [
            { 'name': 'default', 'refs': 'input-queue', 'events': [], },
        ],
        'outputs': [
            { 'name': 'default', 'refs': 'output-queue', },
            { 'name': 'monitoring', 'refs': 'monitoring-queue', },
        ],
    }
    settings = Settings(**worker_config)
    assert vars(settings) == {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'instances': 1,
        'parameters': { 'param1': 'values1', 'param2': 'values2', },
        'inputs':    { 'default': { 'refs': 'input-queue', 'events': [], }, },
        'outputs':   {
            'default': { 'refs': 'output-queue', 'events': [], },
            'monitoring': { 'refs': 'monitoring-queue', 'events': [], },
        },
    }

# ==========================================================
#
#  parse_queues() tests
#

def test_worker_settings_parse_queues_as_default():
    ''' test for worker settings with parsing defaults
    '''
    assert Settings.parse_gueues(None) == {
        'default': {'events': [], 'refs': None}
    }

    assert Settings.parse_gueues('source-queue') == {
        'default': {'events': [], 'refs': 'source-queue' }
    }

def test_worker_settings_parse_queues_as_list():
    ''' test for parsing queues specified as list
    '''
    assert Settings.parse_gueues([{'name': 'default'}]) == {
        'default': {'events': [], 'refs': None }
    }

    assert Settings.parse_gueues([{'name': 'default', 'refs': 'source' }, ]) == {
        'default': {'events': [], 'refs': 'source' }
    }

def test_worker_settings_parse_queues_errors_handline():
    ''' test for parsing queues and errors handling
    '''
    assert Settings.parse_gueues({ 'name': 'default' }) == {}

    with pytest.raises(TypeError):
        Settings.parse_gueues([ 'default', ])
