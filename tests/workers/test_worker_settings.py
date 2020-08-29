
import pytest

from eventsflow.workers.settings import Settings


def test_worker_settings_no_name():

    with pytest.raises(TypeError):
        settings = Settings(**{ 
            'name': None, 
        })


def test_worker_settings_no_type():

    with pytest.raises(TypeError):
        settings = Settings(**{ 
            'name': 'TestWorker',
            'type': None, 
        })


def test_worker_settings_incorrect_parameters():

    with pytest.raises(TypeError):
        settings = Settings(**{ 
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker', 
            'parameters': [],
        })


def test_worker_settings_no_parameters():

    with pytest.raises(TypeError):
        settings = Settings(**{ 
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker', 
            'parameters': { 'batch_size': 100 },
            'instances': 0,
        })

def test_worker_settings_none_queue():

    settings = Settings(**{ 
            'name': 'TestWorker',
            'type': 'eventsflow.workers.DummyWorker', 
            'parameters': { 'batch_size': 100 },
            'instances': 1,
            'inputs': None, })
    assert settings.inputs ==  { 'default': { 'refs': None, 'events': [], }, }


def test_worker_settings_incorrect_queue_defs():

    with pytest.raises(TypeError):
        settings = Settings(**{ 
                'name': 'TestWorker',
                'type': 'eventsflow.workers.DummyWorker', 
                'parameters': { 'batch_size': 100 },
                'instances': 1,
                'inputs': ['input-queue', ], })

def test_worker_settings_simple():

    WORKER_CONFIG = {
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
    settings = Settings(**WORKER_CONFIG)
    assert settings.name == 'TestWorker'
    assert settings.type == 'eventsflow.workers.DummyWorker'
    assert settings.instances == 1
    assert settings.parameters == {
                        'param1': 'values1',
                        'param2': 'values2', }
    assert settings.inputs ==  { 'default': { 'refs': 'input-queue', 'events': [], }, }
    assert settings.outputs == { 'default': { 'refs': 'output-queue', 'events': [], }, }
    

def test_worker_settings_simple_as_dict():

    WORKER_CONFIG = {
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
    settings = Settings(**WORKER_CONFIG)
    assert vars(settings) == {
        'name': 'TestWorker',
        'type': 'eventsflow.workers.DummyWorker',
        'instances': 1,
        'parameters': { 'param1': 'values1', 'param2': 'values2', },
        'inputs':     { 'default': { 'refs': 'input-queue', 'events': [], }, },
        'outputs':    { 'default': { 'refs': 'output-queue', 'events': [], }, },
    }

def test_worker_settings_simple_list_of_queues():

    WORKER_CONFIG = {
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
    settings = Settings(**WORKER_CONFIG)
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

    assert Settings.parse_gueues(None) == {
        'default': {'events': [], 'refs': None}
    }
    
    assert Settings.parse_gueues('source-queue') == {
        'default': {'events': [], 'refs': 'source-queue' }
    }
    
def test_worker_settings_parse_queues_as_list():

    assert Settings.parse_gueues([{'name': 'default'}]) == {
        'default': {'events': [], 'refs': None }
    }

    assert Settings.parse_gueues([{'name': 'default', 'refs': 'source' }, ]) == {
        'default': {'events': [], 'refs': 'source' }
    }

def test_worker_settings_parse_queues_errors_handline():

    assert Settings.parse_gueues({ 'name': 'default' }) == {}

    with pytest.raises(TypeError):
        Settings.parse_gueues([ 'default', ])
