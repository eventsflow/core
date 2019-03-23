
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
    assert settings.inputs == [ { 'name': 'default', 'refs': None, 'events': [], }, ]


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
    assert settings.inputs == [ { 'name': 'default', 'refs': 'input-queue', 'events': [], }, ]
    assert settings.outputs == [ { 'name': 'default', 'refs': 'output-queue', 'events': [], }, ]
    

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
        'inputs':    [ { 'name': 'default', 'refs': 'input-queue', 'events': [], }, ],
        'outputs':   [ { 'name': 'default', 'refs': 'output-queue', 'events': [], }, ],
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
        'inputs':    [ { 'name': 'default', 'refs': 'input-queue', 'events': [], }, ],
        'outputs':   [ 
            { 'name': 'default', 'refs': 'output-queue', 'events': [], }, 
            { 'name': 'monitoring', 'refs': 'monitoring-queue', 'events': [], }, 
        ],
    }
