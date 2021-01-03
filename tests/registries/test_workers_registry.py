''' Tests for Worker Registry
'''
import pytest

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.workers.process import ProcessingWorker


def test_workers_registry_init():
    ''' test for worker registry initialization
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    assert registry is not None

def test_workers_registry_load_empty_workers_config():
    ''' test load empty workers config
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    registry.load([])
    assert list(registry.workers(status='all')) == list()

def test_workers_registry_load_incorrect_workers_config():
    ''' incorrect workers config, shall be the list of workers, passed as dict
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    with pytest.raises(TypeError):
        registry.load({'name': 'TestWorker', })


def test_workers_registry_load_workers_config_wo_worker_type():
    ''' incorrect workers config, missed worker type
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestWorker', },
        ])

def test_workers_registry_load_workers_config_incorrect_worker_type():
    ''' incorrect worker type
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())

    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestWorker', 'type': 'eventsflow.workers.ProcessingWorker', },
        ])

    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestWorker', 'type': 'eventsflow.workers.v2.ProcessingWorker', },
        ])


def test_workers_registry_load_workers_config():
    ''' correct load of workers config to registry
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    registry.load([
        {'name': 'TestWorker', 'type': 'eventsflow.workers.process.ProcessingWorker', },
    ])
    assert [ type(w) for w in registry.workers()] == [ ProcessingWorker, ]


def test_workers_registry_load_workers_queues():
    ''' load workers to registry with queues
    '''

    queues_config = [
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ]
    queues = QueuesRegistry()
    queues.load(queues_config)

    workers_config = [
        {   'name': 'TestWorker',
            'type': 'eventsflow.workers.process.ProcessingWorker',
            'inputs': 'SourceQueue',
            'outputs':  'TargetQueue',
        },
    ]
    registry = WorkersRegistry(queues=queues)
    registry.load(workers_config)

    assert [ type(w) for w in registry.workers()] == [ ProcessingWorker, ]


def test_workers_registry_load_workers_queues_with_events():
    ''' load workers to registry with queues and events
    '''

    queues_config = [
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ]
    queues = QueuesRegistry()
    queues.load(queues_config)

    test_events = [
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
    ]

    workers_config = [
        {   'name': 'TestWorker',
            'type': 'eventsflow.workers.process.ProcessingWorker',
            'inputs': [
                {'name': 'default', 'refs': 'SourceQueue', 'events': test_events }
            ],
            'outputs': [
                {'name': 'default', 'refs': 'TargetQueue', 'events': test_events }
            ],
        },
    ]
    registry = WorkersRegistry(queues=queues)
    registry.load(workers_config)

    assert [ type(w) for w in registry.workers()] == [ ProcessingWorker, ]
    assert queues.get('SourceQueue')
    assert queues.get('TargetQueue')

    # Source Queue
    events = []
    for _ in test_events:
        event = queues.get('SourceQueue').consume()
        events.append(event.to_dict())

    assert events == test_events

    # Target Queue
    events = []
    for _ in test_events:
        event = queues.get('TargetQueue').consume()
        events.append(event.to_dict())

    assert events == test_events
