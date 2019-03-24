
import pytest

from eventsflow.events import Event

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.workers.process import ProcessingWorker


def test_workers_registry_init_no_queues_registry():

    with pytest.raises(TypeError):
        registry = WorkersRegistry()
        assert registry is not None


def test_workers_registry_init():

    registry = WorkersRegistry(queues=QueuesRegistry())
    assert registry is not None


def test_workers_registry_load_empty_workers_config():

    registry = WorkersRegistry(queues=QueuesRegistry())
    with pytest.raises(TypeError):
        registry.load([])


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
    assert [ type(w) for w in registry.workers] == [ ProcessingWorker, ] 


def test_workers_registry_load_workers_queues():
    ''' load workers to registry with queues
    '''

    QUEUES = [
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ]
    queues = QueuesRegistry()
    queues.load(QUEUES)

    WORKERS = [
        {   'name': 'TestWorker', 
            'type': 'eventsflow.workers.process.ProcessingWorker', 
            'inputs': 'SourceQueue', 
            'outputs':  'TargetQueue',
        },
    ]
    registry = WorkersRegistry(queues=queues)
    registry.load(WORKERS)

    assert [ type(w) for w in registry.workers] == [ ProcessingWorker, ] 

def test_workers_registry_load_workers_queues_with_events():
    ''' load workers to registry with queues and events
    '''

    QUEUES = [
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ]
    queues = QueuesRegistry()
    queues.load(QUEUES)

    EVENTS = [
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
    ]

    WORKERS = [
        {   'name': 'TestWorker', 
            'type': 'eventsflow.workers.process.ProcessingWorker', 
            'inputs': [
                {'name': 'default', 'refs': 'SourceQueue', 'events': EVENTS }
            ], 
            'outputs': [
                {'name': 'default', 'refs': 'TargetQueue', 'events': EVENTS }
            ], 
        },
    ]
    registry = WorkersRegistry(queues=queues)
    registry.load(WORKERS)

    assert [ type(w) for w in registry.workers] == [ ProcessingWorker, ] 
    assert queues.get('SourceQueue')
    assert queues.get('TargetQueue')

    # Source Queue
    events = []
    for _ in EVENTS:
        event = queues.get('SourceQueue').consume()
        events.append(event.to_dict())
    
    assert events == EVENTS

    # Target Queue
    events = []
    for _ in EVENTS:
        event = queues.get('TargetQueue').consume()
        events.append(event.to_dict())
    
    assert events == EVENTS
