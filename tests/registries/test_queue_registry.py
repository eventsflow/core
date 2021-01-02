''' Tests for Queues Registry
'''
import pytest

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.queues.local.queues import EventsQueue


def test_queues_registry_init():
    ''' test for Queue Registry Initialization
    '''
    registry = QueuesRegistry()
    assert registry is not None

def test_queues_registry_load_empty_queues_config():
    ''' test for load empty queues
    '''
    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([])

def test_queues_registry_load_incorrect_queues_config():
    ''' test for load incorrect queues config
    '''
    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load({'name': 'TestQueue#1', 'type': 'eventsflow.queues.local.EventsQueue', })

def test_queues_registry_load_queues_config_wo_queue_type():
    ''' test for loading queues without queue type
    '''
    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestQueue#1', },
        ])

def test_queues_registry_load_queues_config_with_missed_queue_name():
    ''' test for loading queues with missed queue name
    '''
    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            { 'type': 'eventsflow.queues.local.EventsQueue', },
        ])

def test_queues_registry_load_queues_config_with_unk_queue_type():
    ''' tests for loading queue with unknown queue type
    '''
    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestQueue#1', 'type': 'eventsflow.queues.EventsQueue', },
        ])

def test_queues_registry_load():
    ''' load queues to registry
    '''
    registry = QueuesRegistry()
    registry.load([
        {'name': 'TestQueue#1', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    assert registry is not None
    assert registry.get('TestQueue#1') is not None
    assert isinstance(registry.get('TestQueue#1'), EventsQueue)

    assert [ name for name in registry.queues ] == [ 'TestQueue#1', ]
