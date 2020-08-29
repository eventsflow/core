
import pytest

from eventsflow.registries.queues import QueuesRegistry


def test_queues_registry_init():

    registry = QueuesRegistry()
    assert registry is not None


def test_queues_registry_load_empty_queues_config():

    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([])


def test_queues_registry_load_incorrect_queues_config():

    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load({'name': 'TestQueue#1', 'type': 'eventsflow.queues.local.EventsQueue', })


def test_queues_registry_load_queues_config_wo_queue_type():

    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestQueue#1', },
        ])


def test_queues_registry_load_queues_config_with_missed_queue_name():

    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            { 'type': 'eventsflow.queues.local.EventsQueue', },
        ])

def test_queues_registry_load_queues_config_with_unk_queue_type():

    registry = QueuesRegistry()
    with pytest.raises(TypeError):
        registry.load([
            {'name': 'TestQueue#1', 'type': 'eventsflow.queues.EventsQueue', },
        ])


def test_queues_registry_load():
    ''' load queues to registry
    '''
    from eventsflow.queues.local.queues import EventsQueue

    registry = QueuesRegistry()
    registry.load([
        {'name': 'TestQueue#1', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    assert registry is not None
    assert registry.get('TestQueue#1') is not None
    assert isinstance(registry.get('TestQueue#1'), EventsQueue)

    assert [ name for name in registry.queues ] == [ 'TestQueue#1', ]
