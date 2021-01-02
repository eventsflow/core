''' Tests for Workers Registry
'''
import time
import pytest

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.events import EventStopProcessing

from tests.test_common import SampleProcessingWorker


def test_workers_registry_flow_worker_status():
    ''' check workers status
    '''
    registry = WorkersRegistry(queues=QueuesRegistry())
    registry.load([
        {
            'name': 'TestWorker',
            'type': 'tests.test_common.SampleProcessingWorker',
        },
    ])

    assert [ type(w) for w in registry.workers(status='active')] == []
    assert [ type(w) for w in registry.workers()] == [ SampleProcessingWorker, ]
    assert [ type(w) for w in registry.workers(status='all')] == [ SampleProcessingWorker, ]
    assert [ type(w) for w in registry.workers(status='inactive')] == [ SampleProcessingWorker, ]

    with pytest.raises(TypeError):
        assert [ type(w) for w in registry.workers(status='unknown')] == []

def test_workers_registry_flow_start_workers_and_check_status():
    ''' run workers and check workers status
    '''
    queues_registry = QueuesRegistry()
    queues_registry.load([
        {
            'name': 'records',
            'type': 'eventsflow.queues.local.EventsQueue',
        }
    ])

    workers_registry = WorkersRegistry(queues=queues_registry)
    workers_registry.load([
        {
            'name': 'TestWorker',
            'type': 'tests.test_common.SampleProcessingWorker',
            'inputs': 'records'
        },
    ])

    assert [ type(w) for w in workers_registry.workers(status='active')] == [ ]

    workers_registry.start()
    assert [
        type(w) for w in workers_registry.workers(status='active')
    ] == [ SampleProcessingWorker, ]

    queues_registry.get('records').publish(EventStopProcessing())
    time.sleep(1)

    assert [
        type(w) for w in workers_registry.workers(status='active')
    ] == []

    assert [
        type(w) for w in workers_registry.workers(status='inactive')
    ] == [ SampleProcessingWorker, ]
