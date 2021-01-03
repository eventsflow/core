''' Tests | Workers | Process
'''
import pytest

from eventsflow.events import Event

from eventsflow.queues.local import EventsQueue
from eventsflow.queues.local import QueueEmpty

from tests.common_libs import create_queues_registry
from tests.common_libs import create_workers_registry
from tests.common_libs import create_test_processing_worker


def test_process_worker_publish_event_to_unk_queue():
    ''' test when processing worker publishes an event to unknown queue
    '''
    worker = create_test_processing_worker()
    worker.publish('event', queue_name='target')


def test_process_worker_queue_action_consume():
    ''' test for processing worker consumes events
    '''
    test_event = Event(name='TestEvent#1')

    queues = create_queues_registry()
    workers = create_workers_registry(queues)

    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    worker = list(workers.workers())[0]
    assert worker

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume().to_dict() == test_event.to_dict()

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume('default').to_dict() == test_event.to_dict()

    assert worker.consume('unknown') is None

def test_process_worker_queue_action_publish():
    ''' test for processing worker publishes an event
    '''
    test_event = Event(name='TestEvent#1')

    queues = create_queues_registry()
    workers = create_workers_registry(queues)

    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    worker = list(workers.workers())[0]
    assert worker

    worker.publish(test_event)
    assert queues.get('TargetQueue').consume().to_dict() == test_event.to_dict()

    worker.publish(test_event, queue_name='default')
    assert queues.get('TargetQueue').consume().to_dict() == test_event.to_dict()

    worker.publish(test_event, queue_name='unknown')
    with pytest.raises(QueueEmpty):
        assert queues.get('TargetQueue').consume(timeout=1)

def test_process_worker_queue_action_commit():
    ''' test for processing worker commits an event
    '''
    test_event = Event(name='TestEvent#1')

    queues = create_queues_registry()
    workers = create_workers_registry(queues)

    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    worker = list(workers.workers())[0]
    assert worker

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume().to_dict() == test_event.to_dict()
    worker.commit()

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume('default').to_dict() == test_event.to_dict()
    worker.commit('default')

    worker.commit('unknown')
