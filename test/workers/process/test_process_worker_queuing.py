
import pytest

from eventsflow.events import Event

from eventsflow.workers.settings import Settings
from eventsflow.workers.process import ProcessingWorker

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.queues.local.queues import EventsQueue
from eventsflow.queues.local.queues import LocalQueueEmpty

from common import SampleProcessingWorker


def test_process_worker_publish_event_to_unk_queue():

    settings = Settings(**{
        'name': 'TestProcessingWorker',
        'type': 'eventsflow.workers.process.ProcessingWorker',
    })

    worker = ProcessingWorker(settings)
    worker.publish('event', queue_name='target')


def test_process_worker_queue_action_consume():
    ''' worker queue's actions

    - consume
    '''
    test_event = Event(name='TestEvent#1')

    queues = QueuesRegistry()
    queues.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    workers = WorkersRegistry(queues=queues)
    workers.load([
        {   'name': 'TestWorker', 
            'type': 'common.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers())[0]
    assert worker
    
    queues.get('SourceQueue').publish(test_event)
    assert worker.consume().to_dict() == test_event.to_dict()

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume('default').to_dict() == test_event.to_dict()

    assert worker.consume('unknown') == None

def test_process_worker_queue_action_publish():
    ''' worker queue's actions

    - publish
    '''
    test_event = Event(name='TestEvent#1')

    queues = QueuesRegistry()
    queues.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    workers = WorkersRegistry(queues=queues)
    workers.load([
        {   'name': 'TestWorker', 
            'type': 'common.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers())[0]
    assert worker
    
    worker.publish(test_event)
    assert queues.get('TargetQueue').consume().to_dict() == test_event.to_dict()

    worker.publish(test_event, queue_name='default')
    assert queues.get('TargetQueue').consume().to_dict() == test_event.to_dict()

    worker.publish(test_event, queue_name='unknown')
    with pytest.raises(LocalQueueEmpty):
        assert queues.get('TargetQueue').consume(timeout=1)


def test_process_worker_queue_action_commit():
    ''' worker queue's actions:

    - commit
    '''
    test_event = Event(name='TestEvent#1')

    queues = QueuesRegistry()
    queues.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    assert isinstance(queues.get('SourceQueue'), EventsQueue)
    assert isinstance(queues.get('TargetQueue'), EventsQueue)

    workers = WorkersRegistry(queues=queues)
    workers.load([
        {   'name': 'TestWorker', 
            'type': 'common.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers())[0]
    assert worker
    
    queues.get('SourceQueue').publish(test_event)
    assert worker.consume().to_dict() == test_event.to_dict()
    worker.commit()

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume('default').to_dict() == test_event.to_dict()
    worker.commit('default')

    worker.commit('unknown')
