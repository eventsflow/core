
import pytest

from eventsflow.workers.settings import Settings
from eventsflow.workers.process import ProcessingWorker

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.events import Event
from eventsflow.events import EventStopProcessing

from eventsflow.queues.local.queues import EventsQueue
from eventsflow.queues.local.queues import LocalQueueEmpty


class SampleProcessingWorker(ProcessingWorker):
    def process(self, event):
        self.publish(event)
        return True


def test_process_worker_init():

    settings = Settings(**{
        'name': 'TestProcessingWorker',
        'type': 'eventsflow.workers.process.ProcessingWorker',
        'inputs': 'default',
    })

    worker = ProcessingWorker(settings)
    assert worker


def test_process_worker_run_not_implemented():

    settings = Settings(**{
        'name': 'TestProcessingWorker',
        'type': 'eventsflow.workers.process.ProcessingWorker',
    })

    worker = ProcessingWorker(settings)
    with pytest.raises(NotImplementedError):
        worker.process('event')


def test_process_worker_publish_event_to_unk_queue():

    settings = Settings(**{
        'name': 'TestProcessingWorker',
        'type': 'eventsflow.workers.process.ProcessingWorker',
    })

    worker = ProcessingWorker(settings)
    worker.publish('event', queue_name='target')
    

def test_process_worker_run():

    EVENTS = [
        {'name': 'EventTest#1', 'metadata': {}, 'payload': []},
    ]

    queues = QueuesRegistry()
    queues.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])

    workers = WorkersRegistry(queues=queues)
    workers.load([
        {   'name': 'TestWorker', 
            'type': 'test_process_worker.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': [
                {'name': 'default', 'refs': 'SourceQueue', 'events': EVENTS }
            ], 
            'outputs': [
                {'name': 'default', 'refs': 'TargetQueue', }
            ], 
        },
    ])

    for worker in workers.workers:
        worker.run()

    source_queue = queues.get('SourceQueue')
    assert source_queue
    assert source_queue.empty() is True

    target_queue = queues.get('TargetQueue')
    assert target_queue

    for event in EVENTS:
        assert event == target_queue.consume().to_dict()
        target_queue.commit()


def test_process_worker_stop_processing_by_event():

    queues = QueuesRegistry()
    queues.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])

    workers = WorkersRegistry(queues=queues)
    workers.load([
        {   'name': 'TestWorker', 
            'type': 'test_process_worker.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
        },
    ])

    queues.get('SourceQueue').publish(EventStopProcessing())

    for worker in workers.workers:
        assert worker.consume() == None

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
            'type': 'test_process_worker.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers)[0]
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
            'type': 'test_process_worker.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers)[0]
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
            'type': 'test_process_worker.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
            'outputs': 'TargetQueue', 
        },
    ])

    worker = list(workers.workers)[0]
    assert worker
    
    queues.get('SourceQueue').publish(test_event)
    assert worker.consume().to_dict() == test_event.to_dict()
    worker.commit()

    queues.get('SourceQueue').publish(test_event)
    assert worker.consume('default').to_dict() == test_event.to_dict()
    worker.commit('default')

    worker.commit('unknown')
