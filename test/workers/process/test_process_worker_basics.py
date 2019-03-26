
import pytest

from eventsflow.workers.settings import Settings
from eventsflow.workers.process import ProcessingWorker

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

from eventsflow.events import Event
from eventsflow.events import EventStopProcessing

from eventsflow.queues.local.queues import EventsQueue


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
            'type': 'common.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': [
                {'name': 'default', 'refs': 'SourceQueue', 'events': EVENTS }
            ], 
            'outputs': [
                {'name': 'default', 'refs': 'TargetQueue', }
            ], 
        },
    ])

    for worker in workers.workers():
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
            'type': 'common.SampleProcessingWorker', 
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue', 
        },
    ])

    queues.get('SourceQueue').publish(EventStopProcessing())

    for worker in workers.workers():
        assert worker.consume() == None


