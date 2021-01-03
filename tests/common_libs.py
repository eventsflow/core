''' Common tools and classes for tests
'''
import logging

from eventsflow.workers.settings import Settings
from eventsflow.workers.process import ProcessingWorker
from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry

logger = logging.getLogger(__name__)


class SampleProcessingWorker(ProcessingWorker):
    ''' Sample Processing Worker, used for tests
    '''
    def process(self, event):
        yield event


def create_queues_registry() -> QueuesRegistry:
    ''' return queues registry with source and target queues
    '''
    registry = QueuesRegistry()
    registry.load([
        {'name': 'SourceQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
        {'name': 'TargetQueue', 'type': 'eventsflow.queues.local.EventsQueue', },
    ])
    return registry

def create_workers_registry(queues:QueuesRegistry) -> WorkersRegistry:
    ''' return workers registry
    '''
    registry = WorkersRegistry(queues=queues)
    registry.load([
        {   'name': 'TestWorker',
            'type': 'common_libs.SampleProcessingWorker',
            'parameters': { 'timeout': 1, },
            'inputs': 'SourceQueue',
            'outputs': 'TargetQueue',
        },
    ])
    return registry

def create_test_processing_worker():
    ''' return Test Processing Worker instance
    '''
    return ProcessingWorker(Settings(**{
        'name': 'TestProcessingWorker',
        'type': 'eventsflow.workers.process.ProcessingWorker',
    }))
