''' Common tools and classes for tests
'''
import logging

from eventsflow.workers.process import ProcessingWorker

logger = logging.getLogger(__name__)

class SampleProcessingWorker(ProcessingWorker):
    ''' Sample Processing Worker, used for tests
    '''
    def process(self, event):
        yield event
