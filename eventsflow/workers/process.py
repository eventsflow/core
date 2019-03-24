
import logging
import multiprocessing as mp

from collections import Counter

from eventsflow.events import EventStopProcessing
from eventsflow.workers.settings import DEFAULT_EVENT_WAITING_TIMEOUT
from eventsflow.queues.local.queues import LocalQueueEmpty 


logger = logging.getLogger(__name__)


class ProcessingWorker(mp.Process):

    def __init__(self, settings):
        ''' Processing worker
        '''
        super(ProcessingWorker, self).__init__()

        self.name           = settings.name
        self.inputs         = settings.inputs
        self.outputs        = settings.outputs
        self.parameters     = settings.parameters
        self.timeout        = self.parameters.get('timeout', DEFAULT_EVENT_WAITING_TIMEOUT)

        self._metrics = Counter(dict())

    def open_worker(self):
        ''' run method before worker start
        '''
        logger.info('Worker: {}, status: started'.format(self.name))

    def close_worker(self):
        ''' run method before worker completed
        '''
        logger.info('Worker: {}, status: completed'.format(self.name))

    def run(self):
        ''' processing run
        '''
        self.open_worker()

        while True:
            # TODO support multiple queues
            event = self.consume()
            if event and self.process(event):
                self.commit()

            # if no active input queues, stop processing
            if len(self.inputs) == 0:
                break

        logger.info('Worker: {}, no input queues, status: stopping '.format(self.name))
        self.close_worker()

    def process(self, event):
        ''' process event

        the method shall return True if incoming event was processed succefuly 
        '''
        raise NotImplementedError('The method process does not implemented')

    def consume(self, queue_name='default'):
        ''' consume event from the queue
        '''
        queue = self.inputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('Worker: {}, unknown queue: {}'.format(self.name, queue_name))
            return None
            
        try:
            # consume event from queue    
            event = queue.consume(timeout=self.timeout)
        except LocalQueueEmpty as err:
            logger.warning('Worker: {}, queue: {}, timeout: {}, no events'.format(self.name, queue_name, self.timeout))
            # remove queue from inputs
            self.inputs.pop(queue_name)
            event = None

        if type(event) == EventStopProcessing:
            logger.info('Worker: {}, process completed for queue: {}'.format(self.name, queue_name))
            # commit event
            queue.commit()
            # remove queue from inputs
            self.inputs.pop(queue_name)
            event = None

        return event

    def publish(self, event, queue_name='default'):
        ''' send event to the queue
        '''
        queue = self.outputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('worker: {}, The attempt to send the event to unknow queue name: {}'.format(self.name, queue_name))
        else:
            queue.publish(event)

    def commit(self, queue_name='default'):
        ''' commit event as processed
        '''
        queue = self.inputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('worker: {}, The attempt to commit the event to unknow queue name: {}'.format(self.name, queue_name))
        else:
            queue.commit()
