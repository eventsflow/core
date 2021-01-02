''' Workers | Process Module
'''

import logging
import multiprocessing as mp

from collections import Counter

from eventsflow.events import Event
from eventsflow.events import EventDrop
from eventsflow.events import EventStopProcessing

from eventsflow.workers.settings import Settings
from eventsflow.workers.settings import DEFAULT_EVENT_WAITING_TIMEOUT

from eventsflow.queues.local.queues import LocalQueueEmpty


logger = logging.getLogger(__name__)


class ProcessingWorker(mp.Process):
    ''' Processing Worker
    '''
    def __init__(self, settings:Settings):
        ''' Processing worker

        ### Parameters
        - settings: worker settings
        '''
        super().__init__()

        self.name = settings.name
        self.inputs = settings.inputs
        self.outputs = settings.outputs
        self.parameters = settings.parameters
        self.timeout = self.parameters.get('timeout', DEFAULT_EVENT_WAITING_TIMEOUT)

        self._metrics = Counter(dict())

    def open_worker(self):
        ''' the method will be runned before worker start
        '''
        logger.info('Worker: %s, status: started', self.name)

    def close_worker(self):
        ''' the method will be runned before worker completed
        '''
        logger.info('Worker: %s, status: completed', self.name)

    def run(self):
        ''' run worker processing loop

        ### TODO
        - support multiple queues
        - add performance metrics:
            - waiting for event
            - processing event
            - total number of input events and output events
        '''
        self.open_worker()

        while True:
            incoming_event = self.consume()
            if incoming_event:
                for outgoing_event in self.process(incoming_event):
                    # skip events if they are marked as EventDrop
                    if isinstance(outgoing_event, EventDrop):
                        continue
                    # send events to outputs
                    outgoing_event.metadata['eventsflow.source'] = self.name
                    for output in outgoing_event.metadata.get('eventsflow.outputs', ['default']):
                        self.publish(outgoing_event, queue_name=output)
                self.commit()

            # if no active input queues, stop processing
            if len(self.inputs) == 0:
                break

        logger.info('Worker: %s, no input queues, status: stopping ', self.name)
        self.close_worker()

    def process(self, event:Event) -> Event:
        ''' the method shall yield events or EventDrop if the events need to drop
        '''
        raise NotImplementedError('The method process does not implemented')

    def consume(self, queue_name:str='default') -> Event:
        ''' consume event from the queue

        ### Parameters

        - queue_name: the input queue
        '''
        queue = self.inputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('Worker: %s, unknown queue: %s', self.name, queue_name)
            return None

        try:
            # consume event from queue
            event = queue.consume(timeout=self.timeout)
        except LocalQueueEmpty:
            logger.warning('Worker: %s, queue: %s, timeout: %s, no events',
                self.name, queue_name, self.timeout)
            # remove queue from inputs
            self.inputs.pop(queue_name)
            event = None

        if isinstance(event, EventStopProcessing):
            logger.info('Worker: %s, process completed for queue: %s',
                self.name, queue_name)
            # commit event
            queue.commit()
            # remove queue from inputs
            self.inputs.pop(queue_name)
            event = None

        return event

    def publish(self, event:Event, queue_name:str='default'):
        ''' send event to the queue
        '''
        queue = self.outputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('worker: %s, The attempt to send the event to unknow queue name: %s',
                self.name, queue_name)
        else:
            queue.publish(event)

    def commit(self, queue_name:str='default'):
        ''' commit event as processed in queue
        '''
        queue = self.inputs.get(queue_name, {}).get('refs', None)
        if not queue:
            logger.error('worker: %s, The attempt to commit the event to unknow queue name: %s',
                self.name, queue_name)
        else:
            queue.commit()
