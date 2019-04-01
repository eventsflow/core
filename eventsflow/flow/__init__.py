
import os
import time
import logging
import importlib


from eventsflow.flow.parser import FlowParser
from eventsflow.flow.settings import Settings as FlowSettings

from eventsflow.events import EventStopProcessing

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry


logger = logging.getLogger(__name__)


class Flow(object):
    ''' Events Processing Flow
    '''
    def __init__(self, path):

        queues_conf, workers_conf = FlowParser(path).parse()

        self._queues    = QueuesRegistry()
        self._queues.load(queues_conf)

        self._workers   = WorkersRegistry(queues=self._queues)
        self._workers.load(workers_conf)

    @property
    def queues(self):
        ''' returns the list of queues
        '''
        return self._queues.queues

    @property
    def workers(self):
        ''' returns the list of workers
        '''
        return self._workers.workers

    def get_current_status(self, with_logging=False):
        ''' returns the current workers status
        '''
        active_workers = list(self._workers.workers(status='active'))
        stopped_workers = list(self._workers.workers(status='inactive'))

        current_state = {
            'activeWorkers':            active_workers,
            'activeWorkersByName':      [p.name for p in active_workers],
            'inactiveWorkersByName':    [p.name for p in stopped_workers],
            'queues':                   dict([(name, queue.size()) for name, queue in self._queues.queues.items()]),
        } 
        if with_logging:
            logger.info('Queues stats: {}'.format(current_state.get('queues', [] )))
            logger.info('Active workers: {}'.format(current_state.get('activeWorkersByName', [] )))
            logger.info('Stopped workers: {}'.format(current_state.get('stoppedWorkersByName', [] )))

        return current_state 

    def run(self):
        ''' run flow
        '''
        # start all workers in the registry
        self._workers.start()

        while True:
            
            current_status = self.get_current_status(with_logging=True)

            if not current_status.get('activeWorkers', []):
                break

            for queue_name, queue_instance in self._queues.queues.items():
                queue_size = queue_instance.size()
                
                num_producers = 0
                for producer in active_workers:
                    for output_queue_instance in getattr(producer, 'outputs', dict()).values():
                        if output_queue_instance and output_queue_instance == queue_instance:
                            num_producers += 1
                    
                num_consumers = 0
                for consumer in active_workers:
                    for input_queue_instance in getattr(consumer, 'inputs', dict()).values():
                        if input_queue_instance and input_queue_instance == queue_instance:
                            num_consumers += 1

                if queue_size == 0 and num_producers == 0:
                    for _ in range(num_consumers):
                        logger.info('Sending stop processing task to queue: {}'.format(queue_name))
                        queue_instance.put(EventStopProcessing)

            time.sleep(FlowSettings.STATUS_CHECK_TIME_INTERVAL)

        logger.info('Processing completed')
