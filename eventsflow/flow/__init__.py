
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

        self._workers   = WorkersRegistry()
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

    def get_current_status(self):
        ''' returns the current status
        '''
        active_workers = self._workers.workers(status='active')
        stopped_workers = self._workers.workers(status='stopped')
        return {
            'activeWorkers':            active_workers,
            'activeWorkersByName':      [p.name for p in active_workers],
            'stoppedWorkersByName':     [p.name for p in stopped_workers],
            'queues':                   dict([(n, q.qsize()) for n, q in self._queues.items()]),
        }

    def run(self):
        ''' run flow
        '''
        # start all workers in the registry
        self._workers.start()

        while True:
            
            current_status = self.get_current_status()

            logger.info('Queues stats: {}'.format(current_status.get('queues', [] )))
            logger.info('Active workers: {}'.format(current_status.get('activeWorkersByName', [] )))
            logger.info('Stopped workers: {}'.format(current_status.get('stoppedWorkersByName', [] )))

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
                    # logger.info('Queue name: {}, queue size: {}, num of producers: {}, num of consumers: {}'.format(
                    #     queue_name, queue_size, num_producers, num_consumers
                    # ))

                    for _ in range(num_consumers):
                        logger.info('Sending stop processing task to queue: {}'.format(queue_name))
                        queue_instance.put(EventStopProcessing)

            time.sleep(FlowSettings.STATUS_CHECK_TIME_INTERVAL)

        logger.info('Processing completed')
