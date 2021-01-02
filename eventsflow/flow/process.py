''' Eventsflow Process Flow
'''
import time
import logging
from typing import List

from eventsflow.flow.parser import FlowParser
from eventsflow.flow.settings import Settings as FlowSettings

from eventsflow.events import EventStopProcessing

from eventsflow.registries.queues import QueuesRegistry
from eventsflow.registries.workers import WorkersRegistry


logger = logging.getLogger(__name__)


class Flow:
    ''' Processing Flow
    '''
    def __init__(self, path:str, extra_vars=None):
        ''' Initialize Processing Flow

        ### Parameters

        - path: the path to a flow configuration file
        - extra_vars: extra variables, optional. From command line `--vars` argument
        '''
        if not extra_vars:
            extra_vars = list()

        queues, workers = FlowParser(path).parse(extra_vars)

        self._queues = QueuesRegistry()
        self._queues.load(queues)

        self._workers = WorkersRegistry(queues=self._queues)
        self._workers.load(workers)

    @property
    def queues(self) -> List:
        ''' returns the list of queues
        '''
        return self._queues.queues

    @property
    def workers(self) -> List:
        ''' returns the list of workers
        '''
        return self._workers.workers

    def get_current_status(self, with_logging:bool=False):
        ''' returns the current workers status
        '''
        active_workers = list(self._workers.workers(status='active'))
        stopped_workers = list(self._workers.workers(status='inactive'))

        current_state = {
            'activeWorkers': active_workers,
            'activeWorkersByName': [p.name for p in active_workers],
            'inactiveWorkersByName': [p.name for p in stopped_workers],
            'queues': dict([
                (name, queue.size()) for name, queue in self._queues.queues.items()
            ]),
        }
        if with_logging:
            logger.info('Queues stats: %s', current_state.get('queues', []))
            logger.info('Active workers: %s', current_state.get('activeWorkersByName', []))
            logger.info('Stopped workers: %s', current_state.get('stoppedWorkersByName', []))

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
                for producer in current_status.get('activeWorkers', []):
                    for output_queue_instance in getattr(producer, 'outputs', dict()).values():
                        if output_queue_instance and output_queue_instance == queue_instance:
                            num_producers += 1

                num_consumers = 0
                for consumer in current_status.get('activeWorkers', []):
                    for input_queue_instance in getattr(consumer, 'inputs', dict()).values():
                        if input_queue_instance and input_queue_instance == queue_instance:
                            num_consumers += 1

                if queue_size == 0 and num_producers == 0:
                    for _ in range(num_consumers):
                        logger.info('Sending stop processing task to queue: %s', queue_name)
                        queue_instance.put(EventStopProcessing)

            time.sleep(FlowSettings.STATUS_CHECK_TIME_INTERVAL)

        logger.info('Processing completed')
