''' Workers Registry Module
'''
import copy
import logging

from eventsflow.events import Event
from eventsflow.registries.queues import QueuesRegistry
from eventsflow.workers.settings import Settings as WorkerSettings

from eventsflow.utils import split_worker_uri


logger = logging.getLogger(__name__)


class WorkersRegistry:
    ''' Workers Registry
    '''
    def __init__(self, queues:QueuesRegistry):
        ''' Initialize Workers Registry
        '''
        if not isinstance(queues, QueuesRegistry):
            raise TypeError('No queues registry founded, {}'.format(queues))

        self._queues_registry = queues
        self._workers_registry = list()

    def workers(self, status:str='all'):
        ''' iterate by workers with their statuses

        ### Parameters
        - status: possible statuses:
            - all
            - active
            - inactive
        '''
        if status not in ('all', 'active', 'inactive'):
            raise TypeError('The status shall as `all`, `active` or `inactive`')

        for worker in self._workers_registry:
            if status == 'all':
                yield worker
            elif status == 'active' and worker.is_alive():
                yield worker
            elif status == 'inactive' and not worker.is_alive():
                yield worker

    def start(self):
        ''' start workers in the registry
        '''
        for worker in self._workers_registry:
            worker.start()

    def load(self, workers:list):
        ''' load workers to registry
        '''
        if not isinstance(workers, (list, tuple)):
            err_msg = 'Expected workers list, {} founded'.format(type(workers))
            logger.error(err_msg)
            raise TypeError(err_msg)

        if len(workers) == 0:
            logger.warning('No workers found')
            return

        # loop by workers definitions
        for worker in workers:
            # getting workers settings
            worker_settings = WorkerSettings(**worker)

            # create worker instances
            for instance_id in range(worker_settings.instances):
                _worker_settings = copy.deepcopy(worker_settings)
                _worker_settings.name = '{name}#{process_id:0>3}'.format(
                                        name=_worker_settings.name,
                                        process_id=instance_id)

                # update queues settings by references to queues
                _worker_settings = self._update_worker_settings_by_queue_refs(_worker_settings)

                _instance = self._create_worker_instance(_worker_settings)
                self._workers_registry.append(_instance)

        logger.info('Workers: %s', self._workers_registry)

    @staticmethod
    def _create_worker_instance(settings):
        ''' create worker instance
        '''
        worker_instance = None
        _module, _class = split_worker_uri(settings.type)
        try:
            worker_module = __import__(_module, globals(), locals(), [_class,], 0)
            worker_class = getattr(worker_module, _class)
            worker_instance = worker_class(settings)
        except AttributeError as err:
            err_msg = f'Worker module: {_module}, worker class: {_class}, error message: {err}'
            logger.error(err_msg)
            raise TypeError(err_msg) from None
        except ImportError as err:
            err_msg = f'Worker module: {_module}, worker class: {_class}, error message: {err}'
            logger.error(err_msg)
            raise TypeError(err_msg) from None

        return worker_instance

    def _update_worker_settings_by_queue_refs(self, settings):
        ''' assign queues to worker inputs/outputs and
            publish events to the queues if present
        '''
        for _, input_queue in settings.inputs.items():
            queue = self._queues_registry.get(input_queue.get('refs'))
            if 'events' in input_queue.keys() and queue is not None:
                for event in input_queue.get('events', []):
                    queue.publish(Event(**event))
            input_queue['refs'] = queue

        for _, output_queue in settings.outputs.items():
            queue = self._queues_registry.get(output_queue.get('refs'))
            if 'events' in output_queue.keys() and queue is not None:
                for event in output_queue.get('events', []):
                    queue.publish(Event(**event))
            output_queue['refs'] = queue

        return settings
