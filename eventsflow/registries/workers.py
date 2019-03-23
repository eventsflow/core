
import sys
import copy
import logging

from eventsflow.workers.settings import Settings as WorkerSettings

from eventsflow.utils import split_worker_uri


logger = logging.getLogger(__name__)


class WorkersRegistry(object):
    ''' Workers Registry
    '''
    def __init__(self, queues=None):

        if not queues:
            raise TypeError('No queues registry founded, {}'.format(queues))

        self._queues_registry = queues
        self._workers_registry  = list()

    @property
    def workers(self):
        ''' iterate by workers
        '''
        for worker in self._workers_registry:
            yield worker

    def load(self, workers):
        ''' load workers to registry
        '''
        if not workers:
            err_msg = 'No workers founded'
            logger.error(err_msg)
            raise TypeError(err_msg)

        if not isinstance(workers, (list, tuple)):
            err_msg = 'Expected workers list, {} founded'.format(type(workers))
            logger.error(err_msg)
            raise TypeError(err_msg)

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

        logger.info('Workers: {}'.format(self._workers_registry))

    def _create_worker_instance(self, settings):
        ''' create worker instance
        '''
        worker_instance = None
        _module, _class = split_worker_uri(settings.type)
        try:
            worker_module = __import__(_module, globals(), locals(), [_class,], 0)
            worker_class = getattr(worker_module, _class)
            worker_instance = worker_class(settings)
        except AttributeError as err:
            err_msg = 'Worker module: {}, worker class: {}, error message: {}'.format(_module, _class, err)
            logger.error(err_msg)
            raise TypeError(err_msg)
        except ImportError as err:
            err_msg = 'Worker module: {}, worker class: {}, error message: {}'.format(_module, _class, err)
            logger.error(err_msg)
            raise TypeError(err_msg)

        return worker_instance

    def _update_worker_settings_by_queue_refs(self, settings):

        for input in settings.inputs:
            input['refs'] = self._queues_registry.get(input.get('refs'))
    
        for output in settings.outputs:
            output['refs'] = self._queues_registry.get(output.get('refs'))

        return settings
