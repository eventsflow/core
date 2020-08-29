
import logging
import importlib

logger = logging.getLogger(__name__)


class QueuesRegistry(object):
    ''' Queues Registries
    '''
    def __init__(self):

        self._registry  = dict()

    def get(self, name):
        ''' get queue by name
        '''
        return self._registry.get(name, None)

    @property
    def queues(self):
        ''' return the list of queues
        '''
        return self._registry

    def load(self, queues):
        ''' load queues to registry
        '''
        if not queues:
            err_msg = 'No queues founded'
            logger.error(err_msg)
            raise TypeError(err_msg)

        if not isinstance(queues, (list, tuple)):
            err_msg = 'Expected queue list, {} founded'.format(type(queues))
            logger.error(err_msg)
            raise TypeError(err_msg)

        for queue in queues:
            try:
                queue_module, queue_class = queue['type'].rsplit('.', 1)
            except KeyError as err:
                err_msg = 'Queue type shall be specified, {}'.format(queue)
                logger.error(err_msg)
                raise TypeError(err_msg)

            queue_size = queue.get('size', 0)
            try:
                self._registry[queue['name']] = getattr(importlib.import_module(queue_module), queue_class)(size=queue_size)
            except KeyError as err:
                err_msg = 'Cannot create queue, no queue name: {}'.format(queue)
                logger.error(err_msg)
                raise TypeError(err_msg)
            except AttributeError as err:
                err_msg = 'Cannot create queue: {}, error: {}'.format(queue, err)
                logger.error(err_msg)
                raise TypeError(err_msg)
        
        logger.info('Queues: {}'.format(self._registry))

