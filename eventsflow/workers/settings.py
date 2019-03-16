
import sys
import logging

logger = logging.getLogger(__name__)

# Default processing timeout, how long worker is waiting for new event
DEFAULT_PROCESSING_TIMEOUT = 300

# Time interval for checking status
STATUS_CHECK_TIME_INTERVAL = 5


class Settings(object):
    ''' Worker Settings
    '''

    def __init__(self, queues, **settings):

        # worker name
        self.name           = settings.get('name', None)
        # worker parameters
        self.parameters     = settings.get('parameters', {})
        self.input_queues   = self.get_queues(settings.get('input', None), queues)
        self.output_queues  = self.get_queues(settings.get('output', None), queues)
        self.instances      = settings.get('instances', 1)
        self.events         = settings.get('events', [])

        try:
            self.worker_module, self.worker_class = self.parse_type(settings['type'])
        except KeyError as err:
            raise TypeError('Worker type shall be specified, {}'.format(settings))

    def parse_type(self, worker_type):
        
        worker_module, worker_class = worker_type.rsplit('.', 1)
        return worker_module, worker_class

    def get_queues(self, worker_queues, all_queues):

        queues = { 'default': None }
        if not worker_queues:
            return queues

        try:
            if worker_queues and isinstance(worker_queues, str):
                queues['default'] = all_queues[worker_queues]
            elif worker_queues and isinstance(worker_queues, dict):
                queues = { name: all_queues[queue] for name, queue in worker_queues.items() }
            else:
                raise TypeError('Unknown worker queue type, {}'.format(type(worker_queues)))
        except KeyError as err:
            logger.error('Undefined queue name, {}'.format(err))
            sys.exit(1)

        return queues
        
    def get_worker_settings(self, process_id):

        return {
            'name':             '{name}#{process_id:0>3}'.format(name=self.name, process_id=process_id),
            'parameters':       self.parameters,
            'input_queues':     self.input_queues,
            'output_queues':    self.output_queues,
            'items':            self.items,
        }

    @property
    def workers(self):

        workers = list()
        for i in range(self.instances):
            worker_settings = self.get_worker_settings(process_id=i)
            try:
                # worker_class = getattr(importlib.import_module(self.worker_module),self.worker_class)
                worker_module = __import__(self.worker_module, globals(), locals(), [self.worker_class,], 0)
                worker_class = getattr(worker_module, self.worker_class)
                workers.append(worker_class(**worker_settings))
            except ImportError as err:
                logger.error('Worker module: {}, worker class: {}, error message: {}'.format(
                    self.worker_module, self.worker_class, err))
                sys.exit(1)
        return workers
