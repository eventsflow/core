
import sys
import logging

logger = logging.getLogger(__name__)

# Default processing timeout, how long worker is waiting for new event
DEFAULT_EVENT_WAITING_TIMEOUT = 300


# 
# Worker settings example:
# ------------------------
# 
# - name: TestWorker
#   type: eventsflow.workers.DummyWorker
#   description: Test worker
#   instances: 1
#   parameters:
#     param1: values1
#     param2: values2
#   inputs: input-queue
#   outputs: output-queue
#
# Example with named queues and passing events via worker's configuration
# -----------------------------------------------------------------------
#  
# - name: TestWorker
#   type: eventsflow.workers.DummyWorker
#   description: Test worker
#   instances: 1
#   parameters:
#     param1: values1
#     param2: values2
#   inputs: 
#   - name: default
#     refs: input-queue
#     events: 
#     - { "name": "EventTest", "metadata": {}, "payload": [] }
#   outputs: output-queue


class Settings(object):
    ''' Worker Settings
    '''

    def __init__(self, **settings):

        # worker name
        self.name           = settings.get('name', None)
        if not self.name:
            raise TypeError('The worker name shall be specifed, {}'.format(self.name))

        # worker type
        self.type           = settings.get('type', None)
        if not self.type:
            raise TypeError('The worker type shall be specifed, {}'.format(self.type))

        # worker parameters
        self.parameters     = settings.get('parameters', {})
        if not isinstance(self.parameters, dict):
            raise TypeError('The parameters shall be specified as dictionary, founded {}'.format(type(self.parameters)))
                
        # no of instances
        self.instances      = settings.get('instances', 1)
        if not isinstance(self.instances, int) or self.instances <= 0:
            raise TypeError('The instances parameter shall be int type and equals 1 or more')

        # events
        # self.events         = settings.get('events', [])

        # inputs
        self.inputs     = self.parse_gueues(settings.get('inputs', None))
        
        # outputs
        self.outputs    = self.parse_gueues(settings.get('outputs', None)) 

    @staticmethod
    def parse_gueues(queues):
        ''' parse queues
        '''
        _queues = dict()

        if queues is None:
            _queues['default'] = { 'refs': None, 'events': [] }

        elif isinstance(queues, str):
            _queues['default'] = {'refs': queues, 'events': [] }

        elif isinstance(queues, (list, tuple)):
            for queue in queues:
                if not isinstance(queue, dict):
                    raise TypeError('The queue shall be specified as dictionary, founded: {}'.format(type(queue)))
                _queues[queue.get('name')] = {
                    'refs': queue.get('refs', None),
                    'events': queue.get('events', []),
                }
        else:
            logger.warning('Founded queues definition but cannot process it, expected list, founded: {}'.format(type(queues)))

        return _queues
