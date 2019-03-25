
import os
import yaml
import logging

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


from eventsflow.utils import parse_with_extra_vars


class IncorrectFlowFormat(Exception):
    pass


logger = logging.getLogger(__name__)


class FlowParser(object):
    ''' The Flow Parser
    '''
    def __init__(self, path):

        if not os.path.isfile(path):
            raise IOError('The file does not exist, {}'.format(path))

        self._path = path

    def parse(self, extra_vars={}):

        flow_details = parse_with_extra_vars(self._path, extra_vars)

        if not flow_details:
            raise IncorrectFlowFormat('Incorrect Flow format')

        # parse topology YAML file
        try:
            flow = yaml.load(flow_details, Loader=Loader)
        except  yaml.YAMLError as err:
            raise IncorrectFlowFormat('Incorrect Flow format: {}'.format(err))

        # load queues and workers 
        queues  = flow.get('queues', [])
        workers = flow.get('workers', [])
        
        return queues, workers
