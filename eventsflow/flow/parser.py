''' Flow Parser Module
'''
import os
import logging

import yaml

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


from eventsflow.utils import parse_with_extra_vars


class IncorrectFlowFormat(Exception):
    ''' The exception for an incorrect flow format
    '''

logger = logging.getLogger(__name__)


class FlowParser:
    ''' Flow Parser
    '''
    def __init__(self, path:str):
        ''' Initialize Flow Parser
        '''
        if not os.path.isfile(path):
            raise IOError(f'The file does not exist, {path}')

        self._path = path

    def parse(self, extra_vars=None):
        ''' parse flow configuration
        '''
        if not extra_vars:
            extra_vars = dict()

        flow_details = parse_with_extra_vars(self._path, extra_vars)

        if not flow_details:
            raise IncorrectFlowFormat('Incorrect Flow format')

        # parse topology YAML file
        try:
            flow = yaml.load(flow_details, Loader=Loader)
        except  yaml.YAMLError as err:
            raise IncorrectFlowFormat(f'Incorrect Flow format: {err}')

        # load queues and workers
        queues  = flow.get('queues', [])
        workers = flow.get('workers', [])

        return queues, workers
