''' Tests for Flow parser
'''
import pytest

from eventsflow.flow.parser import FlowParser
from eventsflow.flow.parser import IncorrectFlowFormat

def test_flow_parser_init():
    ''' test for flow parser initialization
    '''
    parser = FlowParser('tests/resources/flows/simple-flow.yaml')
    assert parser

def test_flow_parser_path_doesnt_exist():
    ''' test for flow parser when a path doesn't exist
    '''
    with pytest.raises(IOError):
        parser = FlowParser('tests/resources/flows/missed-flow.yaml')
        assert parser

def test_flow_parser_parse():
    ''' test for flow parser, run parse() method
    '''
    parser = FlowParser('tests/resources/flows/simple-flow.yaml')
    queues, workers = parser.parse()
    assert queues
    assert workers

def test_flow_parser_empty_flow():
    ''' test for flow parser with empty flow
    '''
    parser = FlowParser('tests/resources/flows/empty.yaml')
    with pytest.raises(IncorrectFlowFormat):
        parser.parse()

def test_flow_parser_corrupted_flow():
    ''' test for flow parser with corrupted flow
    '''
    parser = FlowParser('tests/resources/flows/flow-corrupted.yaml')
    with pytest.raises(IncorrectFlowFormat):
        parser.parse()
