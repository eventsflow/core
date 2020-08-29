
import pytest

from eventsflow.flow.parser import FlowParser
from eventsflow.flow.parser import IncorrectFlowFormat


def test_flow_parser_init():

    parser = FlowParser('tests/resources/flows/simple-flow.yaml')
    assert parser


def test_flow_parser_path_doesnt_exist():

    with pytest.raises(IOError):
        parser = FlowParser('tests/resources/flows/missed-flow.yaml')
        assert parser


def test_flow_parser_parse():

    parser = FlowParser('tests/resources/flows/simple-flow.yaml')
    queues, workers = parser.parse()
    assert queues
    assert workers


def test_flow_parser_empty_flow():

    parser = FlowParser('tests/resources/flows/empty.yaml')
    with pytest.raises(IncorrectFlowFormat) as err:
        parser.parse()


def test_flow_parser_corrupted_flow():

    parser = FlowParser('tests/resources/flows/flow-corrupted.yaml')
    with pytest.raises(IncorrectFlowFormat) as err:
        parser.parse()
