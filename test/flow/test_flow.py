

import pytest

from eventsflow.flow import Flow


def test_flow_init():

    flow = Flow(path='test/resources/flows/simple-flow.yaml')
    assert flow


def test_flow_get_queues():

    from eventsflow.queues.local import EventsQueue

    flow = Flow(path='test/resources/flows/simple-flow.yaml')
    assert [ queue_name for queue_name in flow.queues ] == [ 'TestQueue', ]


def test_flow_get_workers():

    from eventsflow.workers.process import ProcessingWorker
    
    flow = Flow(path='test/resources/flows/simple-flow.yaml')
    assert [ type(w) for w in flow.workers() ] == [ProcessingWorker,]


def test_flow_get_current_status():

    flow = Flow(path='test/resources/flows/simple-flow.yaml')
    assert flow.get_current_status(with_logging=True) == {
        'activeWorkers':            [],
        'activeWorkersByName':      [],
        'inactiveWorkersByName':    ['TestWorker#000',],
        'queues':                   {'TestQueue': -2147483647}, # the queue is not initialized
    }

