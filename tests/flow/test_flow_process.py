''' Tests for Eventsflow Flow
'''
from eventsflow.flow.process import Flow
from eventsflow.workers.process import ProcessingWorker


def test_flow_init():
    ''' test for Flow initialization
    '''
    flow = Flow(path='tests/resources/flows/simple-flow.yaml')
    assert flow

def test_flow_get_queues():
    ''' test for flow, getting queue list
    '''
    flow = Flow(path='tests/resources/flows/simple-flow.yaml')
    assert list(flow.queues) == [ 'TestQueue', ]

def test_flow_get_workers():
    ''' test for flow, getting workers list
    '''
    flow = Flow(path='tests/resources/flows/simple-flow.yaml')
    assert [ type(w) for w in flow.workers() ] == [ProcessingWorker,]

def test_flow_get_current_status():
    ''' test for flow, getting current status
    '''
    flow = Flow(path='tests/resources/flows/simple-flow.yaml')
    assert flow.get_current_status(with_logging=True) == {
        'activeWorkers':            [],
        'activeWorkersByName':      [],
        'inactiveWorkersByName':    ['TestWorker#000',],
        'queues':                   {'TestQueue': 0}, # the queue is not initialized
    }
