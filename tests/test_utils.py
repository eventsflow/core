''' Tests for utils
'''
from eventsflow.utils import split_worker_uri
from eventsflow.utils import parse_with_extra_vars


def test_flow_simple_template():
    ''' test for processing simple template
    '''
    source = 'tests/resources/flows/simple-template.yaml'
    target = 'tests/resources/flows/simple-template-result.yaml'

    assert parse_with_extra_vars(
                source, {'filename': 'test.yaml'}
            ).strip() == open(target).read().strip()

def test_flow_unspecified_placeholder():
    ''' test for processing unspecified placeholder
    '''
    source = 'tests/resources/flows/unspecified-placeholder.yaml'

    assert parse_with_extra_vars(
                source, {'filename': 'test.yaml'}
            ) is None


def test_split_worker_uri():
    ''' test for spliting worker URI
    '''
    assert split_worker_uri('') == (None, None)
    assert split_worker_uri(None) == (None, None)

    assert split_worker_uri(
        'eventsflow.workers.process.ProcessingWorker'
    ) == ('eventsflow.workers.process', 'ProcessingWorker')
