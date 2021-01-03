''' Tests for Local Events Queue
'''
import pytest

from eventsflow.events import Event

from eventsflow.queues.local import QueueFull
from eventsflow.queues.local import EventsQueue

def test_events_queue_init():
    ''' tests initialize events queue
    '''
    events_queue = EventsQueue()
    assert events_queue

def test_events_queue_publish_consume_commit():
    ''' test pubish/consume events from queue
    '''
    events_queue = EventsQueue()
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    events_queue.publish(event)
    received_event = events_queue.consume()
    events_queue.commit()

    assert isinstance(received_event, Event)
    assert received_event.name == event.name
    assert received_event.metadata == event.metadata
    assert received_event.payload == event.payload


def test_events_queue_with_size():
    ''' test queue with predefined size
    '''
    queue_size = 5

    events_queue = EventsQueue(size=queue_size)
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    for _ in range(queue_size):
        events_queue.publish(event)

    events = []
    for _ in range(queue_size):
        events.append(events_queue.consume())

    assert len(events) == queue_size


def test_events_queue_overflow():
    ''' test events queue overflow
    '''
    queue_size = 6
    events_queue = EventsQueue(size=queue_size-1)
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    with pytest.raises(QueueFull):
        for _ in range(queue_size):
            events_queue.publish(event, timeout=1)

def test_events_queue_many_commits():
    ''' test many commits handling
    '''
    events_queue = EventsQueue()

    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])
    events_queue.publish(event)

    with pytest.raises(ValueError):
        event = events_queue.consume()
        events_queue.commit()
        events_queue.commit()
