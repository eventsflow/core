
import pytest

from eventsflow.events import Event
from eventsflow.queues.local import EventsQueue


def test_events_queue_init():

    events_queue = EventsQueue()
    assert events_queue


def test_events_queue_publish_consume_commit():

    events_queue = EventsQueue()
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    events_queue.publish(event)
    received_event = events_queue.consume()
    events_queue.commit()

    assert type(received_event) == type(event)
    assert received_event.name == event.name
    assert received_event.metadata == event.metadata
    assert received_event.payload == event.payload


def test_events_queue_with_size():

    QUEUE_SIZE = 5

    events_queue = EventsQueue(size=QUEUE_SIZE)
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    for _ in range(QUEUE_SIZE):
        events_queue.publish(event)
    
    events = []
    for _ in range(QUEUE_SIZE):
        events.append(events_queue.consume())
    
    assert len(events) == QUEUE_SIZE


def test_events_queue_overflow():

    from eventsflow.queues.local.queues import LocalQueueFull

    QUEUE_SIZE = 6

    events_queue = EventsQueue(size=QUEUE_SIZE-1)
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])

    with pytest.raises(LocalQueueFull):
        for _ in range(QUEUE_SIZE):
            events_queue.publish(event, timeout=1)

    
def test_events_queue_many_commits():

    events_queue = EventsQueue()
    
    event = Event(name='TestEvent', metadata={'k1': 'v1'}, payload=['TestEventPayload', ])
    events_queue.publish(event)

    with pytest.raises(ValueError):
        event = events_queue.consume()
        events_queue.commit()
        events_queue.commit()
