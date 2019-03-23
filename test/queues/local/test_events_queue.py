
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

