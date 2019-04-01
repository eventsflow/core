
import json

from eventsflow import events


def test_base_event():

    event = events.Event(name='Event#1', metadata=dict(), payload=list())
    assert event.name == 'Event#1'


def test_stop_processing_event():

    event = events.EventStopProcessing()
    assert event.name == 'EventStopProcessing'
    assert event.metadata == {}
    assert event.payload == []


def test_event_drop():

    event = events.EventDrop()
    assert event.name == 'EventDrop'
    assert event.metadata == {}
    assert event.payload == []


def test_event_with_attrs():

    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])

    assert event.name == 'Event#1'
    assert event.metadata == { 'k1': 'v1' }
    assert event.payload == [ 'p1', 'p2', 'p3']


def test_change_events_attrs():

    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])

    event.metadata.update({'k2': 'v2'})
    event.payload.append('p4')

    assert event.name == 'Event#1'
    assert event.metadata == { 'k1': 'v1', 'k2': 'v2', }
    assert event.payload == [ 'p1', 'p2', 'p3', 'p4',]


def test_event_to_dict():

    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])
    assert event.to_dict() == {
        'name': 'Event#1', 'metadata': { 'k1': 'v1' }, 'payload': [ 'p1', 'p2', 'p3']
    }


def test_event_to_json():

    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])
    assert json.loads(event.to_json()) == json.loads("""
        {"name": "Event#1", "metadata": { "k1": "v1" }, "payload": [ "p1", "p2", "p3"] }
    """)

