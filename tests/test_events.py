''' Tests for Events
'''
import json

from eventsflow import events

def test_base_event():
    ''' test for base event
    '''
    event = events.Event(name='Event#1', metadata=dict(), payload=list())
    assert event.name == 'Event#1'

def test_stop_processing_event():
    ''' test for StopProcessing event
    '''
    event = events.EventStopProcessing()
    assert event.name == 'EventStopProcessing'
    assert event.metadata == {}
    assert event.payload == []

def test_event_drop():
    ''' test for EventDrop event
    '''
    event = events.EventDrop()
    assert event.name == 'EventDrop'
    assert event.metadata == {}
    assert event.payload == []

def test_event_with_attrs():
    ''' test for events with attributes
    '''
    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])

    assert event.name == 'Event#1'
    assert event.metadata == { 'k1': 'v1' }
    assert event.payload == [ 'p1', 'p2', 'p3']

def test_change_events_attrs():
    ''' test for changing events attributes
    '''
    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])

    event.metadata.update({'k2': 'v2'})
    event.payload.append('p4')

    assert event.name == 'Event#1'
    assert event.metadata == { 'k1': 'v1', 'k2': 'v2', }
    assert event.payload == [ 'p1', 'p2', 'p3', 'p4',]

def test_event_to_dict():
    ''' test for converting event as dict
    '''
    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])
    assert event.to_dict() == {
        'name': 'Event#1', 'metadata': { 'k1': 'v1' }, 'payload': [ 'p1', 'p2', 'p3']
    }

def test_event_to_json():
    ''' test for converting event as json
    '''
    event = events.Event(name = 'Event#1', metadata = { 'k1': 'v1' }, payload = [ 'p1', 'p2', 'p3'])
    assert json.loads(event.to_json()) == json.loads("""
        {"name": "Event#1", "metadata": { "k1": "v1" }, "payload": [ "p1", "p2", "p3"] }
    """)
