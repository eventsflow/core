''' Events Module
'''
import json

class Event:
    ''' Event
    '''
    name        = 'EventBase'
    metadata    = dict()
    payload     = list()

    def __init__(self, name:str, metadata:dict=None, payload:list=None):
        ''' initialize event
        @param name     the event name
        @param metadata the metadata is key/value storage
                        with additional event information
        @param payload  the list of payload(-s)
        '''
        self.name   = name
        self.metadata   = metadata if metadata else dict()
        self.payload    = payload if payload else list()

    def to_dict(self) -> dict:
        ''' convert event to dict
        '''
        return {
            'name': self.name,
            'metadata': self.metadata,
            'payload': self.payload,
        }

    def to_json(self) -> str:
        ''' convert event to json
        '''
        return json.dumps(self.to_dict())


class EventDrop(Event):
    ''' Drop Event
    '''
    def __init__(self):

        super().__init__(name='EventDrop')


class EventStopProcessing(Event):
    ''' Event for Stop Processing
    '''
    def __init__(self):

        super().__init__(name='EventStopProcessing')
