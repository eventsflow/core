
import json

class Event(object):

    name        = 'EventBase'
    metadata    = dict()
    payload     = list()

    def __init__(self, name, metadata=dict(), payload=list()):

        self.name   = name
        self.metadata   = metadata
        self.payload    = payload

    def to_dict(self):
        ''' convert event to dict
        '''
        return {
            'name': self.name,
            'metadata': self.metadata,
            'payload': self.payload,
        }

    def to_json(self):
        ''' convert event to json
        '''
        return json.dumps(self.to_dict())


class EventDrop(Event):

    def __init__(self):

        super(EventDrop, self).__init__(name='EventDrop')


class EventStopProcessing(Event):

    def __init__(self):

        super(EventStopProcessing, self).__init__(name='EventStopProcessing')
