

class Event(object):

    name        = 'EventBase'
    metadata    = dict()
    payload     = list()

    def __init__(self, name, metadata=dict(), payload=list()):

        self.name   = name
        self.metadata   = metadata
        self.payload    = payload


class EventStopProcessing(Event):

    def __init__(self):

        super(StopProcessingEvent, self).__init__(name='EventStopProcessing')
