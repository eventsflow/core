''' Initialize Local Queues Module
'''
# import sys

from eventsflow.queues.local import context

from eventsflow.queues.local.queues import QueueFull
from eventsflow.queues.local.queues import QueueEmpty
from eventsflow.queues.local.queues import EventsQueue


# copy stuff from default context
__all__ = [x for x in dir(context.default_context) if not x.startswith('_')]
globals().update((name, getattr(context.default_context, name)) for name in __all__)
