''' Local Queues Context
'''
from multiprocessing.context import ForkContext
from multiprocessing.context import DefaultContext

default_context = DefaultContext(ForkContext())
