# 
#   Based on source code from https://github.com/python/cpython/blob/3.6/Lib/multiprocessing/context.py
# 
# 
__all__ = []

from multiprocessing import process

from multiprocessing.context import BaseContext as mpBaseContext
from multiprocessing.context import DefaultContext as mpDefaultContext
from multiprocessing.context import Process 


class BaseContext(mpBaseContext):

    def EventsQueue(self, size=0):
        '''Returns a events queue object
        '''
        from .queues import EventsQueue
        return EventsQueue(size, ctx=self.get_context())


class DefaultContext(BaseContext):
    Process = Process

    def __init__(self, context):
        self._default_context = context
        self._actual_context = None

    def get_context(self, method=None):
        if method is None:
            if self._actual_context is None:
                self._actual_context = self._default_context
            return self._actual_context
        else:
            return super().get_context(method)

    def set_start_method(self, method, force=False):
        if self._actual_context is not None and not force:
            raise RuntimeError('context has already been set')
        if method is None and force:
            self._actual_context = None
            return
        self._actual_context = self.get_context(method)

    def get_start_method(self, allow_none=False):
        if self._actual_context is None:
            if allow_none:
                return None
            self._actual_context = self._default_context
        return self._actual_context._name

    def get_all_start_methods(self):
        if sys.platform == 'win32':
            return ['spawn']
        else:
            if reduction.HAVE_SEND_HANDLE:
                return ['fork', 'spawn', 'forkserver']
            else:
                return ['fork', 'spawn']


DefaultContext.__all__ = list(x for x in dir(DefaultContext) if x[0] != '_')


class ForkProcess(process.BaseProcess):
    _start_method = 'fork'
    @staticmethod
    def _Popen(process_obj):
        from .popen_fork import Popen
        return Popen(process_obj)


class ForkContext(BaseContext):
    _name = 'fork'
    Process = ForkProcess


_default_context = DefaultContext(ForkContext())
