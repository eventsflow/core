
__all__ = [ 'EventsQueue', ]


import logging

from collections import Counter

from queue import Empty as LocalQueueEmpty
from queue import Full as LocalQueueFull

from multiprocessing.queues import Queue


logger = logging.getLogger(__name__)


class EventsQueue(Queue):

    def __init__(self, maxsize=0, *, ctx):
        
        Queue.__init__(self, maxsize, ctx=ctx)
        self._unfinished_events = ctx.Semaphore(0)
        self._cond = ctx.Condition()

    def __getstate__(self):
        
        return Queue.__getstate__(self) + (self._cond, self._unfinished_events)

    def __setstate__(self, state):
        
        Queue.__setstate__(self, state[:-2])
        self._cond, self._unfinished_events = state[-2:]

    def consume(self, block=True, timeout=None):
        ''' consume event from queue
        '''
        return self.get(block=block, timeout=timeout)

    def publish(self, obj, block=True, timeout=None):
        ''' publish event into queue
        '''
        assert not self._closed
        if not self._sem.acquire(block, timeout):
            raise LocalQueueFull

        with self._notempty, self._cond:
            if self._thread is None:
                self._start_thread()
            self._buffer.append(obj)
            self._unfinished_events.release()
            self._notempty.notify()

    def commit(self):
        
        with self._cond:
            if not self._unfinished_events.acquire(False):
                raise ValueError('commit() called too many times')
            if self._unfinished_events._semlock._is_zero():
                self._cond.notify_all()

    def join(self):
        
        with self._cond:
            if not self._unfinished_events._semlock._is_zero():
                self._cond.wait()
