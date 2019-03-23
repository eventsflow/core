
__all__ = [ 'EventsQueue', ]


import logging

from collections import Counter
from multiprocessing.queues import Queue


logger = logging.getLogger(__name__)


class EventsQueue(Queue):

    def __init__(self, maxsize=0, *, ctx):
        
        Queue.__init__(self, maxsize, ctx=ctx)
        self._unfinished_tasks = ctx.Semaphore(0)
        self._cond = ctx.Condition()

    def __getstate__(self):
        
        return Queue.__getstate__(self) + (self._cond, self._unfinished_tasks)

    def __setstate__(self, state):
        
        Queue.__setstate__(self, state[:-2])
        self._cond, self._unfinished_tasks = state[-2:]

    def consume(self, timeout=0):
        ''' consume event from queue
        '''
        return self.get(timeout=timeout)

    def publish(self, obj, block=True, timeout=None):
        ''' publish event into queue
        '''
        assert not self._closed
        if not self._sem.acquire(block, timeout):
            raise Full

        with self._notempty, self._cond:
            if self._thread is None:
                self._start_thread()
            self._buffer.append(obj)
            self._unfinished_tasks.release()
            self._notempty.notify()

    def commit(self):
        
        with self._cond:
            if not self._unfinished_tasks.acquire(False):
                raise ValueError('task_done() called too many times')
            if self._unfinished_tasks._semlock._is_zero():
                self._cond.notify_all()

    def join(self):
        
        with self._cond:
            if not self._unfinished_tasks._semlock._is_zero():
                self._cond.wait()
