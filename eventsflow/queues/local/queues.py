''' Local Queues
'''

import queue
import logging
import multiprocessing

from multiprocessing.queues import Queue


logger = logging.getLogger(__name__)


class QueueFull(queue.Full):
    ''' Exception in case of Local Queue is Full
    '''

class QueueEmpty(queue.Empty):
    ''' Exception in case of Local Queue is Empty
    '''

class EventsQueue(Queue):
    ''' Events Queue
    '''
    def __init__(self, size=0):
        ctx = multiprocessing.get_context()
        Queue.__init__(self, size, ctx=ctx)
        self._unfinished_tasks = ctx.Semaphore(0)
        self._cond = ctx.Condition()

    def __getstate__(self):
        return Queue.__getstate__(self) + (self._cond, self._unfinished_tasks)

    def __setstate__(self, state):
        Queue.__setstate__(self, state[:-2])
        self._cond, self._unfinished_tasks = state[-2:]

    def consume(self, block=True, timeout=None):
        ''' consume the event from queue
        '''
        try:
            event = self.get(block=block, timeout=timeout)
            return event
        except queue.Empty:
            raise QueueEmpty from None

    def publish(self, obj, block=True, timeout=None):
        ''' publish event into queue
        '''
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")
        if not self._sem.acquire(block, timeout):
            raise QueueFull

        with self._notempty, self._cond:
            if self._thread is None:
                self._start_thread()
            self._buffer.append(obj)
            self._unfinished_tasks.release()
            self._notempty.notify()

    def commit(self):
        ''' commit event in queue
        '''
        with self._cond:
            if not self._unfinished_tasks.acquire(False):
                raise ValueError('commit() called too many times')
            if self._unfinished_tasks._semlock._is_zero():
                self._cond.notify_all()

    def join(self):
        ''' join queue
        '''
        with self._cond:
            if not self._unfinished_tasks._semlock._is_zero():
                self._cond.wait()

    def size(self):
        ''' Return the approximate size of the queue.
        Because of multithreading/multiprocessing semantics, this number is not reliable.
        '''
        _size = self.qsize()
        if self.empty() or _size <= 0:
            return 0
        return _size
