# -*- coding: utf-8 -*-
# Michael Breen
# Copyright (C) 2015 MPSTOR mpstor.com
"""Eskimo Locks: an alternative approach to locking.
A possible use case is where a pool of locks might otherwise be considered
as a way to synchronize operations on a changing set of external resources.
"""
import threading
import time

__copyright__ = "Copyright (c) 2014, 2015 MPSTOR mpstor.com"
__license__ = "See LICENSE"
__author__ = "Michael Breen"

class Timeout(Exception):
    pass


class Lock(object):
    """
    A Lock object locks one or more hashable objects.

    Unlike conventional locks, it's the objects that are locked, not the
    Locks: two different (writer-mode) Locks cannot simultaneously lock
    the same object or any object which evaluates as equal to it.
    (ID strings or numbers can serve as proxies for other resources.)

    In contrast to the usual model, where a thread holds a lock, it is the
    objects that are shared across threads, and not normally the Lock;
    should you find a reason to pass a Lock from one thread to another,
    the second thread could unlock the object(s) locked by the first.

    Multiple objects can be locked in a single atomic operation.

    Shared-exclusive locking, or reader-writer locking, is supported.

    Priority: if an object is locked by one or more readers then the
    first writer to block on that object has priority over later
    readers and writers, so that a continual rotation of readers will not
    stall writers; in all other cases, priority is undefined.

    Simplicity of implementation is preferred over other criteria.
    """
    # one global lock to control updates to the set of locked objects
    _guard = threading.Condition()
    # maps each locked object to the Lock exclusively holding it
    _locked_objs = {}
    # maps each shared object to a list of Locks sharing it
    _shared_objs = {}

    def __init__(self, obj, *objs, **params):
        """Lock(obj1, ..., shared=False, timeout=None)

        obj1, ...: one or more hashable objects to be locked.
        shared: True for a shared (reader) Lock.
        timeout: None or time in seconds acquire() is allowed to block,
        waiting for the objects to be locked to become available.
        """
        self._objs = set(objs + (obj,))
        self._shared = params.pop('shared', False)
        self._timeout = params.pop('timeout', None)
        if params:
            raise TypeError("unexpected keyword argument")

    def timeout(self, seconds):
        """Change this Lock's timeout and return the altered Lock.
        Useful for, e.g.,

            with my_lock.timeout(1.5):
                do_stuff()
        """
        self._timeout = seconds
        return self

    def acquire(self):
        """Lock this Lock's object(s), blocking if necessary.
        If timeout is None then block as long as necessary, and return None.
        If the objects can be locked immediately, return timeout.
        If blocking is required and timeout <= 0 then raise Timeout.
        If blocking is required and timeout > 0 then either raise Timeout
        on expiry of the timer or return the time in seconds to spare,
        i.e., timeout minus the time spent blocked (due to timing
        imperfections, this may conceivably be less than zero).
        """
        remaining = [self._timeout]
        def wait_till_free(lock_map):
            while self._objs & set(lock_map) and (
                    remaining[0] is None or remaining[0] > 0):
                s1 = time.time()
                Lock._guard.wait(remaining[0])
                if remaining[0] is not None:
                    remaining[0] -= time.time() - s1
            if self._objs & set(lock_map):
                raise Timeout("Timeout locking %r." % list(self._objs))
        # (Time blocked on the guard is disregarded.)
        Lock._guard.acquire()
        try:
            wait_till_free(Lock._locked_objs)
            if self._shared:
                for obj in self._objs:
                    locks = Lock._shared_objs.setdefault(obj, [])
                    locks.append(self)
            else:
                Lock._locked_objs.update({x: self for x in self._objs})
                try:
                    wait_till_free(Lock._shared_objs)
                except:
                    self.release()
                    raise
            return remaining[0]
        finally:
            Lock._guard.release()

    __enter__ = acquire

    def release(self, *ignored):
        """Unlock this Lock's object(s).
        Raise RuntimeError if the object(s) are not locked by this Lock.
        """
        def complain():
            raise RuntimeError("Attempt to release unacquired Lock.")
        Lock._guard.acquire()
        try:
            for obj in self._objs:
                if self._shared:
                    try:
                        locks = Lock._shared_objs[obj]
                        locks.remove(self)
                    except:
                        complain()
                    if not locks:
                        del Lock._shared_objs[obj]
                else:
                    if Lock._locked_objs.get(obj) is not self:
                        complain()
                    del Lock._locked_objs[obj]
        finally:
            Lock._guard.notify_all()
            Lock._guard.release()

    __exit__ = release
