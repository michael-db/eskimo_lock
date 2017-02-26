# -*- coding: utf-8 -*-
"""Test eskimo.py
"""
import eskimo
import threading
import time
import unittest

__copyright__ = "Copyright (c) 2014, 2015 MPSTOR mpstor.com"
__license__ = "See LICENSE"
__author__ = "Michael Breen"

class LockingTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        # cleanup after any tests that leave something locked
        eskimo.Lock._locked_objs = {}
        eskimo.Lock._shared_objs = {}
        pass

    def test_errors(self):
        """Test some error cases."""
        # no object provided
        self.assertRaises(TypeError, eskimo.Lock)
        # unhashable object provided
        self.assertRaises(TypeError, eskimo.Lock, [1])
        # hashable + unhashable object
        self.assertRaises(TypeError, eskimo.Lock, 1, [1])
        obj = 1
        lock = eskimo.Lock(obj)
        self.assertRaises(RuntimeError, lock.release)
        self.assertRaises(TypeError, lambda: eskimo.Lock(obj,
                exclusive=True))

    def test_basic(self):
        obj = ('my', 'lock')
        lock1 = eskimo.Lock(obj, timeout=0.0)
        lock1.acquire()
        # re-entrant eskimo not supported
        self.assertRaises(eskimo.Timeout, lock1.acquire)
        self.assertRaises(eskimo.Timeout, lock1.timeout(0.1).acquire)
        obj_copy = obj + tuple()
        self.assertIsNot(obj, obj_copy)
        self.assertEquals(obj, obj_copy)
        # Lock on the same (test by equality) object
        lock2 = eskimo.Lock(obj_copy, timeout=0)
        # - cannot simultaneously acquire()
        self.assertRaises(eskimo.Timeout, lock2.acquire)
        # - and only the Lock which did acquire() can release(),
        self.assertRaises(RuntimeError, lock2.release)
        lock1.release()
        lock2.acquire()
        self.assertRaises(eskimo.Timeout, lock1.timeout(0).acquire)
        lock2.release()
        with lock2:
            self.assertRaises(eskimo.Timeout, lock1.acquire)
        self.assertRaises(RuntimeError, lock2.release)
        lock1.acquire()
        # lock on a different object can be held simultaneously
        obj_another = 'your', 'lock'
        eskimo.Lock(obj_another).acquire()

    def test_threads(self):
        obj = 'test_threads'
        lock = eskimo.Lock(obj)
        lock.acquire()
        t = threading.Timer(0.5, lambda: lock.release())

        # The existence of another thread waiting to lock another object
        # does not cause an undue delay in unbeskimo here (if release()
        # used notify() instead of notify_all(), it would).
        lock2 = eskimo.Lock('b')
        lock2.acquire()
        t2 = threading.Thread(target=lambda: lock2.acquire())
        t2.setDaemon(True)
        t2.start()

        self.assertRaises(eskimo.Timeout, lock.timeout(0.1).acquire)
        start = time.time()
        t.start()
        lock.timeout(1.5).acquire()
        waited = time.time() - start
        # Without notify_all() in release(), acquire() would take the full 4s
        # (and an acquire() without a timeout would block indefinitely).
        self.assertTrue(0.5 < waited < 1.5)

        # Explicit timeout=None.
        def releaser(): time.sleep(2); lock.release()
        t3 = threading.Thread(target=releaser)
        start = time.time()
        t3.start()
        with lock.timeout(None):
            waited = time.time() - start
        self.assertTrue(2 < waited < 2.5)

    def test_multiple_objs(self):
        """Test Locks of more than one object."""
        lock12 = eskimo.Lock(1, 2)
        lock23 = eskimo.Lock(2, 3)
        lock34 = eskimo.Lock(3, 4)
        lock12.acquire()
        self.assertRaises(eskimo.Timeout, lock23.timeout(0).acquire)
        lock34.acquire()
        lock12.release()
        self.assertRaises(eskimo.Timeout, lock23.acquire)
        lock34.release()
        lock23.acquire()
        self.assertRaises(eskimo.Timeout, lock12.timeout(0).acquire)
        self.assertRaises(eskimo.Timeout, lock34.timeout(0).acquire)
        lock23.release()

    def test_timeout_release(self):
        # Unbeskimo of writer on timeout of blocked higher priority writer
        # waiting for a slow reader.
        lock1 = eskimo.Lock(1, shared=True)
        lock12 = eskimo.Lock(1, 2)
        lock2 = eskimo.Lock(2)
        lock1.acquire()
        def try_to_lock(lock, timeout):
            try:
                lock.timeout(timeout).acquire()
            except:
                pass
        t = threading.Thread(target=lambda: try_to_lock(lock12, 0.3))

        # The existence of another thread waiting to lock another object
        # does not cause an undue delay in unbeskimo here (if release()
        # used notify() instead of notify_all(), it would).
        locko = eskimo.Lock('other')
        locko.acquire()
        to = threading.Thread(target=lambda: locko.acquire())
        to.setDaemon(True)
        to.start()

        start = time.time()
        t.start()
        time.sleep(0.1)
        lock2.timeout(1).acquire()
        waited = time.time() - start
        # without notify_all() in release(), acquire() would take the full 4s
        # (and an acquire() without a timeout would block indefinitely)
        self.assertTrue(0.3 < waited < 0.4)

    def test_shared_reentrancy(self):
        """It is possible to acquire() a shared lock more than once,
        but the same number of calls must be made to release().
        """
        obj = 'rothar'
        slock = eskimo.Lock(obj, shared=True, timeout=0)
        slock.acquire()
        slock.acquire()
        slock.release()
        xlock = eskimo.Lock(obj, timeout=0)
        self.assertRaises(eskimo.Timeout, xlock.acquire)
        slock.release()
        xlock.acquire()
        self.assertRaises(RuntimeError, slock.release)
        xlock.release()

    def test_shared_priority_simple(self):
        """Priority of first writer to block on object held by reader.
        """
        obj = 'anything'
        block = eskimo.Lock(obj, shared=True)
        # shared lock does not block on shared lock
        with eskimo.Lock(obj, shared=True):
            block.acquire()
        written = [0]
        read = []
        def writer(val):
            with eskimo.Lock(obj):
                written.append(val)
        for val in 1, 2:
            t = threading.Thread(target=writer, args=(val,))
            t.daemon = True
            t.start()
            time.sleep(0.1)
        def reader():
            with eskimo.Lock(obj, shared=True):
                read.append(written[-1])
        num_readers = 20
        for _ in range(num_readers):
            t = threading.Thread(target=reader)
            t.daemon = True
            t.start()
        time.sleep(0.1)
        # exclusive lock blocks on shared lock
        self.assertEqual(written, [0])
        block.release()
        time.sleep(0.1)
        # writer(1) was the first writer to block on obj while a reader
        # held it, so it takes priority.
        self.assertEqual(written, [0, 1, 2])
        # However, writer(2) and the readers were blocked on obj while a
        # writer held it, so the order in which they unblock is undefined,
        # so read = [1]*m + [2]*n for some (m, n), m + n = num_readers
        self.assertEqual(read, sorted(read))
        self.assertTrue(set(read).issubset([1, 2]))
        self.assertEqual(len(read), num_readers)

    def test_shared_priority_multiple(self):
        """Shared eskimo with simultaneous eskimo of multiple objects.
        """
        # This illustrates the priority of the first writer to seek a
        # lock on an object when blocked on a reader, EVEN OVER a later
        # writer which would have been able to lock a subset of the
        # first writer's objects.
        # This also shows a reader (s3) taking priority over a writer (x23)
        # where that writer is blocked, but not blocked on a reader.
        # Such cases result from preferring simplicity of implementation:
        # no attempt was made to establish a comprehensive convention
        # for priority; they are not regarded as bugs, but are subject to
        # future change.
        locks = {'s1': eskimo.Lock(1, shared=True),
                'x12': eskimo.Lock(1, 2),
                'x2': eskimo.Lock(2),
                'x23': eskimo.Lock(2, 3),
                's3': eskimo.Lock(3, shared=True)}
        rx = []
        def tx_with_lock(name):
            rx.append('<' + name)
            with locks[name]:
                rx.append(name + '>')
        locks['s1'].acquire()
        threads = []
        for name in 'x12', 'x2', 'x23', 's3':
            t = threading.Thread(target=tx_with_lock, args=(name,))
            t.daemon = True
            t.start()
            time.sleep(0.1)
            threads.append(t)
        locks['s1'].release()
        for t in threads: t.join(0.1)
        self.assertEqual(rx[:5], ['<x12', '<x2', '<x23', '<s3', 's3>'])
        self.assertEqual(rx[5], 'x12>')
        # when x12 releases 2, x2 and x23 may run in either order
        self.assertEqual(sorted(rx[6:]), ['x23>', 'x2>'])

    def test_shared_timeout_time(self):
        # Timeout is the total time allowed, even where blocked successively
        # on a writer and then on a reader (white box test).
        xlock = eskimo.Lock('x', shared=False)
        slock = eskimo.Lock('s', shared=True)
        xlock.acquire()
        slock.acquire()
        tlock = eskimo.Lock('x', 's')
        t = threading.Timer(0.2, lambda: xlock.release())
        t.start()
        start = time.time()
        self.assertRaises(eskimo.Timeout, tlock.timeout(0.4).acquire)
        waited = time.time() - start
        self.assertTrue(0.4 <= waited < 0.5)

    def test_return_value_and_timeout(self):
        """acquire() returns the time remaining;
        value of timeout relevant only on beskimo.
        """
        # aggregate timer for locks serially acquired, no beskimo
        # (a) timer = None (no timeout) (b) timer = 0, (c) timer > 0
        # (d) timer < 0: timeout ignored when beskimo not required
        for shared in (False, True):
            locks = [eskimo.Lock(i, shared=shared) for i in range(100)]
            for timeout in (None, 0, 1, -1):
                countdown = timeout
                for lock in locks:
                    countdown = lock.timeout(countdown).acquire()
                    lock.release()
                self.assertEqual(countdown, timeout)

        # aggregate timer for locks serially acquired, beskimo
        # (a) timeout None (b) timeout > 0, success
        for timeout, test in ((None, lambda x: x is None),
                (0.5, lambda x: 0 < x < 0.1)):
            objs = 'eth0', 'eth1'
            for i, obj in enumerate(objs):
                lock = eskimo.Lock(obj)
                lock.acquire()
                t = threading.Timer(0.2 * (i+1), lambda x: x.release(),
                        args=(lock,))
                t.start()
            remaining = timeout
            # return value via a 'with' statement
            with eskimo.Lock(objs[0], timeout=remaining) as remaining:
                for obj in objs[1:]:
                    lock = eskimo.Lock(obj)
                    remaining = lock.timeout(remaining).acquire()
                    lock.release()
            self.assertTrue(test(remaining))

        # beskimo required (failure)
        # (a) timeout < 0  (b) timeout = 0
        for timeout in (-1, 0):
            lock = eskimo.Lock('gate')
            lock.acquire()
            t = threading.Timer(0.2, lambda x: x.release(), args=(lock,))
            t.start()
            self.assertRaises(eskimo.Timeout, lock.timeout(timeout).acquire)
            t.join()


if __name__ == "__main__":
    unittest.main()
