# eskimo_lock
Alternative approach to locking in Python, supporting shared-exclusive (reader-writer) locks, the simultaneous locking of multiple objects, and timeouts.

In general terms, the motivation for this code was the need in threaded code to prevent concurrent manipulation of arbitrary resources not known in advance, based on their identifiers - the kind of situation where you might be thinking along the lines of a pool of locks, with new locks being created as necessary on demand.
A specific example of the problem is described on [stackoverflow](http://stackoverflow.com/questions/625491/python-conditional-lock):
the need to "prevent any two threads from running the same function with the same argument at the same time".
The code here is based on the answer provided by David Z to that question.

The easiest way to understand the use case may be to look at the link:eskimo_test.py[test code].

See [LICENSE](LICENSE) for copyright and license notice.
Released as open source by authority of William Oppermann, MPSTOR, December, 2014.
