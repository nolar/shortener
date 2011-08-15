

class Counter(object):
    def next(self):
        # repeat:
        # read all stroages in one operation
        #   select random of them
        #   increment it
        #   store it
        # one successful, return the sum of all values (including incremented one)
        # THIS IS JUST A BAD TOOL FOR THIS.
        pass

"""
The only reason for the counter to exists now, is to make a rotating storage of fixed size:
* incr the counter and get it
* store into item_%d

Is there a way to simulate this behavior? (i.e. more or less atomic tail of size N)

The simplest solution:
* Store into one single domain with timestamp added (microseconds as int) - in a queue or immed
* Run a daemon to purge it time to time by timestamp

So, no counter used at all. BUT: in case of a small flow, it will purge too much.
The rough solution is to adapt the timedelta interval by counting how many items are there.
And to store the approximation somewhere, just for fast-start next time.
It has no real effect except for domain cleanup and speedup.
The read is simple: sort by ts desc, limit N.


    TODO: safe way without the batches and heavy centralization:
    TODO: 1. increment central counter
    TODO: 2. Get counter % 100 (or any other MAX_HISTORY)
    TODO: 3. Store the item as item_%d, where %d = counter % 100
    TODO: The only problem is a INCR operation, which is absent in AWS,
    TODO: and if are going to add memcache support - thenits persistence.
"""
