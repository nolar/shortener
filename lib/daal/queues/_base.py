# coding: utf-8

class Queue(object):
    """
    Base class for all queue implementations. Just declares a protocol:
    * push(item) should put the item to the queue.
    * pull() should pull one item from the queue for the specified time.
    * delete() should remove the pulled item from the queue.

    If the item has not been deleted after the timeout specified, it should
    reappear to other consumers. It is on the queue's implementation to decide
    how to keep the records on item's timeouts & reservations.

    It is expected that one single queue is operation with the same class in the
    same code in Python, so there are no protocol specification on how items should
    be serialized and deserialized. Though JSON is prefered for heterogenous systems.

    Currently, there is only one implementation: SQSQueue. See there for details.
    """

    def __init__(self):
        super(Queue, self).__init__()

    def push(self, item):
        raise NotImplemented()

    def pull(self, timeout=60, factory=None):
        raise NotImplemented()

    def delete(self, item):
        raise NotImplemented()
