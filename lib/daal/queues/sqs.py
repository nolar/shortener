# coding: utf-8
from ._base import Queue
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import json


__all__ = ['SQSQueue']


class SQSItem(dict):
    """
    Utilitary class used in SQSQueue.pull() operation as default factory.
    This is just a dict object with ability to add additional attributes.
    There is no any reason to use this class explicitly, no matter what for.

    The reason for this is inability of built-in python types to hold additional
    synamic attributes, which are used in pull() as a way to refer to the message
    and the queue for further delete() operation. Using queue's own mapping is
    highly unwanted for the case these items are not deleted (or deleted by other
    processes), thus leading to memory leakage.
    """
    pass


class SQSQueue(Queue):
    """
    Amazon SQS queue.

    Implementation details on pull() and delete() methods:
    We store additional information, that is neede to address the items,
    within the items themselves. This is a hack, but it allows us not to
    store this information in queue's own buffer, so causing memory leaks
    when the messages are never deleted. In this case we just do not store
    any reference to an items, so it will be garbage collected when forgot.
    """

    def __init__(self, access_key, secret_key, region=None, name=None):
        super(SQSQueue, self).__init__()
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.name = name
        self.connected = False
        self._messages = {}

    def push(self, item):
        """
        Pushes an item to the queue. Item can be of any type, that can be JSON-encoded:
        strings, ints, lists, dicts, etc. Though dict-based type is expected to work
        with pull() & delete() operations later.
        """

        # First, extract the data to be stored and serialize them.
        body = json.dumps(item)

        # Prepare the queue-specific message instance.
        message = Message()
        message.set_body(body)

        # Connect and push the message.
        self._connect()
        self.queue.write(message)

    def pull(self, timeout=60, factory=SQSItem):
        """
        Pulls an item from the queue. If there is nothing in the queue to pull,
        returns None (unfortunately, there is no way to implement blocking pull
        on a HTTP-based queue in adequate way).

        Timeout specifies for how long this item will be reserved for processing.
        A consumer of the queue must delete() the item before that time, or the
        item will reappear on the queue for other consumers after this timeout.

        If there are data pulled, calls the factory and returns its result. The
        result of the factory is expected to be either None (thus, behaving as
        if there were no data in the queue) or any object instance. The factory
        can also be a class if its constructor can accept the dict of data as its
        first argument. By default, SQSItem is used (dict descendant class).

        FIXME: try not to use SQSItem and return the result as-is, but there should
        FIXME: be a way to associate the item with the message for futher deletion.
        """
        self._connect()
        message = self.queue.read(timeout)
        if message is not None:
            body = message.get_body()
            data = json.loads(body)
            item = factory(data)

            # Mark for futher deletion, but do not refer to it from ouselves (for garbage collectors).
            if item is not None:
                item.__dict__['_queue_'] = self.queue
                item.__dict__['_message_'] = message

            return item
        else:
            return None

    def delete(self, item):
        """
        Deletes the item from the queue, so it would not reappear again after
        the timeout (specified in the pull operation) will expire.
        """

        # Exract the information on the queue and message as when the item was pulled.
        # Those references are highly required to manage the queue consistently.
        queue = item.__dict__.get('_queue_', None)
        message = item.__dict__.get('_message_', None)
        if queue is None:
            raise Exception("Cannot delete an item, since it is not from a queue.")
        if queue is not self.queue:
            raise Exception("Cannot delete an item, since it is not from my queue.")

        # Connect and delete the message of the item (note: not the item itself).
        self._connect()
        self.queue.delete_message(message)

    def _connect(self):
        """
        Connects to the queue. If already connected, does nothing.
        """
        if not self.connected:
            self.connection = SQSConnection(self.access_key, self.secret_key, region=self.region)
            self.queue = self.connection.create_queue(self.name)
            self.connected = True
        return self

