# coding: utf-8
from ._base import Registry


class Blackhole(Registry):
    def __init__(self):
        super(Blackhole, self).__init__()

    def register(self, url):
        pass # It just has nothing to register.

    def maintain(self):
        pass # It just has nothing to maintain.


class Notifier(Registry):
    """
    Notifier is a kind of registry, which pushes new url to a queue for later processing.
    The processing is usually done by background daemon who pulls from the same queue and
    registers the url with other registries (such as Analytics, etc).
    """

    def __init__(self, queue):
        super(Notifier, self).__init__()
        self.queue = queue

    def register(self, url):
        self.queue.push(dict(url))

    def maintain(self):
        pass # It just has nothing to maintain.

