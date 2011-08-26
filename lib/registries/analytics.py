# coding: utf-8
from ._base import Registry

class Analytics(Registry):
    """
    Analytics is a kind of registry, which pushes new url to analytical dimensions it contain.
    Each dimension is stored with a specific key in this container and is accessed by this key
    as a field name (analytics_instance.dimension_name). The same dimension can be stored in
    multiple analytics containers, though this makes no much sense.
    """

    def __init__(self, **kwargs):
        super(Analytics, self).__init__()
        self.__dict__.update(kwargs)
        self.dimensions = kwargs.values()

    def register(self, url):
        for dimension in self.dimensions:
            dimension.register(url)

    def maintain(self):
        for dimension in self.dimensions:
            dimension.maintain()
