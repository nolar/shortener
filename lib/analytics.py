# coding: utf-8


class Analytics(object):
    """
    Analytics class is a container for the dimensions (see "dimensions" package).
    Each dimension is stored with a specific key in this container and is accessed
    by this key as a field name (analytics_instance.dimension_name). The same
    dimension can be stored in multiple analytics container, though this makes
    no much sense.
    """
    
    def __init__(self, **kwargs):
        super(Analytics, self).__init__()
        self.__dict__.update(kwargs)
        self.stats = kwargs.values()
    
    def update(self, shortened_url):
        for stat in self.stats:
            stat.update(shortened_url)
    
    def maintain(self):
        for stat in self.stats:
            stat.maintain()
