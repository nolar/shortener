# coding: utf-8


class Analytics(object):
    def __init__(self, **kwargs):
        super(Analytics, self).__init__()
        self.__dict__.update(kwargs)
        self.stats = kwargs.values()
    
    def update(self, shortened_url):
        for stat in self.stats:
            stat.update(shortened_url)
