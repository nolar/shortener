# coding: utf-8

class Registry(object):
    """
    Base class for URL registries. URL registry is something that accepts just
    created URL and registers it somehow somewhere for some purpose. Actually,
    it is just an interface and protocol specification for such classes.
    """
    
    def register(self, url):
        raise NotImplementedError()

    def maintain(self):
        raise NotImplementedError()
