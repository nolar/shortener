# coding: utf-8

class Dimension(object):
    """
    Base class for analytics dimensions. Just introduces empty protocol.
    """
    
    def __init__(self, storage):
        super(Dimension, self).__init__()
        self.storage = storage
    
    def register(self, shortened):
        """
        Updates the dimension with new shortened URL instance.
        Usually performed in daemon mode, so it can be long an heavy.
        """
        raise NotImplemented()
    
    def retrieve(self, *args, **kwargs):
        """
        Retrieves the dimension information according to the parameters.
        Usually performed in web request context, so it should be fast.
        
        Set of parameters can be different, since no one call for abstract
        dimension; i.e., if someone call for the dimension info, one calls
        for very specific dimension and one knows what parameters it accepts.
        """
        raise NotImplemented()
    
    def maintain(self):
        """
        Maintains and organizes the information gathered.
        Usually performed in daemon mode or in cron tasks.
        """
        raise NotImplemented()
