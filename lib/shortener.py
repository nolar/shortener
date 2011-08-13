# coding: utf-8
from .generator import Generator

class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass

class Shortener(object):
    """
    Methods for API.
    """
    
    #!!!TODO: manual debug only
    database = {
        'api': None,
    }
    
    domain = 'helloworld'
    
    def __init__(self):
        super(Shortener, self).__init__()
    
    def shorten(self, url):
        self.generator = Generator()
        
        #!!! the code below should be repeated N times to generate really unique ID
        #!!! the only way to be sure it is unique, is to store and catch a violation
        id = self.generator.generate()
        if id in Shortener.database:
            raise ShortenerIdExistsError("The id requested is used already.")
        else:
            Shortener.database[id] = url
            return 'http://%s/%s' % (self.domain, id)
    
    def resolve(self, id):
        if id in Shortener.database and Shortener.database[id] is not None:
            return Shortener.database[id]
        else:
            raise ShortenerIdAbsentError("Such id does not exists.")
    
    def get_last(self, n):
        raise NotImplemented()
    
    def get_popular(self, td):
        raise NotImplemented()

