# coding: utf-8
from .generator import Generator
from .storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
import datetime
import random

class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass

class Shortener(object):
    """
    Methods for API.
    """
    
    def __init__(self, daal, host, generator):
        super(Shortener, self).__init__()
        self.generator = generator
        self.host = host
        self.daal = daal
    
    def resolve(self, id):
        try:
            item = self.daal.urls.fetch(id)
            #todo later: we can add checks for moderation status here, etc.
            return item['url']
        except StorageItemAbsentError, e:
            raise ShortenerIdAbsentError("Such url does not exist.")
    
    def shorten(self, url, id_wanted=None, retries=10, remote_addr=None, remote_port=None):
        """
        Shortens the url and saves it with the id requested or generated.
        
        In case if the id is generated and happens to exist already, this method
        tries to generate it few more times. This is needed to avoid collisions
        of manually requested ids with automatically generated ids, since this
        cannot be done by the generator itself (it should not know the purpose
        of the ids, nor if some of the ids are reserved or used explicitly).
        """
        
        while retries > 0:
            # This code will be executed until the first success or retries will expire.
            try:
                id = id_wanted or self.generator.generate()
                self.daal.urls.store(id, {
                    'url': url,
                    'create_ts': datetime.datetime.utcnow(),
                    'remote_addr': remote_addr,
                    'remote_port': remote_port,
                }, unique=True)
                ##!!!!FIXME: url pattern should be generated or specified somewhere else
                return 'http://%s/%s' % (self.host, id)
            
            # We handle only the "id exists" error here, to try few more times before failing.
            except StorageExpectationError, e:
                
                # If we wanted to create the url with the very specific id, we cannot recover from this error.
                if id_wanted:
                    raise ShortenerIdExistsError("This id exists already, try another one.")
                
                # If we cannot save the item for too much tries, there is something wrong with the storage itself.
                retries -= 1
                if retries == 0:
                    raise e
    
    def get_last_urls(self, n):
        raise NotImplemented()
    
    def get_top_domains(self, td):
        raise NotImplemented()

