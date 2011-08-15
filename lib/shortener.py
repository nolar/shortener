# coding: utf-8
from .daal.storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from .generators import Generator, CentralizedGenerator, FakeGenerator
from .url import URL
import time
import re

class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass

class Shortener(object):
    """
    Methods for API.
    """
    
    def __init__(self, host, sequences, urls, shortened_queue, statistics):
        super(Shortener, self).__init__()
        self.sequences = sequences
        self.urls = urls
        self.host = host
        self.shortened_queue = shortened_queue
        self.statistics = statistics
        self.generator = CentralizedGenerator(sequences)
    
    def resolve(self, id):
        try:
            item = self.urls.fetch(id)
            #todo later: we can add checks for moderation status here, etc.
            return URL(**item)
        except StorageItemAbsentError, e:
            raise ShortenerIdAbsentError("Such url does not exist.")
    
    def shorten(self, url, id_wanted=None, retries=10, remote_addr=None, remote_port=None):
        """
        Shortens the url and saves it with the id requested or generated.
        Return the absolute url of the new url shortcut.
        """
        
        # Despite that generator guaranties unique ids within that generator,
        # there could be other urls stored already with this id. For example,
        # if the url was stored with the manually specified id earlier; or if
        # there was another generator algorythm before. The only way to catch
        # these conflicts is to try to store, and see if that has succeded
        # (note that the generators should not know the purpose of the id and
        # cannot check for uniqueness by themselves; though that would not help).
        
        def try_create():
            # Usual scenario: generate the id, try to store the item.
            id = id_wanted or self.generator.generate()
            #if re.match(r'(^v\d+/) | (^/)', id, re.I | re.X):
            #    #!!!FIXME
            #    #??? "v"-starting sequences can be very long, we cannot just re-generate them here,
            #    #??? we must ask the generator not to try to create them, and fail in case of id_wanted.
            #    raise StorageExpectationError() # just to jump to the exception handler below
            
            # Build the resulting url with the host requested and id generated.
            shortened_url = URL(host = self.host, id = id, url = url,
                                created_ts = time.time(),
                                remote_addr = remote_addr,
                                remote_port = remote_port,
                                )
            
            # Store an item as a dit of values.
            self.urls.store(id, dict(shortened_url), unique=True)
            
            return shortened_url
        
        # If we wanted to create the url with the very specific id, we cannot recover from this error.
        shortened_url = self.urls.repeat(try_create, retries=retries if not id_wanted else 1,
                                    exception=lambda e: ShortenerIdExistsError("This id exists already, try another one.") if id_wanted else None)
        
        # Notify the stats that a new url has been born. Let them torture it a bit.
        self.update_stats(shortened_url)
        
        return shortened_url 
    
    def update_stats(self, shortened_url):
        # Notify the daemons that new url has born. Let them torture it a bit.
        # They update the "last urls" and "top domains" structures, in particular.
        # We do not do the updates here in web request, since we do not need the immediate effect.
        #self.shortened_queue.push({'host': shortened_url.host, 'id': shortened_url.id})
        self.statistics.update(shortened_url)

