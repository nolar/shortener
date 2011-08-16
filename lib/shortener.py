# coding: utf-8
from .daal.storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from .generators import Generator, CentralizedGenerator, FakeGenerator
from .url import URL
import time
import re

class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass
class ShortenerBadUrlError(Exception): pass

class Shortener(object):
    """
    API methods to operate on the URLs: shorten long urls and resolve short ones.
    All analytical operations are in Analytics & Dimension classes & descendants.
    """
    
    def __init__(self, host, sequences, urls, shortened_queue, analytics):
        super(Shortener, self).__init__()
        self.sequences = sequences
        self.urls = urls
        self.host = host
        self.shortened_queue = shortened_queue
        self.analytics = analytics
        self.generator = CentralizedGenerator(sequences, prohibit=r'(^v\d+/) | (^/) | (//)')
        #!!!FIXME: this prohibition approach is not good, because you will stuch at "v0..." block,
        #!!!FIXME: since it is VERY LARGE and you'll iterate over all of it.
        #???TODO: possible solution is to prhibit some beginnings and endings only (v\d+/  & .html/.json)
        #???TODO: and also some sequences in the middle (//) -- this can be controlled _inside_ the
        #???TODO: Sequence recursion (i.e. not only on the resulting string) and be fastened easyly (probably).
        #???TODO: SequenceRules class with check_start, check_end, check_middle methods?
    
    def resolve(self, id):
        """
        Resolves the short id. If you want to resolve the url, you should
        first parse it into a host & id parts, and the ask proper shortener
        instance (specific for each host) to resolve that id.
        
        Returns an URL instance with all fields fulfilled.
        """
        
        try:
            item = self.urls.fetch(id)
            #todo later: we can add checks for moderation status here, etc.
            return URL(**item)
        except StorageItemAbsentError, e:
            raise ShortenerIdAbsentError("Such url does not exist.")
    
    def shorten(self, url, id_wanted=None, retries=10, remote_addr=None, remote_port=None):
        """
        Shortens the long url and saves it with the id requested or generated.
        
        Returns an URL instance with all fields fulfilled as if it has been resolved.
        """
        
        #??? do we care? maybe that is a view's responsibility to validate the input?
        if '://' not in url or len(url) > 8*1024:
            raise ShortenerBadUrlError("URL is not an URL?")
        
        # Despite that generator guaranties unique ids within that generator,
        # there could be other urls stored already with this id. For example,
        # if the url was stored with the manually specified id earlier; or if
        # there was another generator algorythm before. The only way to catch
        # these conflicts is to try to store, and see if that was successful
        # (note that the generators should not know the purpose of the id and
        # cannot check for uniqueness by themselves; that would not help, btw).
        def try_create():
            # Usual scenario: generate the id, then try to store the item.
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
        
        # Notify the analytics that a new url has been born. Let them torture it a bit.
        # They update the "last urls" and "top domains" structures, in particular.
        # We do not do the updates here in web request, since we do not need the immediate effect.
        self.shortened_queue.push(dict(shortened_url))
        #self.analytics.update(shortened_url)
        
        return shortened_url 

