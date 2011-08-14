# coding: utf-8
from .generators import Generator, CentralizedGenerator, FakeGenerator
from .storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from .storages import WrappedStorage, SdbStorage
from .queues import SQSQueue
import datetime
import random

class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass


class ShortenedURL(object):
    def __init__(self, host, id):
        super(ShortenedURL, self).__init__()
        self.host = host
        self.id = id
    
    def __str__(self):
        return self.url
    
    def __unicode__(self):
        return self.url
    
    @property
    def url(self):
        return 'http://%s/%s' % (self.host, self.id)


class Shortener(object):
    """
    Methods for API.
    """
    
    def __init__(self, host, sequences, urls, shortened_queue):
        #!!! add: last_urls_storage
        #!!! add: top_domains_storage
        
        super(Shortener, self).__init__()
        self.sequences = sequences
        self.urls = urls
        self.host = host
        self.shortened_queue = shortened_queue
        
        # Since shorteners for different hosts are isolated, wrap all the storages with hostname prefix.
        #!!! be sure that host is NORMALIZED, i.e. "go.to:80"  === "go.to", to avoid unwatned errors.
        #!!! probably, check with the list of available host domains in the config db.
        if self.host:
            self.sequences = WrappedStorage(self.sequences, prefix=self.host+'_')
            self.urls      = WrappedStorage(self.urls     , prefix=self.host+'_')
        
        self.generator = generator=CentralizedGenerator(sequences)
    
    def resolve(self, id):
        try:
            item = self.urls.fetch(id)
            #todo later: we can add checks for moderation status here, etc.
            return item['url']
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
        while retries > 0:
            try:
                # Usual scenario: generate the id, try to store the item.
                id = id_wanted or self.generator.generate()
                if re.match(r'(^v\d+/) | (^/)', id, re.I | re.X):
                    #!!!FIXME
                    #??? "v"-starting sequences can be very long, we cannot just re-generate them here,
                    #??? we must ask the generator not to try to create them, and fail in case of id_wanted.
                    raise StorageExpectationError() # just to jump to the exception handler below
                
                self.urls.store(id, {
                    'url': url,
                    'create_ts': datetime.datetime.utcnow(),
                    'remote_addr': remote_addr,
                    'remote_port': remote_port,
                }, unique=True)
                
                # Notify the daemons that new url has born. Let them torture it a bit.
                # They update the "last urls" and "top domains" structures, in particular.
                # We do not do the updates here in web request, since we do not need the immediate effect.
                self.shortened_queue.push({'host': self.host, 'id': id})
                
                # Build the resulting url with the host requested and id generated.
                return ShortenedURL(self.host, id)
                
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


class AWSShortener(Shortener):
    def __init__(self, access_key, secret_key, host):
        super(AWSShortener, self).__init__(host,
            sequences   = SdbStorage(access_key, secret_key, 'sequences' ),
            #generators = SdbStorage(access_key, secret_key, 'generators'),
            urls        = SdbStorage(access_key, secret_key, 'urls'      ),
            shortened_queue = SQSQueue(access_key, secret_key, name='urls'),
            )
