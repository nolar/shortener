# coding: utf-8
from .generators import Generator, CentralizedGenerator, FakeGenerator
from .storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from .storages import WrappedStorage, SdbStorage
from .queues import SQSQueue
import datetime
import random
import re

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
    
    def __init__(self, host, sequences, urls, shortened_queue, last_urls_stats, top_domains_stats):
        super(Shortener, self).__init__()
        self.sequences = sequences
        self.urls = urls
        self.host = host
        self.shortened_queue = shortened_queue
        self.last_urls_stats  = last_urls_stats
        self.top_domains_stats = top_domains_stats
        
        # Since shorteners for different hosts are isolated, wrap all the storages with hostname prefix.
        #!!! be sure that host is NORMALIZED, i.e. "go.to:80"  === "go.to", to avoid unwatned errors.
        #!!! probably, check with the list of available host domains in the config db.
        if self.host:
            self.sequences = WrappedStorage(self.sequences, prefix=self.host+'_')
            self.urls      = WrappedStorage(self.urls     , prefix=self.host+'_')
            self.last_urls_stats = WrappedStorage(self.last_urls_stats, prefix=self.host+'_')
            self.top_domains_stats  = WrappedStorage(self.top_domains_stats, prefix=self.host+'_')
        
        self.generator = CentralizedGenerator(sequences)
    
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
        
        def try_create():
            # Usual scenario: generate the id, try to store the item.
            id = id_wanted or self.generator.generate()
            #if re.match(r'(^v\d+/) | (^/)', id, re.I | re.X):
            #    #!!!FIXME
            #    #??? "v"-starting sequences can be very long, we cannot just re-generate them here,
            #    #??? we must ask the generator not to try to create them, and fail in case of id_wanted.
            #    raise StorageExpectationError() # just to jump to the exception handler below
            
            item = {
                'url': url,
                'create_ts': datetime.datetime.utcnow(),
                'remote_addr': remote_addr,
                'remote_port': remote_port,
            }
            
            self.urls.store(id, item, unique=True)
            
            return id, item
        
        # If we wanted to create the url with the very specific id, we cannot recover from this error.
        id, item = self.urls.repeat(try_create, retries=retries if not id_wanted else 1,
                                    exception=lambda e: ShortenerIdExistsError("This id exists already, try another one.") if id_wanted else None)
        
        # Build the resulting url with the host requested and id generated.
        shortened_url = ShortenedURL(self.host, id)#!!! add other info here from item
        
        # Notify the daemons that new url has born. Let them torture it a bit.
        # They update the "last urls" and "top domains" structures, in particular.
        # We do not do the updates here in web request, since we do not need the immediate effect.
        #self.shortened_queue.push({'host': shortened_url.host, 'id': shortened_url.id})
        self.update_stats(shortened_url)  # <-- in case of immediate action
        
        return shortened_url 
    
    def update_stats(self, shortened_url):
        self.update_last_urls(shortened_url)
        self.update_top_domains(shortened_url)
    
    def update_last_urls(self, shortened_url):
        """
        Updates the records for the last_urls statistics by adding the specified
        shortened url there.
        
        WARNING:
        This routine works with centralized data items using locks or conditional
        writes, which can and will cause the slowdowns and bottlenecks under load.
        It should be called from background daemons only, not from the web requests.
        """
        
        # Implementation details:
        # Since we store the list of last urls in batches of fixed size,
        # and a pointer to the last batch, so to add an item to the stats we need
        # to find that last batch and append the item to it. In case the batch
        # is fulfilled (size limit is reached), move the pointer to the next empty
        # batch.
        #!!!TODO: Why the batches? This is not a timeline-based algorythm,
        #!!!TODO: we can store them sequentally one-by-one, as if batch_size=1.
        #!!!TODO: And in this case it is just the centralized generator, so on.
        #!!!TODO: The whole solution looks not very good.
        
        bunch_size = 3
        def try_append():
            #!!! Here the tricky thing is: we have two items to write here: pointer ^ bunch#N.
            #!!! And we cannot do this atomically, so we emulate it... HOW? Is it reliable?
            
            try:
                pointer = self.last_urls_stats.fetch('pointer')
            except StorageItemAbsentError, e:
                pointer = {'last_bunch': 0}
            
            last_bunch = int(pointer['last_bunch'])
            try:
                bunch = self.last_urls_stats.fetch('bunch_%s' % last_bunch)
            except StorageItemAbsentError, e:
                bunch = {'items': ''}
            
            old_items = bunch['items']
            items = filter(None, bunch['items'].split(':::'))
            items.append(shortened_url.url)
            
            new_items = ':::'.join(items)
            bunch['items'] = new_items
            self.last_urls_stats.store('bunch_%s' % last_bunch, bunch, expect={'items':old_items or False})
            
            if len(items) >= bunch_size:
                next_bunch = last_bunch + 1
                self.last_urls_stats.store('pointer', {'last_bunch': next_bunch}, expect={'last_bunch':last_bunch or False})
        
        self.last_urls_stats.repeat(try_append, retries=5)
    
    def get_last_urls(self, n):
        """
        Returns the last N urls added for this specific shortener host domain.
        There is not statistics for global shortener urls, but it can be added
        easily (just remove the wrapping aroung last_urls storage).
        """
        
        # Implementation details:
        # Since we store all the urls added in separate batches of fixed size,
        # and shift a pointer to the last batch, so to get N last urls we need
        # to read this pointer, and then to read fixed amount of batches from
        # the last bunch backward (can be done in one single storage request).
        # So we have only two(!) reads for the whole request: pointer & batches.
        
        # Read the pointers to the latest batch and the latest item. If the list
        # will change between we have read the pointers and the batches, we will
        # ignore those changes, since we have the very precise pointer to the item.
        try:
            pointer = self.last_urls_stats.fetch('pointer')
            last_bunch = int(pointer.get('last_bunch', 0))
            last_count = int(pointer.get('last_count', 0))#??? ignore? is it needed?
        except StorageItemAbsentError, e:
            last_bunch = 0
            last_count = 0#???? ignored? is it needed?
        
        # Calculate amount and indexes of the bunches we are going to read.
        bunch_size = 3
        bunch_count = (n / bunch_size) + (1 if n % bunch_size else 0) + 1
        first_bunch = max(0, last_bunch - bunch_count + 1)
        
        # Read the bunches with multi-get operation of the storage.
        bunch_ids = ['bunch_%s' % i for i in range(first_bunch, last_bunch+1)]
        bunches = self.last_urls_stats.fetch(bunch_ids)
        
        # Merge the urls into one single list, then cut extra items.
        last_items = []
        for bunch in bunches:
            bunch_items = bunch['items'].split(':::')
            last_items.extend(bunch_items)#!!! check for propert order/sorting
        last_items = last_items[-n:]
        return last_items
    
    def update_top_domains(self, shortened_url):
        # update the "top domains" lists (using restored target url)
        #!!!
        pass
    
    def get_top_domains(self, timedelta):
        raise NotImplemented()


class AWSShortener(Shortener):
    def __init__(self, access_key, secret_key, host):
        super(AWSShortener, self).__init__(host,
            sequences   = SdbStorage(access_key, secret_key, 'sequences' ),
            #generators = SdbStorage(access_key, secret_key, 'generators'),
            urls        = SdbStorage(access_key, secret_key, 'urls'      ),
            shortened_queue = SQSQueue(access_key, secret_key, name='urls'),
            last_urls_stats = SdbStorage(access_key, secret_key, 'last_urls'),
            top_domains_stats = SdbStorage(access_key, secret_key, 'top_domains'),
            )
