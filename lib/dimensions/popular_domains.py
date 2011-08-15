# coding: utf-8
from ..daal.storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from ._base import Dimension
import datetime
import time
import random
import re
import urlparse


__all__ = ['PopularDomainsDimension']


class PopularDomainsDimension(Dimension):
    def update(self, shortened_url):
        # update the "top domains" lists (using restored target url)
        #!!!
        
        domain = urlparse.urlparse(shortened_url.url).hostname#!!! catch parsing errors?
        slicesize = 12*60*60 # 12hr (maybe try 2d?)
        timeslice = int(shortened_url.created_ts / slicesize) * slicesize
        slice_key = 'timeslice_%s' % (timeslice)
        
        def try_append():
            try:
                slice = self.storage.fetch(slice_key)
            except StorageItemAbsentError, e:
                slice = {}
            
            old_count = int(slice.get(domain, 0))
            slice[domain] = old_count + 1
            
            self.storage.store(slice_key, slice, expect={domain:old_count or False})
            
        self.storage.repeat(try_append, retries=3)
    
    def retrieve(self, n, timedelta):
        
        slicesize = 12*60*60 # 12hr (maybe try 2d?)
        slice_now = int(time.time() / slicesize) * slicesize
        seconds_to_past = int(timedelta.days * 24*60*60 + timedelta.seconds)
        slice_past = int((time.time() - seconds_to_past) / slicesize) * slicesize
        
        slice_ids = ['timeslice_%s' % timeslice for timeslice in xrange(slice_past, slice_now+1, slicesize)]
        
        slices = self.storage.fetch(slice_ids)
        
        # Combine all the slices to one single dictionary.
        combined = {}
        for slice in slices:
            for domain, count in slice.items():
                if domain != 'id': #!!! bad idea to store some unexpected values
                    combined[domain] = combined.get(domain, 0) + int(count)
        
        # Get top N domains from the combine dictionary.
        #!!!TODO: the algorythm could be merged with the combine cycle, to drop low values on the go.
        #!!!FIXME: with hunders and thousands of domains this sort will be hard to do.
        flat = combined.items()
        flat.sort(lambda a,b: cmp(a[1], b[1]), reverse=True)
        tops = flat[:n]
        tops = [dict(domain=domain, count=count) for domain, count in tops]
        return tops
