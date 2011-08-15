# coding: utf-8
from ..daal.storages import StorageUniquenessError, StorageItemAbsentError, StorageExpectationError
from ._base import Dimension
import datetime
import time
import random
import re
import urlparse

__all__ = ['RecentTargetsDimension']


class RecentTargetsDimension(Dimension):
    def update(self, shortened_url):
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
                pointer = self.storage.fetch('pointer')
            except StorageItemAbsentError, e:
                pointer = {'last_bunch': 0}
            
            last_bunch = int(pointer['last_bunch'])
            try:
                bunch = self.storage.fetch('bunch_%s' % last_bunch)
            except StorageItemAbsentError, e:
                bunch = {'items': ''}
            
            old_items = bunch['items']
            items = filter(None, bunch['items'].split(':::'))
            items.append(shortened_url.shortcut)
            
            new_items = ':::'.join(items)
            bunch['items'] = new_items
            self.storage.store('bunch_%s' % last_bunch, bunch, expect={'items':old_items or False})
            
            if len(items) >= bunch_size:
                next_bunch = last_bunch + 1
                self.storage.store('pointer', {'last_bunch': next_bunch}, expect={'last_bunch':last_bunch or False})
        
        self.storage.repeat(try_append, retries=5)
    
    def retrieve(self, n):
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
            pointer = self.storage.fetch('pointer')
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
        bunches = self.storage.fetch(bunch_ids)
        
        # Merge the urls into one single list, then cut extra items.
        last_items = []
        for bunch in bunches:
            bunch_items = bunch['items'].split(':::')
            last_items.extend(bunch_items)#!!! check for propert order/sorting
        last_items = last_items[-n:]
        return last_items


