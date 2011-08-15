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
    """
    This analytics dimension collects the information on the latest items (urls)
    added to the system, and provides the most recent N of them.
    """
    
    def update(self, shortened_url):
        """
        Updates the records for the last_urls analytics by adding the specified
        shortened url there.
        
        WARNING:
        This routine works with centralized data items using locks or conditional
        writes, which can and will cause the slowdowns and bottlenecks under load.
        It should be called from background daemons only, not from the web requests.
        """
        
        timestamp = int(time.time() * 1000000)
        item = dict(shortened_url)
        item['timestamp'] = timestamp
        key = 'item_%s_%s' % (shortened_url.host, shortened_url.id) # does no matter actually, but must be unique
        self.storage.store(key, item)
    
    def retrieve(self, n):
        """
        Returns the last N urls added for this specific shortener host domain.
        There is not analytics for global shortener urls, but it can be added
        easily (just remove the wrapping aroung last_urls storage).
        """
        items = self.storage.query(where="timestamp > ''", order="timestamp desc", limit=n)
        return items[:n] #TODO: return as list of URL instances?
    
    def maintain(self):
        #!!!todo: purge old items, determine the best timedelta to keep MAX_N items at least
        #!!!todo: store the last value of determined timedelta for further fast re-calcs.
        pass

