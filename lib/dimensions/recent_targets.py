# coding: utf-8
from ..url import URL
from ._base import Dimension
import time


__all__ = ['RecentTargetsDimension']


class RecentTargetsDimension(Dimension):
    """
    Analytics dimension for gathering the most recent items (urls) added.
    
    The information is stored as a flat log of items, where each item is marked
    with timestamp when it was created (up to microseconds). The timestamp is
    used for sorting the items before retrieving N most recent of them.
    
    Also, the timestamp is used in the ids of the items in the log, though this
    is not required. It is just a good start for unique item id for the storage
    across all the processes, and the items are never referred by these log ids.
    
    Over the time, maintainance daemon removes the old items from this log to
    keep the queries fast.
    """
    
    def register(self, shortened_url):
        """
        Updates the records for the recent targets analytics by adding the
        specified shortened url.
        """

        def gen_item():
            timestamp = int(time.time() * 1000000)
            item = dict(shortened_url)
            item['timestamp'] = timestamp
            return item
        self.storage.create(gen_item)
    
    def retrieve(self, n):
        """
        Returns the last N urls added for this specific shortener host domain.
        There is not analytics for global shortener urls, but it can be added
        easily (just remove the wrapping aroung last_urls storage).
        """
        items = self.storage.select(sorters=[('timestamp', True)], limit=n)
        items = items[:n]
        #TODO: return as list of URL instances?
        items = [URL(**item) for item in items]
        return items
    
    def maintain(self):
        #!!!todo: purge old items, determine the best timedelta to keep MAX_N items at least
        #!!!todo: store the last value of determined timedelta for further fast re-calcs.
        pass

