# coding: utf-8
from ._base import Dimension
import datetime
import time
import urlparse
import functools


__all__ = ['PopularDomainsDimension']


class PopularDomainsDimension(Dimension):
    """
    Analytics dimension for gathering the top domains by the number of URLs.
    Here, domain is a hostname part of the URL shortened. Information is filtered
    by the maximum number of domains (top 10, top 100, etc), and by the age of
    the URLS (last day, last month, etc). Age lowest precision is specified as
    a class constant; changing it can affect or reset all the statistics gathered.
    
    The information is storage in pieces with itemname "timeslice_%s", where "%s"
    is a timestamp rounded to the nearest granularity border, so all the records
    made in that peroid of time are written to the same item. The item itself is
    a dictionary, where keys are normalized domain names and values are counters.
    
    When the information is retrieved, we fetch fixed amount of items (from now
    to the past: the lower the granularity, the more items must be fetched.).
    Fetched items are merged (summed) into one single dictionary then. This
    dictionary is sorted and returned.
    
    When time goes, old items are not used anymore and can be deleted (or ignored).
    """
    
    AGE_PRECISION = datetime.timedelta(seconds=12*60*60) # 12 hrs
    
    def register(self, shortened_url):
        #TODO: exclude port information from the hostname, if specified.
        #TODO: normalize domain, remove "www" prefix, strip, lowercase.
        
        # Precalculate
        age_precision = self.AGE_PRECISION
        slicesize = int(age_precision.days*24*60*60 + age_precision.seconds)
        domain = urlparse.urlparse(shortened_url.url).hostname
        timeslice = int(shortened_url.created_ts / slicesize) * slicesize
        slice_key = 'timeslice_%s' % (timeslice)
        
        def increment(domain, data):
            return {domain: int(data.get(domain, 0)) + 1}
        self.storage.update(slice_key, functools.partial(increment, domain),
            field=domain,#!!! remove this, we should not care.
            retries=3,
            )
    
    def retrieve(self, n, timedelta):
        
        # Precalculate the ids of the slices to be fetched.
        age_precision = self.AGE_PRECISION
        slicesize = int(age_precision.days*24*60*60 + age_precision.seconds)
        slice_now = int(time.time() / slicesize) * slicesize
        seconds_to_past = int(timedelta.days * 24*60*60 + timedelta.seconds)
        slice_past = int((time.time() - seconds_to_past) / slicesize) * slicesize
        slice_ids = ['timeslice_%s' % timeslice for timeslice in xrange(slice_past, slice_now+1, slicesize)]
        
        # Fetch them, those items available; ignore unavailable.
        #FIXME: this will eat all our memory on a loaded system.
        #IDEA: calculate in the background too, store a grid of results for N=3-5-10-20-30-50-100, days=1-2-3-7-10-14-30
        #IDEA: isn't it map-reduce job after all?
        slices = self.storage.mfetch(slice_ids)
        
        # Combine all the slices to one single dictionary.
        combined = {}
        for slice in slices:
            for domain, count in slice.items():
                if domain != 'id': #!!! that was bad idea to store some unexpected values implicitly
                    combined[domain] = combined.get(domain, 0) + int(count)
        
        # Get top N domains from the combined dictionary.
        #!!!TODO: the algorythm could be merged with the combine cycle, to drop low values on the go.
        #!!!FIXME: with hundreds and thousands of domains per month, this sort will be hard and heavy.
        flat = combined.items()
        flat.sort(lambda a,b: cmp(a[1], b[1]), reverse=True)
        tops = flat[:n]
        tops = [dict(domain=domain, count=count) for domain, count in tops]
        return tops
    
    def maintain(self):
        #TODO: delete old timeslices
        pass

