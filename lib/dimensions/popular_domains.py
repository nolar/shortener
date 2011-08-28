# coding: utf-8
"""
Analytical dimension to collect information on popular domains of shortened urls.


== PURPOSE & CONCEPT ==

In a very oversimplified approach, when new url is added, in extracts domain name from
that url (domain name is a hostname without ports and meaningless prefixes like "www"),
and then increments the counter for that domain. When requested to find top N domains,
it sorts all tha domains in reverse order and takes N most popular of them.

The counter for each domain is spread over the timeline, so it is possible to calculate
the count of added urls for specific time period, such as a day, a week, a month or so.


== REQUIREMENTS & LIMITATIONS ==

First, this dimension should be designed to be used in a heavily loaded environment,
when you get up to hundreds and thousands of new urls per second.

Second, it should be designed to work with pure key-value storages with no support
for SQL-like filtering, sorting, grouping or aggregation.

Third, even with key-value concept in mind, it should be designed to minimize the
number of storage operations performed in each call, just to increase overall speed
of the operations and to reduce the load on storages.


== SOLUTIONS & TERMINOLOGY ==

==== Time Shards ===

Please note that in the examples below we use timeline of only one day, even part of it,
and is referred in HH:MM notation. Actually, the timeline is measured as numeric value
and should be considered infinite at least in terms of existence of the machines: thus,
unix timestamp is the best choice for this.

To make counters partially expire over the time, we use time shards (time frames, time slices).
The whole timeline is split into small spans of equal duration. The duration of these spans
determine precision of the whole counting and expiring system. Every shard is identified by
the moment of time when it starts on the timeline.

    --------+-----------+-----------+-----------+-----------+-----------+-----------+------>
    shard=00:00 shard=01:00 shard=02:00 shard=03:00 shard=04:00 shard=05:00 shard=06:00 ...

When a new url is added, its creation time (expressed as a numeric value on the timeline)
is checked for what time shard does it belong, and that shard's identifier is used. E.g.,
if we have time shard with the size of 1hr, all the urls added on 01:00, 01:15, 01:59 go
to the shard "01:00"; all the urls added on 02:00, 02:44, etc - go to the "02:00" shard.

                        item(01:00)->shard(01:00)
                        |  item(01:15)->shard(01:00)
                        |  |       item(01:59)->shard(01:00)
                        |  |       |item(02:00)->shard(02:00)
                        |  |       ||        item(02:44)->shard(02:00)
                        |  |       ||        |
                        |  /       /|        /
    --------+-----------+-----------+-----------+-----------+-----------+-----------+------>
    shard=00:00 shard=01:00 shard=02:00 shard=03:00 shard=04:00 shard=05:00 shard=06:00 ...

When time shard of the url is determined, we calculate a key for that url (domain name),
and increment the value of the counter for that specific shard and that specific key.

When someone asks to retrieve counter value for the last N time units (seconds, hours, etc),
we first calculate current time and time at the beginning of the requested interval.
Then we convert both of the time values to shard identifiers, and build a sequential list
of all shards in between these beginning and ending shards, including the boundary shards.
Then we fetch the counters for each of these time shards (usually in multi-fetch), and
calculate a sum. This sum is a resulting counter value for the requested time interval.

            d2=6        d2=7        d2=absent   d2=8
            d1=2        d1=3        d1=4        d1=5
            |           |           |           |
            |           |           |           |
    --------+-----------+-----------+-----------+-----------+-----------+-----------+------>
    shard=00:00 shard=01:00 shard=02:00 shard=03:00 shard=04:00 shard=05:00 shard=06:00 ...
                        \_______________________/
                            03:13, 2 hrs back => 01:00-03:13 => 2 hr 13 min
            \_______________________/
                02:44, 2 hrs back => 00:00-02:44 => 2 hr 44 min

Lets assume that we have only two domain names to consider (d1, d2) with counters values
as shown, and it is 03:13 now, and we want to get the counters for the past 2 hours.
We first calculate the beginning moment as 03:13-2hr=01:13. Then we convert both of the
moments to the shards identifiers: 01:00 and 03:00. Then we build a list of the shards
in between, inclusively: [01:00, 02:00, 03:00]. Then we fetch the counters from all these
3 shards and summarize them:

    d1=3+4+5=12
    d2=7+8=15

If it were 02:44 now, the values would be:

    d1=2+3+4=9
    d2=6+7=13

Obviously, these counters' precision depends on the duration of the time shards, or,
to be more correct, on the number of time shards in the requested interval.


=== Grid Levels ===

To support pure key-value storages with no enlisting feature (SQL SELECT or alike),
we have to build reversed structure, that can be updated on the go. This structure
is implemented as a grid of levels.

When we add a new url to the dimension, we first increase per-domain counter,
and retrieve the value of this counter. The whole grid system works on this value.
For example, lets assume we have seven domains with counter values as follows:

    d0=5, d1=10, d2=99, d3=50, d4=500, d5=999, d6=1234

The grid consists of fixed amount of levels. Each level has its threshold value.

    L1: threshold=10+
    L2: threshold=100+
    L3: threshold=1000+

Each level has extendible list of domain names and a counter with a number of domains.
Once a domain's counter had been increased and we got its value, this value is checked
against levels' thresholds. If the domain moved to the next level on the grid, we add
the domain to that level. Technically, we append this domain name to the list of domain
names for the level, and increase the value of the counter of domains for the level.

We do not delete the domain name from the previous level, since usually we have no interest
in the lower levels, as it will be shown below; and their exclusion will not save us any space,
since higher levels are usually less populated. More on that, such structure allows us
to simplify few algorithms of a retrieval stage: instead of merging few levels with distinct
domains, we just use one lower level that already contains all higher ones.

For our example, the grid would look like this:

    L1: threshold=10+
        domains: d1, d2, d3, d4, d5, d6
        count: 6
    L2: threshold=100+
        domains: d4, d5, d6
        count: 3
    L3: threshold=1000+
        domains: d6
        count: 1

When we want to get popular domains, we first get the number of domains in each of
the levels. Then we calculate how many levels (or, if we do not delete the domains
from previous levels, which single lowest level), starting from the highest one,
will be enough to retrieve specific amount of domains in total. And then we fetch
the lists of domains in those selected levels. The whole thing with level counters
and selection is needed to prevent fetching lower levels each time, since lower level
usually contain too many domains that will not even be used.

For example, if we want to get 2 popular domains:

    L3: count>=N? 1>=2? False => ignore, continue.
    L2: count>=N? 3>=2? True => use it, break.
    L1: not checked.

And then, when we have a list of domains known to be one of the most popular, we
fetch their exact counter values (domain counters, not level counters), sort them
and return the final list of popular domains with their precise counts.

    d6: 1234    - read & returned
    d5: 999     - read & returned
    [d4: 500]   - read, but skipped


==== Mixing Together ====

Here the things get complicated. Now we need to count popular domains for the limited
time period. So we mix time shards with grid levels:

* Each domain counter is time-sharded.
* Each level (counter and list of domains) is time-sharded too.

When we add new url, we add it to time-sharded and grid-leveled storage items. When we
are trying to find top N domains over a time interval, we analyze grid levels on all
time shards involved in that interval. It makes the algorithms a bit (only a bit) more
complicated because of this compound ids and difficult to understand data structure.


==== Possible Bottleneck ====

The most significant problem here is that when new time shard begins, all workers will try
to update the lowest level in this new time shard, since all of the domains' counters were
just reset to zero and they start to grow again (something like "shard's grid warm up").

To address this problem in this specific implementation we just define the first level of
the grid starting not from 0 or 1, but from some greater value (10, 100 maybe). All domain
counters with lower values will be ignored in this case. Since different domain counters have
different speed of increasing, they will reach first level of the grid at different moments
of time, and will not conflict too much for write operations to that grid level's records.

The disadvantage is that on a low-loaded systems you may never get any popular domains at all,
since they will never reach first grid level until time shard ends; and then everything repeats.
For such systems you should define the threshold of the lowest level to "0" or "1".

This is why it is a good idea to define level thresholds separately for each system or host,
or even make it dynamically calculated (not too often, though). What will happen if you change
the levels' thresholds on a working system with fulfilled storage is a challenging topic to think of.
"""

from ..daal.storages import StorageID
from ._base import Dimension
import urlparse
import datetime
import time

__all__ = ['PopularDomainsDimension']


class DomainCounterID(StorageID):
    def __init__(self, time_shard, domain):
        super(DomainCounterID, self).__init__()
        self.time_shard = time_shard
        self.domain = domain

    def __iter__(self):
        return iter([('time_shard', self.time_shard), ('domain', self.domain)])

    def __unicode__(self):
        return '%s_%s' % (self.time_shard, self.domain)


class GridLevelID(StorageID):
    def __init__(self, time_shard, grid_level):
        super(GridLevelID, self).__init__()
        self.time_shard = time_shard
        self.grid_level = grid_level

    def __iter__(self):
        return iter([('time_shard', self.time_shard), ('grid_level', self.grid_level)])

    def __unicode__(self):
        return '%s_%s' % (self.time_shard, self.grid_level)


class PopularDomainsDimension(Dimension):
    """
    Analytics dimension for gathering the top domains by the number of URLs added.
    Domain is a hostname part of the URL shortened, with port and prefixes removed.
    For the implementation details, terminology and algorithms see module description.
    """

    GRID_LEVEL_THRESHOLDS = [0,5,10,20,30,40,50,100,200,300,400,500,1000,2000,3000,4000,5000,10000]
#    GRID_LEVEL_THRESHOLDS = [10, 20, 30, 40, 50]
    TIME_SHARD_DURATION = datetime.timedelta(seconds=12*60*60) # 12 hrs

    def __init__(self, url_domain_counter_storage, grid_level_counter_storage, grid_level_domains_storage):
        super(PopularDomainsDimension, self).__init__()
        self.url_domain_counter_storage = url_domain_counter_storage
        self.grid_level_counter_storage = grid_level_counter_storage
        self.grid_level_domains_storage = grid_level_domains_storage
        self.grid_level_thresholds = self.GRID_LEVEL_THRESHOLDS#??? constructor param with default value?
        self.time_shard_duration = self.TIME_SHARD_DURATION#??? constructor param with default value?

    def register(self, url):
        # Pre-calculate and extract key parameters of the algorithm.
        time_shard = self._calculate_time_shard(url.created_ts)
        url_domain = self._calculate_url_domain(url.url)

        # Increment the domain's counter and get its current value.
        counter_id = DomainCounterID(time_shard=time_shard, domain=url_domain)
        counter_value = self.url_domain_counter_storage.increment(counter_id, +1, retries=3)

        # Calculate what grid level this counter belongs to.
        current_level = self._calculate_grid_level(counter_value)
        initial_level = self._calculate_grid_level(counter_value-1)

        # Re-arrange the domain to next level, if necessary.
        # Ignore all counter before the first grid level (bottleneck solution - see module description).
        # Two atomic operations, but they are not transactional. So the most important one goes first.
        write_now = current_level != initial_level and current_level > 0
        if write_now:
            level_id = GridLevelID(time_shard=time_shard, grid_level=current_level)
            self.grid_level_domains_storage.append(level_id, ':::'+url_domain, retries=3)
            self.grid_level_counter_storage.increment(level_id, +1, retries=3)
            #??? ignore expectation errors?

    def retrieve(self, n, timedelta):
        # Pre-calculate and extract key parameters of the algorithm.
        time_shards = self._get_all_time_shards(time.time(), timedelta)
        grid_levels = self._get_all_grid_levels()

        # Here goes the hard thing. To understand what is happening here, read the module description.
        # First, fetch all grid levels' counters for all significant time shards. Then select only those
        # grid levels, that are sufficient to build the result, and fetch domain lists for all of them.
        # Finally, based on the list of top-candidate domain names, fetch their raw counter values.
        # Note that this operation calls mfetch() only three times, no matter how big or small
        # N and timedelta values are, or how many domains are registered in the storages. And
        # two of three calls use the limited number of ids, so there are no big datasets in memory.
        grid_level_ids_all = self._create_all_grid_level_ids(grid_levels, time_shards)
        grid_level_counters= self.grid_level_counter_storage.mfetch(grid_level_ids_all)
        grid_level_ids_top = self._select_top_grid_level_ids(grid_level_counters, n*3)# m to class/config/constructor???
        grid_level_domains = self.grid_level_domains_storage.mfetch(grid_level_ids_top)
        domain_counter_ids = self._extract_domains_counter_ids(grid_level_domains)
        domain_counters = self.url_domain_counter_storage.mfetch(domain_counter_ids)

        # Map & reduce time-sharded domain counters to flat dict{domain:count}, sort and slice it for top domains.
        reduced = self._reduce_domain_counters(domain_counters)
        domains = self._filter_top_domains(reduced, n)
        return domains

    def maintain(self):
        #TODO: delete old time shards
        pass

    def _calculate_url_domain(self, url):
        """
        Calculates canonical domain name based on url address.
        All ports removed.
        All common prefixes ("www", etc) removed.
        All extra dots removed (for proper DNS notation like "example.com.").
        """
        hostname = urlparse.urlparse(url).hostname
        domain = hostname.split(':')[0]
        domain = domain.lower()
        while domain.startswith('www.'):
            domain = domain[4:]
        domain = domain.strip('.')
        return domain

    def _calculate_time_shard(self, ts):
        """
        Calculates time shard identifier based in timestamp provided.
        """
        #todo later: accept ts as datetime & int & float.
        time_shard_duration = int(self.time_shard_duration.days*24*60*60 + self.time_shard_duration.seconds)
        time_shard = int(ts / time_shard_duration) * time_shard_duration
        return time_shard

    def _get_all_time_shards(self, ts, timedelta):
        """
        Provides the list of identifiers of all time shards between two moments on a timeline.
        Boundary moments are specified with timestamp of base moment and timedelta back to the past.
        TODO later: change to providing two points in time explicitly. use datetime&timedelta math.
        """
        time_shard_size = int(self.time_shard_duration.days*24*60*60 + self.time_shard_duration.seconds)
        timedelta_size = int(timedelta.days * 24*60*60 + timedelta.seconds)
        past_time_shard = int((ts - timedelta_size) / time_shard_size) * time_shard_size
        base_time_shard = int((ts - 0             ) / time_shard_size) * time_shard_size
        time_shards = list(xrange(past_time_shard, base_time_shard+1, time_shard_size))
        return time_shards

    def _calculate_grid_level(self, value):
        #IDEA: grid levels are not required to be sequentially numbered. they can even be strings.
        for index, level in enumerate(self.grid_level_thresholds):
            if value < level:
                return index
        return len(self.grid_level_thresholds)-1

    def _get_all_grid_levels(self):
        #IDEA: grid levels are not required to be sequentially numbered. they can even be strings.
        return list(xrange(1, len(self.grid_level_thresholds)))

    def _create_all_grid_level_ids(self, grid_levels, time_shards):
        return [GridLevelID(time_shard=time_shard, grid_level=grid_level) for grid_level in grid_levels for time_shard in
                time_shards]

    def _select_top_grid_level_ids(self, grid_level_counters, domains_per_time_shard):
        # Re-structure flat list of counters into two-level dict{time_shard:{grid_level:count}}
        # for ease of lookup & iteration.
        struct = {}
        for grid_level_counter in grid_level_counters:
            time_shard = grid_level_counter['time_shard']
            grid_level = grid_level_counter['grid_level']
            count = int(grid_level_counter['count'])
            struct.setdefault(time_shard, {}).update({grid_level: count})

        # Decide which levels are really necessary for retrieving at least M domains per time shard.
        top_grid_level_ids = []
        for time_shard in struct.keys():
            # Try to find the best grid level within time shard. Use the latest (lowest) one if none are enough.
            time_shard_best_grid_level_id = None
            for grid_level in sorted(struct[time_shard].keys(), reverse=True):#??? what if levels (keys) are strings?
                time_shard_best_grid_level_id = GridLevelID(time_shard=time_shard, grid_level=grid_level)
                if struct[time_shard][grid_level] >= domains_per_time_shard:
                    break
            top_grid_level_ids.append(time_shard_best_grid_level_id)
        return top_grid_level_ids

    def _extract_domains_counter_ids(self, grid_level_domains):
        # Parse what domains in what time_shards are candidates to be in the final result.
        pairs = set()#NB: to eliminate duplicates across different grid levels
        for grid_level in grid_level_domains:
            time_shard = grid_level['time_shard']
            domains = grid_level['domains']
            domains = domains.split(':::')
            domains = filter(None, domains)
            pairs.update([(time_shard, domain) for domain in domains])
        counter_ids = [DomainCounterID(time_shard=time_shard, domain=domain) for time_shard, domain in pairs]
        return counter_ids

    def _reduce_domain_counters(self, counters):
        # Calculate the resulting statistics from fetched grid levels.
        combined = {}
        for counter in counters:
            domain = counter['domain']
            value = int(counter['value'])
            combined[domain] = combined.get(domain, 0) + value
        return combined

    def _filter_top_domains(self, combined, n):
        flat = combined.items()
        flat.sort(lambda a,b: cmp(a[1], b[1]), reverse=True)
        tops = flat[:n]
        tops = [dict(domain=domain, count=count) for domain, count in tops]
        return tops
