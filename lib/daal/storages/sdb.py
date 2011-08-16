# coding: utf-8
from ._base import Storage
from ._base import StorageExpectationError, StorageItemAbsentError, StorageUniquenessError
from boto.exception import SDBResponseError
from boto.sdb.connection import SDBConnection

__all__ = ['SDBStorage']

class SDBStorage(Storage):
    """
    Stores all information in Amazon SimpleDB.
    
    Tries to work around some of its limitations to keep the whole semantics:
    * Limit on number of predicates in WHERE IN query for multi-id fetch (20 max).
    * Limit on the lenght of an attribute (1024 chars max).
    * Others to come.
    """
    
    def __init__(self, access_key, secret_key, name):
        super(SDBStorage, self).__init__()
        self.access_key = access_key
        self.secret_key = secret_key
        self.connected = False
        self.connection = None
        self.domain = None
        self.name = name

    def store(self, id, value, expect=None, unique=None):
        """
        Stores one single item with its id. Supports atomic conditional writes:
        * Item must be unique, i.e. raise an error if exists (usually checked by `id` field).
        * Item must meet the condition by one of its fields being equal to specific value.
        If conditional write fails, the storage raises expectation error.
        See Storage.repeat() on how to work with this technique.
        """
        
        if unique is not None and expect is not None:
            raise ValueError("Only expect or exists parameter maybe passed, not both.")
        elif expect is not None:
            expect_field, expect_value = expect.items()[0]
            expect = [expect_field, expect_value]
        elif unique is not None:
            expect_field = unique if isinstance(unique, basestring) else 'id'
            expect_value = False if unique else True
            expect = [expect_field, expect_value]
        else:
            expect = None
        
        self._connect()
        try:
            split = self._split(value)
            #split['id'] = id
            self.domain.put_attributes(id, split, expected_value=expect)
        except SDBResponseError, e:
            if e.code == 'ConditionalCheckFailed':
                raise StorageExpectationError("Storage expecation failed.")
            else:
                raise
    
    def fetch(self, id_or_ids):
        """
        Fetches one or many items by ids. If id is a string, one item is fetched,
        otherwise id is treated as a sequence of ids and all of them are fetched.
        Actual fetch goes in batches of 20 items per requests (SimpleDB limitation).
        """
        
        self._connect()
        if isinstance(id_or_ids, basestring):
            id = id_or_ids
            item = self.domain.get_attributes(id, consistent_read=True)
            if not item:
                raise StorageItemAbsentError("The item '%s' is not found." % id)
            item = self._rejoin(item)
            return item
        else:
            ids = id_or_ids
            items = []
            #??? is there any itertools function to iterate the batches?
            for i in xrange(0, len(ids) / 20 + 1):
                ids_slice = ids[i*20:(i+1)*20]
                if ids_slice:
                    ids_str = ','.join(["'%s'" % id for id in ids_slice])#!!!! ad normal escaping
                    query = 'SELECT * FROM %s WHERE itemName() in (%s)' % (self.domain.name, ids_str)
                    for item in self.domain.select(query):
                        items.append(self._rejoin(item))
            return items
    
    def query(self, columns=None, where=None, order=None, limit=None):
        """
        Builds and executes the SQL-like query to the storage.
        
        TODO: query() is highly unwanted in key-value storages, since it introduces
        TODO: very complexed overhead to the semantics of the class. Try to remove it.
        TODO: As of now, it is used in analytics dimensions only.
        """
        
        self._connect()
        query = ''
        query +=  'SELECT %s' % (','.join(columns) if columns else '*')
        query +=  ' FROM  %s' % (self.domain.name)
        query += (' WHERE %s'    % where) if where else ''
        query += (' ORDER BY %s' % order) if order else ''
        query += (' LIMIT %s'    % limit) if limit else ''
        return [self._rejoin(item) for item in self.domain.select(query)]
    
    def _split(self, value):
        """
        Prepares the item for storage by splitting long attributes into pieces
        of smaller ones (SimpleDB limit of 1024 chars per attribute).
        Another way os to store these data in S3, but since it is usually an URL
        with predictable size, why involve one more subsystem?
        """
        
        split = {}
        for key, val in value.items():
            val = unicode(val)
            if len(val) <= 1024:
                split[key] = val
            else:
                val = unicode(val)
                for i in range(0, len(val)+1, 1024):
                    split[key+'#'+unicode(i)] = val[i:i+1024]
        return split
    
    def _rejoin(self, item):
        """
        Restores the item from storage by joinede all split attributes back
        from pieces into single values (SimpleDB limit of 1024 chars per attribute).
        Another way os to store these data in S3, but since it is usually an URL
        with predictable size, why involve one more subsystem?
        """
        
        joined = {}
        for key, val in item.items():
            if '#' in key:
                key_base, key_index = key.split('#', 1)
                key_index = int(key_index)
                if key_base not in joined:
                    joined[key_base] = {}
                joined[key_base][key_index] = val
            else:
                joined[key] = val
        for key, val in joined.items():
            if isinstance(val, dict):
                joined[key] = ''.join([text for index, text in sorted(val.items(), cmp=lambda a,b:cmp(a[0],b[0]))])
        return joined
    
    def _connect(self):
        """
        Connects to the storage if not connected yet.
        If already connected, does nothing.
        """
        if not self.connected:
            self.connection = SDBConnection(self.access_key, self.secret_key)
            try:
                self.domain = self.connection.get_domain(self.name)
            except SDBResponseError:
                self.domain = self.connection.create_domain(self.name)
            self.connected = True
        return self
