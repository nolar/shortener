# coding: utf-8
from ..item import Item
from ._base import Storage, StorageID
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
        
        id = unicode(StorageID(id))

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
    
    def fetch(self, id):
        """
        Fetches one or many items by ids. If id is a string, one item is fetched,
        otherwise id is treated as a sequence of ids and all of them are fetched.
        Actual fetch goes in batches of 20 items per requests (SimpleDB limitation).
        """
        
        self._connect()

        id = unicode(StorageID(id))
        
        item = self.domain.get_attributes(id, consistent_read=True)
        if not item:
            raise StorageItemAbsentError("The item '%s' is not found." % id)
        
        result = self._rejoin(item)
        return result

    def mfetch(self, ids):
        """
        Fetches one or many items by ids. If id is a string, one item is fetched,
        otherwise id is treated as a sequence of ids and all of them are fetched.
        Actual fetch goes in batches of 20 items per requests (SimpleDB limitation).
        """

        self._connect()

        result = []
        #??? is there any itertools function to iterate the batches?
        chunk_length = 20 #NB: hard-coded limit on number of predicates in SimpleDB queries
        for chunk_offset in xrange(0, len(ids), chunk_length):
            chunk_ids = ids[chunk_offset:chunk_offset+chunk_length]
            chunk_str = ','.join(["'%s'" % unicode(StorageID(id)) for id in chunk_ids])#!!!! add string escaping
            query = 'SELECT * FROM %s WHERE itemName() in (%s)' % (self.domain.name, chunk_str)
            items = self.domain.select(query)
            result.extend(map(self._rejoin, items))
        return result

    def select(self, filters={}, sorters=[], limit=None):
        """
        Builds and executes the SQL-like select to the storage.
        
        TODO: select() is highly unwanted in key-value storages, since it introduces
        TODO: very complexed overhead to the semantics of the class. Try to remove it.
        TODO: As of now, it is used in analytics dimensions only.
        """
        
        self._connect()

        #TODO: escape domain name and field names
        extra_fields = [field for field, order in sorters if field not in filters]
        filters = ' AND '.join(["%s='%s'" % (field, value) for field, value in filters.items()] + ["%s>=''" % field for field in extra_fields])
        sorters = ', '.join(["%s %s" % (field, ["ASC","DESC"][int(bool(order))]) for field, order in sorters])
        
        query = ''
        query += ("SELECT * FROM %s" % (self.domain.name))
        query += (" WHERE %s"    % filters) if filters else ''
        query += (" ORDER BY %s" % sorters) if sorters else ''
        query += (" LIMIT %s"    % limit  ) if limit   else ''
        print(query)

        items = self.domain.select(query)
        result = list(map(self._rejoin, items))
        return result
    
    def try_create(self, factory):
        """
        Makes one attempt to create unique item in the storage.
        Fails if there is an item with the same id.
        This method is never used directly; it is called from Storage.create() method in repeating cycle.
        """
        
        # Generate an item. Field values and even id can be different on each try.
        # Normalize the id for key-value usage scenario.
        item = factory()
        id = item['id'] = unicode(StorageID(item['id']))

        # Ensure the item is absent using an attribute that always exists.
        expected_value = {'id': False}#!!! configurable field name in constructor or class-level

        # Store the values to the physical storage if necessary.
        self.store(id, item, expect=expected_value)

        # Return
        return item # re-fetch?
    
    def try_update(self, id, fn, field=None):
        """
        Makes one attempt to create or update an item in the storage.
        Fails if the item was changed between fetch() and store().
        This method is never used directly; it is called from Storage.update() method in repeating cycle.
        """

        # Normalize the id for key-value usage scenario.
        id = unicode(StorageID(id))

        # Try to fetch the item's values from the physical storage.
        # Fallback to empty list of values if the item does not exist.
        try:
            item = self.fetch(id)
        except StorageItemAbsentError, e:
            item = Item() # what if it is of another type???

        # Prepare expectation criterion for store() based on the data before the changes.
        # Note that default old_value is False - to check for absence of the attribute in SDB.
        expected_value = {field: item.get(field, False)} if field else None

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)
        self.store(id, changes, expect=expected_value)

        # Return
        return changes #??? item changed? re-fetch?

    def try_replace(self, id, fn, field=None):
        """
        Makes one attempt to update an item in the storage.
        Fails if the item does not exist or was changed between fetch() and store().
        This method is never used directly; it is called from Storage.update() method in repeating cycle.
        """

        # Normalize the id for key-value usage scenario.
        id = unicode(StorageID(id))

        # Try to fetch the item's values from the physical storage.
        # Raise an error if the item does not exist.
        try:
            item = self.fetch(id)
        except StorageItemAbsentError, e:
            raise # just to make it very obvious that we pass it through.

        # Prepare expectation criterion for store() based on the data before changes.
        # Note that default old_value is False - to check for absence of the attribute in SDB.
        expected_value = {field: item.get(field, False)} if field else None

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)
        self.store(id, changes, expect=expected_value)

        # Return
        return changes # re-fetch?

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
