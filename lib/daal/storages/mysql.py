# coding: utf-8
from ..item import Item
from ._base import Storage, StorageID
from ._base import StorageExpectationError, StorageItemAbsentError, StorageUniquenessError
import MySQLdb

__all__ = ['MysqlStorage']

class MysqlStorage(Storage):
    """
    Stores all information in Amazon SimpleDB.

    Tries to work around some of its limitations to keep the whole semantics:
    * Limit on number of predicates in WHERE IN query for multi-id fetch (20 max).
    * Limit on the lenght of an attribute (1024 chars max).
    * Others to come.
    """

    def __init__(self, name):
        super(MysqlStorage, self).__init__()
        self.connected = False
        self.connection = None
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

        values = dict(StorageID(id))
        where = '(%s)' % self._id_to_where(id)
        query = "SELECT * FROM `%s` WHERE %s" % (self.name, where) #!!! escape table name

        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, values)
        rows = cursor.fetchall()
        if len(rows) > 1:
            raise StorageBadIdError("ID is not unique enough, few rows returned.")#!!! declare it
        if len(rows) < 1:
            raise StorageItemAbsentError("The item '%s' is not found." % id)
        item = rows[0]

        #??? factory? on Storage level?

        return item

    def mfetch(self, ids):
        """
        Fetches one or many items by ids. If id is a string, one item is fetched,
        otherwise id is treated as a sequence of ids and all of them are fetched.
        Actual fetch goes in batches of 20 items per requests (SimpleDB limitation).
        """

        self._connect()

        where = 'OR'.join(['(%s)' % self._id_to_where(id) for id in ids])
        query = "SELECT * FROM `%s` WHERE %s" % (self.name, where) #!!! escape table name

        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query)
        rows = cursor.fetchall()
        if len(rows) < 1:
            raise StorageItemAbsentError("The item '%s' is not found." % id)
        items = rows

        #??? factory? on Storage level?

        return items

    def select(self, filters={}, sorters=[], limit=None):
        """
        Builds and executes the SQL-like select to the storage.

        TODO: select() is highly unwanted in key-value storages, since it introduces
        TODO: very complexed overhead to the semantics of the class. Try to remove it.
        TODO: As of now, it is used in analytics dimensions only.
        """

        self._connect()

        #TODO: escape domain name and field names
        values = dict(filters)
        extra_fields = [field for field, order in sorters if field not in filters]
        filters = ' AND '.join(["%s=%%(%s)s" % (field, field) for field in filters.keys()])
        sorters = ', '.join(["%s %s" % (field, ["ASC","DESC"][int(bool(order))]) for field, order in sorters])

        query = ''
        query += ("SELECT * FROM %s" % (self.name))#!!! escape
        query += (" WHERE %s"    % filters) if filters else ''
        query += (" ORDER BY %s" % sorters) if sorters else ''
        query += (" LIMIT %s"    % limit  ) if limit   else ''
        print(query)

        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, values)
        items = list(cursor.fetchall())

        return items

    def try_create(self, factory):
        """
        Makes one attempt to create unique item in the storage.
        Fails if there is an item with the same id.
        This method is never used directly; it is called from Storage.create() method in repeating cycle.
        """

        self._connect()

        # Generate an item. Field values and even id can be different on each try.
        # Normalize the id for key-value usage scenario.
        item = factory()
        pk = dict(StorageID(item['id']))
        item.update(pk)

        # Ensure the item is absent using an attribute that always exists.
#        pk_where = '(%s)' % self._id_to_where(item['id'])

        # Store the values to the physical storage if necessary.
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in item.keys()])
        query = "INSERT INTO %s SET %s" % (self.name, assignments)
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, dict(item))
        self.connection.commit()

        # Return
        return item # re-fetch?

    def try_update(self, id, fn, field=None):
        """
        Makes one attempt to create or update an item in the storage.
        Fails if the item was changed between fetch() and store().
        This method is never used directly; it is called from Storage.update() method in repeating cycle.
        """

        self._connect()

        # Try to fetch the item's values from the physical storage.
        # Fallback to empty list of values if the item does not exist.
        try:
            item = self.fetch(id)
        except StorageItemAbsentError, e:
            item = Item() # what if it is of another type???

        # Ensure the item is absent using an attribute that always exists.
        pk_where = '(%s)' % self._id_to_where(id)

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)
        changes.update(dict(StorageID(id)))

        # Store the values to the physical storage if necessary.
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in changes.keys()])
        query = "REPLACE INTO %s SET %s" % (self.name, assignments)
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, changes)
        self.connection.commit()

        # Return
        return changes # re-fetch?

    def try_replace(self, id, fn, field=None):
        """
        Makes one attempt to update an item in the storage.
        Fails if the item does not exist or was changed between fetch() and store().
        This method is never used directly; it is called from Storage.update() method in repeating cycle.
        """

        self._connect()

        # Try to fetch the item's values from the physical storage.
        # Fallback to empty list of values if the item does not exist.
        try:
            item = self.fetch(id)
        except StorageItemAbsentError, e:
            raise # just to make it very obvious that we pass it through.

        # Ensure the item is absent using an attribute that always exists.
        pk_where = '(%s)' % self._id_to_where(id)

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)

        # Store the values to the physical storage if necessary.
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in changes.keys()])
        query = "UPDATE %s SET %s WHERE %s" % (self.name, assignments, pk_where)
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, changes)
        self.connection.commit()

        # Return
        return changes # re-fetch?

    def _id_to_where(self, id):
        pk = dict(StorageID(id))
        where = ' AND '.join(["%s = %%(%s)s" % (field, field) for field in pk.keys()])
        return where

    def _connect(self):
        """
        Connects to the storage if not connected yet.
        If already connected, does nothing.
        """
        if not self.connected:
            self.connection = MySQLdb.connect(host = "localhost",
                                     user = "root",
                                     passwd = "",
                                     db = "shortener")
            self.connected = True
        return self
