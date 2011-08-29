# coding: utf-8
#TODO: explicit micro-transatctions (we have commits but have no begins now). preferable using "with".

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

    def __init__(self, hostname, username, password, database, name):
        super(MysqlStorage, self).__init__()
        self.connection = None
        self.connected = False
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
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

        where, values = self._ids_to_sql([id])
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

        where, values = self._ids_to_sql(ids)
        query = "SELECT * FROM `%s` WHERE %s" % (self.name, where) #!!! escape table name

        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, values)
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
#        pk_where, pk_values = self._ids_to_sql([item['id']])

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
        pk = dict(StorageID(id))

        # Try to fetch the item's values from the physical storage.
        # Fallback to empty list of values if the item does not exist.
        try:
            item = self.fetch(id)
        except StorageItemAbsentError, e:
            item = Item() # what if it is of another type???
            item.update(pk)

        # Ensure the item is absent using an attribute that always exists.
#        pk_where, pk_values = self._ids_to_sql([id]) # is it still used???

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)
        changes.update(dict(StorageID(id)))
        values = dict(item, **changes)
        #!!! separate and make it obvious which fields are for pk, and which are for values. merge them only for the query.

        # Build SQL query to INSERT or UPDATE depending on absence of existence of the item.
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in changes.keys()])
        query = "INSERT INTO %s SET %s ON DUPLICATE KEY UPDATE %s" % (self.name, assignments, assignments)

        # Store the values to the physical storage if necessary.
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, values)
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
        pk_where, pk_values = self._ids_to_sql([id])

        # Get the values to be updated. Store them to the physical storage if necessary.
        changes = fn(item)
        #!!!TODO: I guess here is an error, since pk_values are not merged into changes. But seems we do not use replace() at all.

        # Build SQL query to update an item if it exists.
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in changes.keys()])
        query = "UPDATE %s SET %s WHERE %s" % (self.name, assignments, pk_where)

        # Store the values to the physical storage if necessary.
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, changes)
        #!!!todo: raise StorageItemAbsentError() if  affected_rows == 0
        self.connection.commit()

        # Return
        return changes # re-fetch?

    def append(self, id, value, retries=1):
        #NB: since we use SQL row locks, there is no need to retries.
        #NB: value field must be declared as NOT NULL DEFAULT ''.
        #TODO: we can remove that requirement for DEFAULT value, but have to rewrite all this dict manipulations.

        value_field = 'value'
        self._connect()

        # Execute the query and aquire a row lock on the counter.
        pk = dict(StorageID(id))
        values = dict(pk, value=value)
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in pk.keys()]
                             + ['%s=concat(%s, %%(%s)s)' % (value_field, value_field, value_field)])
        query = "INSERT INTO %s SET %s ON DUPLICATE KEY UPDATE %s" % (self.name, assignments, assignments)
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, values)

        # Fetch the value while it is locked - no one will change it since we updates and till we committed.
        value = self.fetch(id)[value_field]

        # Commit and release the row lock.
        self.connection.commit()

        # Return
        return value

    def prepend(self, id, value, retries=1):
        raise NotImplementedError()#!!!todo later

    def increment(self, id, step, retries=1):
        #NB: since we use SQL row locks, there is no need to retries.
        #NB: value field must be declared as NOT NULL DEFAULT 0.
        #TODO: we can remove that requirement for DEFAULT value, but have to rewrite all this dict manipulations.

        value_field = 'value'
        self._connect()

        pk = dict(StorageID(id))
        assignments = ','.join(['%s=%%(%s)s' % (field, field) for field in pk.keys()] + ['%s=%s+(%d)' % (value_field, value_field, int(step))])
        query = "INSERT INTO %s SET %s ON DUPLICATE KEY UPDATE %s" % (self.name, assignments, assignments)

        # Execute the query and aquire a row lock on the counter.
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, pk)

        # Fetch the value while it is locked - no one will change it since we updates and till we committed.
        value = self.fetch(id)[value_field]

        # Commit and release the row lock.
        self.connection.commit()

        # Return
        return value

    def decrement(self, id, step, retries=1):
        # No special support or optimizations for decrement operation.
        return self.increment(id, -step, retries=retries)

    def _metaescape(self, name):
        """
        Escapes field and table names (and any other SQL entities) for use in queries.
        Does not escape data values with this! Use parameter binding instead!
        """
        return '`%s`' % unicode(meta).replace('`', '``')

    def _ids_to_sql(self, ids):
        """
        Converts few storage IDs to SQL WHERE clause and dict with values for binding.
        List of ids must be non-empty (it's better to catch this situation at higher level).

        The clause returned is build as short as it is possible in this implementation.
        But it resolves all conflicts with the same-named fields with differently values
        by use of automatically generated reference names instead of just field names.

        It also optimizes the number of values by re-using same references for same values.
        This is especially useful when you have dozens of ids (100+), and in most of them
        have fields with equal or repeating values.

        E.g., if we have ids {a=10,b=20} and {a=10,b=30}, it will return these WHERE clause and values:
            clause = (a=%(a1)s and b=%(b1)s) or (a=%(a1)s and b=%(b2)s)
            values = {a1=10, b1=20, b2=30}
        Note that %(a1)s parameter is used for both expressions, since its values is the same.
        """
        wheres = []
        values = []
        references = {} # [field][value] -> key in values
        for index, id in enumerate(ids):
            # Build per-id expressions.
            id_wheres = []
            id_values = []
            for field, value in dict(StorageID(id)).items():
                # First, check if this field&value pair exists already, and create it if it does not yet.
                reference = references.setdefault(field, {}).setdefault(value, '%s%s' % (field, index))

                # Then, add the field&value expression to lists for query building.
                id_wheres.append((field, reference))
                id_values.append((reference, value))

            # Merge per-id expressions into global list of expressions.
            wheres.append(id_wheres)
            values.extend(id_values)
        clause = '(%s)' % 'OR'.join(['(%s)' % ' AND '.join(['%s=%%(%s)s' % (field, reference) for field, reference in id_wheres]) for id_wheres in wheres])
        values = dict(values)
        return clause, values

    def _connect(self):
        """
        Connects to the storage if not connected yet.
        If already connected, does nothing.
        """
        if not self.connected:
            self.connection = MySQLdb.connect(host = self.hostname,
                                     user = self.username,
                                     passwd = self.password,
                                     db = self.database)
            self.connected = True
        return self
