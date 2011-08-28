# coding: utf-8
import functools
import itertools

__all__ = ['Storage', 'StorageItemAbsentError', 'StorageExpectationError', 'StorageUniquenessError']


class StorageItemAbsentError(Exception): pass
class StorageUniquenessError(Exception): pass
class StorageExpectationError(Exception): pass


class StorageID(object):
    """
    Storage ID is used to uniquely identify an item in the storage. In most cases it is
    used only internally within the storages, so external classes still can use regular
    scalars. Explicit creation of storage ids outside of the storages is not recommended.

    Storages must accept ids both as scalars and as instances of this class or its descendants.
    They can normalize the id by calling StorageID(original_id). If the original id happens
    to be another storage id instance, then it will be wrapped within newly created instance,
    but will continue to obey all the conventions and protocols for storage ids.

    Storages are allowed to use the ids only in two ways:
        * by casting them to unicode scalar (suitable for key-value storages):
                physical_key = unicode(StorageID(original_id))
        * by accessing all of their fields (suitable for SQL-based storages):
                compound_key = dict(StorageID(original_id))

    Descendants are allowed to ignore "id" attribute. In this case, i.e. when "id" attribute
    is not initialized in call to StorageID constructor, descendants classes must overrride
    __iter__() & __unicode__() methods. Calls to StorageID methods will raise "not implemented" error.
    This is especially useful for compound IDs with set of fields not including "id".
    """

    def __init__(self, id=None):
        super(StorageID, self).__init__()
        self.id = id

    def __iter__(self):
        if self.id is None:
            raise NotImplementedError()
        elif isinstance(self.id, StorageID):
            return iter(self.id)
        else:
            return iter([('id', unicode(self.id))])

    def __unicode__(self):
        if self.id is None:
            raise NotImplementedError()
        else:
            return unicode(self.id)


class Storage(object):
    """
    Base class for all storages.
    """

    def __init__(self):
        super(Storage, self).__init__()

    def fetch(self, id):
        raise NotImplementedError()

    def mfetch(self, ids):#!!! stupid name! is there a 6-letter verb for multi-id fetch (but not select/search/query)?
        raise NotImplementedError()

    def select(self, filters={}, sorters=[], limit=None):
        raise NotImplementedError()

    @staticmethod
    def repeat(fn, retries=1, exception=None):
        """
        Primitive for repeating tries for conditional writes, etc.

        Since some storages have no locks of any kind, the only way to make
        atomic operations is conditional writes (i.e. write only if specific
        field equals to specific value). Classic solution is to try few times
        in a cycle before giving up. The problem is that you change the data
        inside  that cycle, not before it (since you need current values).
        This primitive is exactly what it does: tries to execute your function
        few times before giving up.

        Usual case for data altering is like this:

            def try_update():
                item = storage.fetch(id)
                old_value = item['field']
                new_value = old_value + 1
                item['field'] = new_value
                storage.store(id, item, expect={'field':old_value})
            storage.repeat(try_update, retries=3)

        Usual case for data creation is like this:

            def try_create():
                id = generate_id()
                storage.store(id, item, unique='id')
            storage.repeat(try_create, retries=3)

        You can also specify what exception will be thrown in case of all
        the tries will fail: it can be either the exception instance, or
        callback which return an exception instance. If exception is not
        specified or the callback returns None, the original exception is
        re-raised. Note that callback can also be a class of the exception.

        TODO: move store() action inside this loop, but simplify it before (now it is too complex).
        """

        while retries > 0:
            try:
                retries = retries - 1
                return fn()
            except StorageExpectationError, e:
                if retries <= 0:
                    if callable(exception):# includes types and classes
                        exception = exception(e)

                    if exception:
                        raise exception
                    else:
                        raise

    @staticmethod
    def ignore(fn, retries=1, exception=None):
            #??? NOT USED? probably as a drop-in replacement for repeat() for fast switched forth and back.
            try:
                retries = retries - 1
                return fn()
                #??? move self.store() here? just to ensure that it tries to store at all, and to make it meaningful.
            except StorageExpectationError, e:
                pass

    def create(self, factory, retries=1):
        """
        Creates new item. If an item with same id already exists, raises an error.
        Similar to "add" action in "set/add/replace" memcached-like approach.

        Developers can try generate few different ids to avoid the error. For this,
        specify id as a callable that returns id value, and retries greater than 1.
        Values can be a callable too to alter the data if needed.
        """
        return self.repeat(functools.partial(self.try_create, factory), retries=retries)

    def update(self, id, fn, retries=1, field=None):
        """
        Updates the item if it exists, or creates a new one if it does not exist.
        Similar to "set" action in "set/add/replace" memcached-like approach,
        except you have item's values fetched before the update.
        """
        return self.repeat(functools.partial(self.try_update, id, fn, field), retries=retries)

    def replace(self, id, fn, retries=1, field=None):
        """
        Updates existing item. If the item does not exist, raises and error.
        Similar to "replace" action in "set/add/replace" memcached-like approach,
        except you have item's values fetched before the update.
        """
        return self.repeat(functools.partial(self.try_replace, id, fn, field), retries=retries)

    def append(self, id, value, retries=1):
        raise NotImplementedError()

    def prepend(self, id, value, retries=1):
        raise NotImplementedError()

    def increment(self, id, step, retries=1):
        raise NotImplementedError()

    def decrement(self, id, step, retries=1):
        raise NotImplementedError()


    def try_create(self, factory):
        raise NotImplementedError()

    def try_update(self, id, fn, field):
        raise NotImplementedError()

    def try_replace(self, id, fn, field):
        raise NotImplementedError()
