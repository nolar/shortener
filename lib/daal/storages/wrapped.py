# coding: utf-8
from ._base import Storage, StorageID
import functools

__all__ = ['WrappedStorage']


class WrappedID(StorageID):
    """
    Extended storage ID with knowledge of per-host separation as introduced by WrappedStorage.
    Instances of this ID substitute all other IDs of all types (StorageID instances and scalars)
    before being passed to the real (wrapped) storage.

    For this to work, all storages must obey the limitation described in StorageID class, i.e. they
    must only convert the ids either to unicode() or to dict(), and never to assume on ids' internals.
    """

    def __init__(self, id, host):
        super(WrappedID, self).__init__(id)
        self.host = host

    def __iter__(self):
        # Note that this expression keeps the keys unique, but overrides the "host" key if any.
        return iter(dict(super(WrappedID, self).__iter__(), host=self.host).items())

    def __unicode__(self):
        # Note that we do not convert the wrapped id ourselves, but ask our parent class to do this.
        return '%s_%s' % (self.host, super(WrappedID, self).__unicode__())


class WrappedStorage(Storage):
    """
    Wrapper storage is a namespace isolation layer, which proxies all calls to the
    wrapped storage, but changes the IDs to contain the namespace specified.
    The namespace is a set of prefix & suffix for the id. Note that this wrapper
    storage does not affect a domain name, and does not know about such a thing.
    
    TODO: Namespace is called "host" here for project-specific reason, but will be renamed later.
    """

    def __init__(self, storage, host):
        super(WrappedStorage, self).__init__()
        self.storage = storage
        self.host = host

    def fetch(self, id):
        return self.storage.fetch(self._wrap_id(id))

    def mfetch(self, ids):
        return self.storage.mfetch(list(map(self._wrap_id, ids)))

    def select(self, filters={}, sorters=[], limit=None):
        return self.storage.select(filters=dict(filters, host=self.host), sorters=sorters, limit=limit)

    def create(self, factory, retries=1):
        return self.storage.create(self._wrap_factory(factory), retries=retries)
    
    def update(self, id, fn, retries=1, field=None):
        return self.storage.update(self._wrap_id(id), fn, retries=retries, field=field)
    
    def replace(self, id, fn, retries=1, field=None):
        return self.storage.replace(self._wrap_id(id), fn, retries=retries, field=field)
    
    def append(self, id, value, retries=1):
        return self.storage.append(self._wrap_id(id), value, retries=retries)

    def prepend(self, id, value, retries=1):
        return self.storage.prepend(self._wrap_id(id), value, retries=retries)

    def increment(self, id, step, retries=1):
        return self.storage.increment(self._wrap_id(id), retries=retries)

    def decrement(self, id, step, retries=1):
        return self.storage.decrement(self._wrap_id(id), retries=retries)

    def _wrap_id(self, id):
        """
        Wraps the id to contain extended information about the host of the item being accessed.
        See the description of WrappedID class for more details on how this works.
        """
        if isinstance(id, (int, basestring, StorageID)):
            return WrappedID(id=id, host=self.host)
        else:
            return [self._wrap_id(i) for i in id]

    def _wrap_factory(self, factory):
        """
        Wraps the factory passed to create() method, so that new generated items
        will contain "host" field. If the item contains the "host" field initially,
        it will be overridden. The type of the items will be kept untouched.
        """
        @functools.wraps(factory)
        def wrapped_factory(*args, **kwargs):
            result = factory(*args, **kwargs)
            result['id'] = self._wrap_id(result['id'])
            result['host'] = self.host
            return result
        return wrapped_factory
