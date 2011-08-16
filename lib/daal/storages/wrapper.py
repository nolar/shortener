# coding: utf-8
from ._base import Storage

__all__ = ['WrapperStorage']

class WrapperStorage(Storage):
    """
    Wrapper storage is a namespace isolation layer, which proxies all calls to the
    wrapped storage, but changes the IDs to contain the namespace specified.
    The namespace is a set of prefix & suffix for the id. Note that this wrapper
    storage does not affect a domain name, and does not know about such a thing.
    
    TODO: Namespace is called "host" here for project-specific reason, but will be renamed later.
    """
    
    def __init__(self, storage, prefix='', suffix=''):
        super(WrapperStorage, self).__init__()
        self.storage = storage
        self.format = '%(prefix)s%(id)s%(suffix)s'
        self.prefix = prefix
        self.suffix = suffix
    
    def store(self, id, value, expect=None, unique=None):
        return self.storage.store(self._wrap_id(id), value, expect=expect, unique=unique)
    
    def fetch(self, id):
        return self.storage.fetch(self._wrap_id(id))
    
    def query(self, columns=None, where=None, order=None, limit=None):
        #!!! itemName() is an internal implementation detail of SDBStorage only, not a wrapper
        addon = "itemName() like '%s%%'" % (self.prefix)#!!! escape
        where = "(%s) AND (%s)" % (where, addon) if where else "(%s)" % (addon)
        return self.storage.query(columns, where, order, limit)
    
    def _wrap_id(self, id):
        if isinstance(id, basestring):
            return self.format % {'prefix': self.prefix, 'suffix': self.suffix, 'id': id }
        else:
            return [self._wrap_id(i) for i in id]
