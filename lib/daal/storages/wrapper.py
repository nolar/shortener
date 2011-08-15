# coding: utf-8
from ._base import Storage

__all__ = ['WrapperStorage']

class WrapperStorage(Storage):
    def __init__(self, storage, format='%(prefix)s%(id)s%(suffix)s', prefix='', suffix=''):
        super(WrapperStorage, self).__init__()
        self.storage = storage
        self.format = format
        self.prefix = prefix
        self.suffix = suffix
    
    def store(self, id, value, expect=None, unique=None):
        return self.storage.store(self._wrap_id(id), value, expect=expect, unique=unique)
    
    def fetch(self, id):
        return self.storage.fetch(self._wrap_id(id))
    
    def repeat(self, fn, retries=1, exception=None):
        return self.storage.repeat(fn, retries, exception)
    
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
