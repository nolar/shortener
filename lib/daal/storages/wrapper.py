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
    
    def select(self, columns=None, where=None, order=None, limit=None):
        return self.storage.select(columns, where, order, limit)
    
    def _wrap_id(self, id):
        if isinstance(id, basestring):
            return self.format % {'prefix': self.prefix, 'suffix': self.suffix, 'id': id }
        else:
            return [self._wrap_id(i) for i in id]
