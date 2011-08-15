# coding: utf-8

__all__ = ['Storage', 'StorageItemAbsentError', 'StorageExpectationError', 'StorageUniquenessError']

class StorageItemAbsentError(Exception): pass
class StorageUniquenessError(Exception): pass
class StorageExpectationError(Exception): pass

class Storage(object):
    def __init__(self):
        super(Storage, self).__init__()
    
    def store(self, id, url):
        raise NotImplemented()
    
    def fetch(self, host, id):
        raise NotImplemented()
    
    @staticmethod
    def repeat(fn, retries=1, exception=None):
        while retries > 0:
            try:
                retries = retries - 1
                return fn()
                #??? move self.store() here? just to ensure that it tries to store at all, and to make it meaningful.
            except StorageExpectationError, e:
                if retries <= 0:
                    if callable(exception):# includes types and classes
                        raise exception(e) or e
                    else:
                        raise exception or e
    
    @staticmethod
    def ignore(fn, retries=1, exception=None):
            try:
                retries = retries - 1
                return fn()
                #??? move self.store() here? just to ensure that it tries to store at all, and to make it meaningful.
            except StorageExpectationError, e:
                pass
