# coding: utf-8

__all__ = ['Storage', 'StorageItemAbsentError', 'StorageExpectationError', 'StorageUniquenessError']

class StorageItemAbsentError(Exception): pass
class StorageUniquenessError(Exception): pass
class StorageExpectationError(Exception): pass

class Storage(object):
    """
    Base class for all storages.
    """
    
    def __init__(self):
        super(Storage, self).__init__()
    
    def store(self, id, url):
        raise NotImplemented()
    
    def fetch(self, host, id):
        raise NotImplemented()
    
    def query(self, columns=None, where=None, order=None, limit=None):
        raise NotImplemented()
    
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
