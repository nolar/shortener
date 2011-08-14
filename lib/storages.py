# coding: utf-8
from boto.exception import SDBResponseError
from boto.sdb.connection import SDBConnection


class StorageItemAbsentError(Exception): pass
class StorageUniquenessError(Exception): pass
class StorageExpectationError(Exception): pass


class Storage(object):
    def __init__(self):
        super(Storage, self).__init__()
    
    def store_url(self, id, url):
        raise NotImplemented()
    
    def fetch_url(self, host, id):
        raise NotImplemented()


class WrappedStorage(Storage):
    def __init__(self, storage, format='%(prefix)s%(id)s%(suffix)s', prefix='', suffix=''):
        super(WrappedStorage, self).__init__()
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
    
    def _wrap_id(self, id):
        if isinstance(id, basestring):
            return self.format % {'prefix': self.prefix, 'suffix': self.suffix, 'id': id }
        else:
            return [self._wrap_id(i) for i in id]


class FakeStorage(Storage):
    data = {}
    
    def store(self, id, value, expect=None, unique=None):
        if unique is not None and expect is not None:
            raise ValueError("Only expect or exists parameter maybe passed, not both.")
        #elif expect is not None:
        #    expect_field, expect_value = expect.items()[0]
        #elif unique is not None:
        #    expect_field = unique if isinstance(unique, basestring) else 'id'
        #    expect_value = bool(unique)
        
        if unique is not None:
            if unique and id in FakeStorage.data:
                raise StorageUniquenessError("Storage item '%s' already exists." % id)
        
        if expect is not None:
            expect_field, expect_value = expect.items()[0]
            real_value = FakeStorage.data.get(id, {}).get(expect_field, None)
            if real_value != expect_value:
                raise StorageExpectationError("Storage expecation failed.")
        
        FakeStorage.data[id] = value
    
    def fetch(self, id):
        try:
            return FakeStorage.data[id]
        except KeyError, e:
            raise StorageItemAbsentError("The item '%s' is not found." % id)


class SdbStorage(Storage):
    
    def __init__(self, access_key, secret_key, domain_name):
        super(SdbStorage, self).__init__()
        self.domain_name = domain_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.connected = False
        self.connection = None
        self.domain = None

    def store(self, id, value, expect=None, unique=None):
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
        
        self.connect()
        try:
            value['id'] = id
            self.domain.put_attributes(id, value, expected_value=expect)
        except SDBResponseError, e:
            if e.code == 'ConditionalCheckFailed':
                raise StorageExpectationError("Storage expecation failed.")
            else:
                raise e
    
    def fetch(self, id):
        self.connect()
        if isinstance(id, basestring):
            item = self.domain.get_attributes(id, consistent_read=True)
            if not item:
                raise StorageItemAbsentError("The item '%s' is not found." % id)
            return item
        else:
            ids = id
            ids_str = ','.join(["'%s'" % id for id in ids])#!!!! ad normal escaping
            #!!! handle when ids is an empty list - return empty result set
            items = list(self.domain.select('SELECT * FROM %s WHERE itemName() in (%s)' % (self.domain.name, ids_str)))
            return items
    
    @staticmethod
    def repeat(fn, retries=1, exception=None):
        while retries > 0:
            try:
                retries = retries - 1
                return fn()
            except StorageExpectationError, e:
                if retries <= 0:
                    if callable(exception):# includes types and classes
                        raise exception(e) or e
                    else:
                        raise exception or e
    
    def connect(self):
        if not self.connected:
            self.connection = SDBConnection(self.access_key, self.secret_key)
            try:
                self.domain = self.connection.get_domain(self.domain_name)
            except SDBResponseError:
                self.domain = self.connection.create_domain(self.domain_name)
            self.connected = True
        return self
