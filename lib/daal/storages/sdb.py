# coding: utf-8
from ._base import Storage
from ._base import StorageExpectationError, StorageItemAbsentError, StorageUniquenessError
from boto.exception import SDBResponseError
from boto.sdb.connection import SDBConnection

__all__ = ['SDBStorage']

class SDBStorage(Storage):
    
    def __init__(self, access_key, secret_key, domain_name):
        super(SDBStorage, self).__init__()
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
            #!!! handle when ids is an empty list - return empty result set
            ids = id
            items = []
            for i in xrange(0, len(ids) / 20 + 1):
                ids_slice = ids[i*20:(i+1)*20]
                ids_str = ','.join(["'%s'" % id for id in ids_slice])#!!!! ad normal escaping
                query = 'SELECT * FROM %s WHERE itemName() in (%s)' % (self.domain.name, ids_str)
                items.extend(self.domain.select(query))
            return items
    
    def connect(self):
        if not self.connected:
            self.connection = SDBConnection(self.access_key, self.secret_key)
            try:
                self.domain = self.connection.get_domain(self.domain_name)
            except SDBResponseError:
                self.domain = self.connection.create_domain(self.domain_name)
            self.connected = True
        return self
