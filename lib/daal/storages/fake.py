# coding: utf-8
from ._base import Storage
from ._base import StorageExpectationError, StorageItemAbsentError, StorageUniquenessError

__all__ = ['FakeStorage']

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
