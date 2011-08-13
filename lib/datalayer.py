# coding: utf-8

class StorageItemExists(Exception): pass
class StorageItemAbsent(Exception): pass

class UrlItem(object):
    def __init__(self, url, create_ts=None, remote_addr=None, remote_port=None):
        super(UrlItem, self).__init__()
        self.url = url
        self.create_ts = create_ts
        self.remote_addr = remote_addr
        self.remote_port = remote_port


class UrlStorage(object):
    def __init__(self):
        super(UrlStorage, self).__init__()
    
    def store_url(self, host, id, url):
        raise NotImplemented()
    
    def fetch_url(self, host, id):
        raise NotImplemented()


class FakeUrlStorage(UrlStorage):
    data = {}
    
    def store_url(self, host, id, item):
        key = '%s_%s' % (host, id)
        if key in FakeUrlStorage.data:
            raise StorageItemExists()
        else:
            FakeUrlStorage.data[key] = item
    
    def fetch_url(self, host, id):
        key = '%s_%s' % (host, id)
        try:
            return FakeUrlStorage.data[key]
        except KeyError, e:
            raise StorageItemAbsent()


class SdbUrlStorage(UrlStorage):
        #!!! utilize expected_value=['id', False] to be sure that item does not exists yet
        #!!! raise if it exists. retry if the id is generated. fail after threshold amount of retries.
    pass
