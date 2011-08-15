# coding: utf-8

class URL(object):
    """
    Represent an URL item being handled in the system. It is not able to restore
    its state and has no access to any storages, etc, so as it does not know how
    the data are really stored. It is just an object to organize data fields and
    access them in a "object.field" notation.
    """
    
    def __init__(self, host, id, url, created_ts=None, remote_addr=None, remote_port=None, **kwargs):
        super(URL, self).__init__()
        self.host = host
        self.id = id
        self.url = url
        self.created_ts = created_ts
        self.remote_addr = remote_addr
        self.remote_port = remote_port
        #??? optionally, if something left in kwargs, probably this is an error/warning?
    
    def __str__(self):
        return self.shortcut
    
    def __unicode__(self):
        return self.shortcut
    
    def __iter__(self):
        return iter(self.__dict__.items())
    
    @property
    def shortcut(self):
        return 'http://%s/%s' % (self.host, self.id)
