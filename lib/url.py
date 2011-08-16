# coding: utf-8

class URL(dict):
    """
    Represent an URL item being handled in the system. It is not able to restore
    its state and has no any access to storages, etc, so as it does not know how
    the data are really stored. It is just an object to organize data fields and
    access them in a "object.field" notation. It inherits from dict to be easily
    used with JSON serializers, etc -- to mimic the built-in type.
    """
    
    def __init__(self, host, id, url, created_ts=None, remote_addr=None, remote_port=None, **kwargs):
        super(URL, self).__init__()
        self['host'] = host
        self['id'  ] = id
        self['url' ] = url
        self['created_ts' ] = float(created_ts) if created_ts is not None else created_ts
        self['remote_addr'] = remote_addr
        self['remote_port'] = remote_port
        self['shortcut'] = 'http://%s/%s' % (host, id)
        #??? optionally, if something left in kwargs, probably this is an error/warning?
    
    def __str__(self):
        return self.shortcut
    
    def __unicode__(self):
        return self.shortcut
    
    def __iter__(self):
        return iter(self.__dict__.items())
    
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("URL instance has no attribute '%s'." % name)
