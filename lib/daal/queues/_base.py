# coding: utf-8

class Queue(object):
    def __init__(self):
        super(Queue, self).__init__()
    
    def push(self, item):
        raise NotImplemented()
    
    def pull(self):
        raise NotImplemented()
    
    def delete(self, item):
        raise NotImplemented()
