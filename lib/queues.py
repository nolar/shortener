

class Queue(object):
    def __init__(self):
        super(Queue, self).__init__()
    
    def push(self, item):
        raise NotImplemented()
    
    def pull(self):
        raise NotImplemented()
    
    def delete(self, item):
        raise NotImplemented()


from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import json

class SQSQueue(Queue):
    def __init__(self, access_key, secret_key, region=None, name=None):
        super(SQSQueue, self).__init__()
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.name = name
        self.connected = False
        self._messages = {}
    
    def connect(self):
        if not self.connected:
            self.connection = SQSConnection(self.access_key, self.secret_key, region=self.region)
            self.queue = self.connection.create_queue(self.name)
            self.connected = True
        return self
    
    def push(self, item):
        body = json.dumps(item)
        
        message = Message()
        message.set_body(body)
        
        self.connect()
        self.queue.write(message)
    
    def pull(self, timeout=10, factory=lambda x:x):
        self.connect()
        message = self.queue.read(timeout)
        if message is not None:
            body = message.get_body()
            data = json.loads(body)
            item = factory(data)
            
            # Mark for futher deletion, but do not refer to it from ouselves (for garbage collectors).
            item.__dict__['_queue_'] = self.queue
            item.__dict__['_message_'] = message
            
            return item
        else:
            return None
    
    def delete(self, item):
        self.connect()
        
        queue = item.__dict__.get('_queue_', None)
        message = item.__dict__.get('_message_', None)
        
        if queue is None:
            raise Exception("Cannot delete an item, since it is not from a queue.")
        if queue is not self.queue:
            raise Exception("Cannot delete an item, since it is not from my queue.")
        
        self.queue.delete_message(message)
