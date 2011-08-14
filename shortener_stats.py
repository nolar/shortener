import sys
import time
from lib.queues import SQSQueue
import settings
from lib.shortener import Shortener, AWSShortener
from lib.storages import SdbStorage

class UrlMessage(object):
    def __init__(self, data):
        self.__dict__.update(data)


def main():
    # attach to the queue, handle each message in cycle:
    queue = SQSQueue(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY, name='urls')
    while True:
        # extract host & id from the message, restore the instance
        item = queue.pull(factory=UrlMessage)
        if item is None:
            time.sleep(1)
            continue
        
        print(item.host, item.id)
        shortener = AWSShortener(host = item.host,
                                access_key = settings.AWS_ACCESS_KEY,
                                secret_key = settings.AWS_SECRET_KEY,
                                )
        resolved = shortener.resolve(item.id)
        shortener.update_stats(resolved)
        
        # delete the message from the queue
        queue.delete(item)


if __name__ == '__main__':
    main()