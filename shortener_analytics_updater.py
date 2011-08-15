import sys
import time
import settings
from lib.daal.queues import SQSQueue
from v1.setup import AWSAnalytics, AWSShortener
import traceback


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
        
        try:
            #??? can we avoid fetching from the storage? can we store all necessary info in the queue itself?
            print(item.host, item.id)
            shortener = AWSShortener(host = item.host,
                                    access_key = settings.AWS_ACCESS_KEY,
                                    secret_key = settings.AWS_SECRET_KEY,
                                    )
            resolved = shortener.resolve(item.id)
            
            analytics = AWSAnalytics(host = item.host,
                                    access_key = settings.AWS_ACCESS_KEY,
                                    secret_key = settings.AWS_SECRET_KEY,
                                    )
            analytics.update(resolved)
            
            # delete the message from the queue
            queue.delete(item)
        except Exception, e:
            traceback.print_exc()


if __name__ == '__main__':
    main()
