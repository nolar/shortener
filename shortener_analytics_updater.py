#!/usr/bin/python
import sys
import time
import settings
from lib.daal.queues import SQSQueue
from lib.url import URL
from v1.setup import AWSAnalytics, AWSShortener
import traceback


def main():
    # attach to the queue, handle each message in cycle:
    queue = SQSQueue(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY, name='urls')
    while True:
        try:
            # extract host & id from the message, restore the instance
            item = queue.pull(factory=lambda data: URL(**data))
            if item is None:
                time.sleep(1)
                continue

            print(item.host, item.id)
            analytics = AWSAnalytics(host = item.host,
                                    access_key = settings.AWS_ACCESS_KEY,
                                    secret_key = settings.AWS_SECRET_KEY,
                                    )
            analytics.update(item)

            # delete the message from the queue
            queue.delete(item)
        except Exception, e:
            print('=' * 80)
            traceback.print_exc()


if __name__ == '__main__':
    main()
