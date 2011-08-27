# coding: utf-8
"""
This module contains pre-configured layout for the shortener system.
It describes all the parameters and all the connections between components.
All views and daemons should use only these pre-configured classes/factories.
No other classes/factories except these ones - to keep the system consistent!
"""
from lib.shortener import Shortener
from lib.generators import CentralizedGenerator
from lib.registries import Analytics, Notifier
from lib.dimensions import RecentTargetsDimension, PopularDomainsDimension
from lib.daal.storages import SDBStorage, WrappedStorage
from lib.daal.queues import SQSQueue
from django.conf import settings


class AWSShortener(Shortener):
    def __init__(self, access_key, secret_key, host):
        super(AWSShortener, self).__init__(
            storage   = WrappedStorage(SDBStorage(access_key, secret_key, 'urls'), host=host),
            registry  = AWSAnalytics(access_key, secret_key, host),
#            registry  = AWSNotifier (access_key, secret_key, host),
            generator = AWSGenerator(access_key, secret_key, host),
            )

class AWSGenerator(CentralizedGenerator):
    def __init__(self, access_key, secret_key, host):
        super(AWSGenerator, self).__init__(
            storage = WrappedStorage(SDBStorage(access_key, secret_key, 'sequences'), host=host),
            prohibit=r'(^v\d+/) | (^/) | (//)',
        )

class AWSAnalytics(Analytics):
    def __init__(self, access_key, secret_key, host):
        super(AWSAnalytics, self).__init__(
            recent_targets  =  RecentTargetsDimension(WrappedStorage(SDBStorage(access_key, secret_key, 'last_urls'  ), host=host)),
            popular_domains = PopularDomainsDimension(WrappedStorage(SDBStorage(access_key, secret_key, 'top_domains'), host=host)),
        )

class AWSNotifier(Notifier):
    def __init__(self, access_key, secret_key, host):
        super(AWSNotifier, self).__init__(
            queue = SQSQueue(access_key, secret_key, name='urls'),
        )

def get_host(request):
    host = request.META.get('HTTP_HOST') #!!! or DEFAULT_HOST?
    host = host.lower()
    host = host[4:] if host.startswith('www.') else host
    host = host[:-3] if host.endswith(':80') else host
    return host

def make_shortener(request):
    return AWSShortener(host=get_host(request),
                        access_key = settings.AWS_ACCESS_KEY,
                        secret_key = settings.AWS_SECRET_KEY,
                        )

def make_analytics(request):
    return AWSAnalytics(host=get_host(request),
                    access_key = settings.AWS_ACCESS_KEY,
                    secret_key = settings.AWS_SECRET_KEY,
                    )

