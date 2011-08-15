# coding: utf-8
"""
This module contains pre-configured layout for the shortener system.
All views and daemons use these pre-configured classes (or factories) only
to access the data stored in the system. No other classes except these ones!
"""

from lib.shortener import Shortener
from lib.analytics import Analytics
from lib.dimensions import RecentTargetsDimension, PopularDomainsDimension
from lib.daal.storages import SDBStorage, WrapperStorage
from lib.daal.queues import SQSQueue
from django.conf import settings


class AWSShortener(Shortener):
    def __init__(self, access_key, secret_key, host):
        # Since shorteners for different hosts are isolated, wrap all the storages with hostname prefix.
        #!!! be sure that host is NORMALIZED, i.e. "go.to:80"  === "go.to", to avoid unwatned errors.
        #!!! probably, check with the list of available host domains in the config db.
        super(AWSShortener, self).__init__(host,
            sequences   = WrapperStorage(SDBStorage(access_key, secret_key, 'sequences' ), prefix=host+'_'),
            #generators = WrapperStorage(SDBStorage(access_key, secret_key, 'generators'), prefix=host+'_'),
            urls        = WrapperStorage(SDBStorage(access_key, secret_key, 'urls'      ), prefix=host+'_'),
            shortened_queue = SQSQueue(access_key, secret_key, name='urls'),
            statistics = AWSAnalytics(access_key, secret_key, host),
            )


#??? shouldn't analytics be a part of shortener? and accessed through a shortener?
class AWSAnalytics(Analytics):
    def __init__(self, access_key, secret_key, host):
        super(AWSAnalytics, self).__init__(
            recent_targets  =  RecentTargetsDimension(WrapperStorage(SDBStorage(access_key, secret_key, 'last_urls'  ), prefix=host+'_')),
            popular_domains = PopularDomainsDimension(WrapperStorage(SDBStorage(access_key, secret_key, 'top_domains'), prefix=host+'_')),
        )


def make_shortener(request):
    return AWSShortener(host=request.META.get('HTTP_HOST'), #!!! or DEFAULT_HOST?
                        access_key = settings.AWS_ACCESS_KEY,
                        secret_key = settings.AWS_SECRET_KEY,
                        )

def make_analytics(request):
    return AWSAnalytics(host=request.META.get('HTTP_HOST'), #!!! or DEFAULT_HOST?
                    access_key = settings.AWS_ACCESS_KEY,
                    secret_key = settings.AWS_SECRET_KEY,
                    )
