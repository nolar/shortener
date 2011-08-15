# Create your views here.
import datetime
from shortener.decorators import as_json, as_html, as_redirector
from shortener.lib.shortener import AWSShortener
from shortener.lib.statistics import AWSStats
from django.conf import settings

from shortener.lib.statistics import Stats, LastUrls, TopDomains
from shortener.lib.storages import SdbStorage


def make_shortener(request):
    return AWSShortener(host=request.META.get('HTTP_HOST'), #!!! or DEFAULT_HOST?
                        access_key = settings.AWS_ACCESS_KEY,
                        secret_key = settings.AWS_SECRET_KEY,
                        )

def make_stats(request):
    return AWSStats(host=request.META.get('HTTP_HOST'), #!!! or DEFAULT_HOST?
                    access_key = settings.AWS_ACCESS_KEY,
                    secret_key = settings.AWS_SECRET_KEY,
                    )

@as_json
@as_html('resolve.html')
@as_redirector('url')
def resolve_view(request, id):
    shortener = make_shortener(request)
    return {
        'resolved': dict(shortener.resolve(id)),
    }

@as_json
@as_html('shorten.html')
def shorten_view(request):
    shortener = make_shortener(request)
    long_url = request.GET.get('url', None)
    short_url = shortener.shorten(long_url,
                                    remote_addr=request.META.get('REMOTE_ADDR'),
                                    remote_port=request.META.get('REMOTE_PORT'))
    return {
        'long_url': long_url,
        'short_url': short_url.shortcut,
    }

@as_json
@as_html('last_urls.html')
def last_urls_view(request, n):
    stats = make_stats(request)
    return {
        'number': int(n),
        'urls': stats.last_urls.retrieve(int(n)),
    }

@as_json
@as_html('top_domains.html')
def top_domains_view(request):
    stats = make_stats(request)
    return {
        'days': 30,
        'number': 10,
        'domains': stats.top_domains.retrieve(10, datetime.timedelta(days=30)),
    }
