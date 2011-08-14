# Create your views here.
import datetime
from shortener.decorators import as_json, as_html, as_redirector
from shortener.lib.shortener import Shortener
from shortener.lib.storages import FakeStorage, WrappedStorage, SdbStorage
from shortener.lib.generators import CentralizedGenerator
from django.conf import settings

def make_shortener(request):
    return Shortener(host=request.META.get('HTTP_HOST'), #!!! or DEFAULT_HOST?
                    sequences   = SdbStorage(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY, 'sequences' ),
                    #generators = SdbStorage(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY, 'generators'),
                    urls        = SdbStorage(settings.AWS_ACCESS_KEY, settings.AWS_SECRET_KEY, 'urls'      ),
                    )

@as_json
@as_html('resolve.html')
@as_redirector('url')
def resolve_view(request, id):
    shortener = make_shortener(request)
    return {
        'url': shortener.resolve(id)
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
        'short_url': short_url,
    }

@as_json
@as_html('last_urls.html')
def last_urls_view(request, n):
    shortener = make_shortener(request)
    return {
        'number': int(n),
        'urls': shortener.get_last_urls(int(n)),
    }

@as_json
@as_html('top_domains.html')
def top_domains_view(request):
    shortener = make_shortener(request)
    return {
        'days': 30,
        'domains': shortener.get_top_domains(datetime.timedelta(days=30)),
    }
