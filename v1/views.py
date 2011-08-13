# Create your views here.
import datetime
from shortener.decorators import as_json, as_html, as_redirector
from shortener.lib.shortener import Shortener


@as_json
@as_html('resolve.html')
@as_redirector('url')
def resolve_view(request, id):
    return {
        'url': Shortener().resolve(id)
    }

@as_json
@as_html('shorten.html')
def shorten_view(request):
    long_url = request.GET.get('url', None)
    short_url = Shortener().shorten(long_url)
    return {
        'long_url': long_url,
        'short_url': short_url,
    }

@as_json
@as_html('last_urls.html')
def last_urls_view(request, n):
    return {
        'number': int(n),
        'urls': Shortener().get_last(int(n)),
    }

@as_json
@as_html('top_domains.html')
def top_domains_view(request):
    return {
        'days': 30,
        'domains': Shortener().get_top(datetime.timedelta(days=30)),
    }
