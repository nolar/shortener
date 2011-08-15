# coding: utf-8
import datetime
from .decorators import as_json, as_html, as_redirector
from .setup import make_analytics, make_shortener


@as_redirector()
def redirect(request, id):
    shortener = make_shortener(request)
    resolved = shortener.resolve(id)
    return resolved.url

@as_json
@as_html('resolve.html')
def resolve(request):
    id = request.GET.get('id', None)
    if not id: raise ValueError("ID must be specified to be resolved.")
    
    shortener = make_shortener(request)
    return {
        'resolved': dict(shortener.resolve(id)),
    }

@as_json
@as_html('shorten.html')
def shorten(request):
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
def analytics_recent_targets(request):
    count = int(request.GET.get('count', 100))
    if count <= 0: raise ValueError("Count must be positive integer.")
    
    analytics = make_analytics(request)
    return {
        'count': count,
        'urls': analytics.recent_targets.retrieve(count),
    }

@as_json
@as_html('top_domains.html')
def analytics_popular_domains(request):
    count = int(request.GET.get('count', 10))
    if count <= 0: raise ValueError("Count must be positive integer.")
    
    days = int(request.GET.get('days', 30))
    if days <= 0: raise ValueError("Days must be positive integer.")
    
    analytics = make_analytics(request)
    return {
        'days': days,
        'count': count,
        'domains': analytics.popular_domains.retrieve(count, datetime.timedelta(days=days)),
    }
