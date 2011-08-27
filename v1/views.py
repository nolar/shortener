# coding: utf-8
import datetime
from .decorators import with_profile, as_json, as_html, as_redirector
from .setup import make_analytics, make_shortener
from lib.shortener import ShortenerIdAbsentError
from django.http import Http404


@as_redirector()
def redirect(request, id):
    try:
        shortener = make_shortener(request)
        resolved = shortener.resolve(id)
        return resolved.url
    except ShortenerIdAbsentError, e:
        raise Http404()

@as_json
@as_html('v1/resolve.html')
@with_profile
def resolve(request):
    id = request.GET.get('id', None)
    if not id: raise ValueError("ID must be specified to be resolved.")
    
    shortener = make_shortener(request)
    shortened = shortener.resolve(id)
    return {
        'shortened': shortened,
    }

@as_json
@as_html('v1/shorten.html')
@with_profile
def shorten(request):
    url = request.GET.get('url', None)
    if not url: raise ValueError("URL is required.")
    
    shortener = make_shortener(request)
    shortened = shortener.shorten(url,
                                    remote_addr=request.META.get('REMOTE_ADDR'),
                                    remote_port=request.META.get('REMOTE_PORT'))
    return {
        'shortened': shortened,
    }

@as_json
@as_html('v1/recent_targets.html')
@with_profile
def analytics_recent_targets(request):
    count = int(request.GET.get('count', 100))
    if count <= 0: raise ValueError("Count must be positive integer.")
    
    analytics = make_analytics(request)
    return {
        'count': count,
        'urls': analytics.recent_targets.retrieve(count),
    }

@as_json
@as_html('v1/popular_domains.html')
@with_profile
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
