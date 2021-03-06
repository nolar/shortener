from django.conf.urls.defaults import patterns, include, url
from .views import resolve, shorten, analytics_recent_targets, analytics_popular_domains

urlpatterns = patterns('',
    url(r'^resolve.(?:html|json)$', resolve),
    url(r'^shorten.(?:html|json)$', shorten),
    url(r'^analytics/recent_targets.(?:html|json)$' , analytics_recent_targets ),
    url(r'^analytics/popular_domains.(?:html|json)$', analytics_popular_domains),
    url(r'', 'django.views.defaults.page_not_found'),
)
