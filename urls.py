from django.conf.urls.defaults import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

from django.views.defaults import page_not_found

urlpatterns = patterns('',
    # Version 1.*.* API call:
    url(r'^v1/resolve.json$', 'shortener.v1.views.resolve'),
    url(r'^v1/shorten.json$', 'shortener.v1.views.shorten'),
    url(r'^v1/analytics/recent_targets.json$' , 'shortener.v1.views.analytics_recent_targets' ),
    url(r'^v1/analytics/popular_domains.json$', 'shortener.v1.views.analytics_popular_domains'),
    url(r'^v1/', 'django.views.defaults.page_not_found'),
    
    # Univertal catcher for encoded urls (non-versioned, thus universal):
    url(r'(.+)$', 'shortener.v1.views.redirect'),
)
