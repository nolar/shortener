from django.conf.urls.defaults import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

from django.views.defaults import page_not_found

urlpatterns = patterns('',
    # V1 API call:
    url(r'^v1/json/resolve/(.+)$', 'shortener.v1.views.resolve_view'),
    url(r'^v1/json/shorten/$', 'shortener.v1.views.shorten_view'),
    url(r'^v1/json/last/(\d+)/$', 'shortener.v1.views.last_urls_view'),
    url(r'^v1/json/top/$', 'shortener.v1.views.top_domains_view'),
    url(r'^v1/', 'django.views.defaults.page_not_found'),
    
    # Univertal catcher for encoded urls (non-versioned, thus universal):
    url(r'(.+)$', 'shortener.v1.views.resolve_view'),
)
