from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns('',
    url(r'^v1/', include('shortener.v1.urls_api')),
    url(r'^$'  , include('shortener.v1.urls_api')),
    url(r''    , include('shortener.v1.urls_all')),
)
