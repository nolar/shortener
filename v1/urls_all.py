from django.conf.urls.defaults import patterns, include, url
from .views import redirect

urlpatterns = patterns('',
    url(r'^(.+)$', redirect),
)
