import os
import sys

sys.path.append('/var/www')
sys.path.append('/var/www/shortener')

os.environ['DJANGO_SETTINGS_MODULE'] = 'shortener.settings'

import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()
