import functools
import json
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response

def as_json(fn):
    @functools.wraps(fn)
    def decorated(request, *args, **kwargs):
        result = fn(request, *args, **kwargs)
        if isinstance(result, HttpResponse):
            return result
        elif '.json' in request.path:
            content = json.dumps(result, indent=4)
            return HttpResponse(content, mimetype='text/plain')#!!! mimetype is for manual debugging
        else:
            return result
    return decorated

def as_html(view):
    def decorator(fn):
        @functools.wraps(fn)
        def decorated(request, *args, **kwargs):
            result = fn(request, *args, **kwargs)
            if isinstance(result, HttpResponse):
                return result
            elif request.path.endswith('.html'):
                return render_to_response(view, result)
            else:
                return result
        return decorated
    return decorator

def as_redirector(field=None):
    def decorator(fn):
        @functools.wraps(fn)
        def decorated(request, *args, **kwargs):
            result = fn(request, *args, **kwargs)
            if isinstance(result, HttpResponse):
                return result
            else:
                if result is not None and isinstance(result, basestring):
                    return HttpResponseRedirect(result)
                else:
                    raise ValueError("Trying to redirect to a non-url (%s)." % repr(result))
        return decorated
    return decorator

