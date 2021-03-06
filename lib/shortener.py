# coding: utf-8
from .daal.storages import StorageItemAbsentError, StorageExpectationError
from .url import URL
import time


class ShortenerIdAbsentError(Exception): pass
class ShortenerIdExistsError(Exception): pass
class ShortenerBadUrlError(Exception): pass


class Shortener(object):
    """
    API methods to shorten long urls and to resolve short ones.

    Shortener accepts a set of pre-instantiated storages, and each of these storages has
    its own specific purpose and data structure.

    It also accepts an instance of Analytics or Notifier class and calls it when new url is added.
    All real analytical operations are in Analytics & Dimension classes and their descendants.
    Notifier is used for delayed updates of the dimensions, with use of queue & daemon.

    Note that Shortener knows nothing about multi-hosted services. This feature is implemented
    as a wrapper for storages, and is initialized when creating the shortener instance. All other
    storages and shortener just do their job within their simplified responsibilities.
    """

    def __init__(self, storage, registry, generator):
        super(Shortener, self).__init__()
        self.storage = storage
        self.registry = registry
        self.generator = generator

    def resolve(self, id):
        """
        Resolves the short id. If you want to resolve the url, you should
        first parse it into a host & id parts, and the ask proper shortener
        instance (specific for each host) to resolve that id.

        Returns an URL instance with all fields fulfilled.
        """

        try:
            item = self.storage.fetch(id)
            #todo later: we can add checks for moderation status here, etc.
            return URL(**item)
        except StorageItemAbsentError, e:
            raise ShortenerIdAbsentError("Such url does not exist.")

    def shorten(self, url, id_wanted=None, retries=10, remote_addr=None, remote_port=None):
        """
        Shortens the long url and saves it with the id requested or generated.

        Returns an URL instance with all fields fulfilled as if it has been resolved.
        """

        #??? do we care? maybe that is a view's responsibility to validate the input?
        if '://' not in url or len(url) > 8*1024:
            raise ShortenerBadUrlError("URL is not an URL?")

        # Despite that generator guaranties unique ids within that generator,
        # there could be other urls stored already with this id. For example,
        # if the url was stored with the manually specified id earlier; or if
        # there was another generator algorithm before. The only way to catch
        # these conflicts is to try to store, and see if that was successful
        # (note that the generators should not know the purpose of the id and
        # cannot check for uniqueness by themselves; that would not help, btw).
        try:
            def gen_data():
                code = id_wanted or self.generator.generate()
                return URL(
                    code = code,
                    url = url,
                    created_ts = int(time.time()),
                    remote_addr = remote_addr,
                    remote_port = remote_port,
                )
            shortened_url = self.storage.create(gen_data,
                retries=retries if not id_wanted else 1,
            )
        except StorageExpectationError, e:
            if id_wanted:
                raise ShortenerIdExistsError("This id exists already, try another one.")
            else:
                raise

        # Notify the registries that a new url has been born. Let them torture it a bit.
        # Registries update the "last urls" and "top domains" structures, in particular.
        # Depending on what registry we were initialized with, the update can happen
        # immediately (if that was an instance of analytics) or be delayed (a notifier).
        self.registry.register(shortened_url)

        # Return shortened URL to the caller.
        return shortened_url
