# coding: utf-8
from .daal.storages import StorageExpectationError, StorageItemAbsentError
import re

LOWERS = 'abcdefghijklmnopqrstuvwxyz'
UPPERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
DIGITS = '0123456789'
SPECIAL = '$-_.+!*\'(),' # these characters are safe for HTTP schema, thoug may be it recognized by highlighters.
SCHEMAS = ';&/:=@' # these characters in HTTP schema may be risky, since they are part of the schema.
ALPHABET = ''.join([LOWERS, UPPERS, DIGITS, SPECIAL])


class DepletedError(Exception): pass


class Generator(object):
    """
    Generator produces the ids, one by one. The purpose and ways of use of these IDs
    are not in responsivility of the generators. IDs are strings with all the characters
    possible in URLs without being encoded. The main generation method is generate().
    The generator can also be used with the built-in next(generator_instance) method.
    """

    def __init__(self):
        super(Generator, self).__init__()

    def __iter__(self):
        while True:
            yield self.generate()

    def generate(self):
        """
        Generates one identifier according to the built-in rules and algorythms.
        The identifier is always a string, no matter how exactly it was generated.
        """
        raise NotImplemented()


class FakeGenerator(Generator):
    """
    For simple prototyping and testing purposes, the simplest generator ever:
    * non-persistent (resets its state on each load of the system),
    * numeric only (i.e. no alphabet generation is used, just a numbers).
    """

    counter = 0

    def generate(self):
        FakeGenerator.counter += 1
        return unicode(FakeGenerator.counter)


class CentralizedGenerator(Generator):
    """
    Basic persistent generator. Stores its last generated value in the storage
    provided under the one single id, that never changes. Since it is bound to
    one single stored item, massive flow of requests will be limited by locking
    or write-expect'ing algorythms, being a bottleneck.
    """

    def __init__(self, storage, id='centralized', letters=None, prohibit=None):
        super(CentralizedGenerator, self).__init__()
        self.storage = storage
        self.id = id
        self.letters = letters
        self.prohibit = prohibit

    def generate(self):
        result = Sequence(self.storage, self.id, letters=self.letters, prohibit=self.prohibit).generate()
        return result


class Sequence(object):
    def __init__(self, storage, id, min_length=None, max_length=None, letters=None, retries=3, prohibit=None):
        super(Sequence, self).__init__()
        self.storage = storage
        self.retries = retries
        self.letters = letters or ALPHABET
        self.id = id
        self.min_length = min_length or 1
        self.max_length = max_length or 1024
        self.prohibit = re.compile(prohibit, re.X) if prohibit else None
        #!!!FIXME: this prohibition approach is not good, because you will stuch at "v0..." block,
        #!!!FIXME: since it is VERY LARGE and you'll iterate over all of it.
        #???TODO: possible solution is to prhibit some beginnings and endings only (v\d+/  & .html/.json)
        #???TODO: and also some sequences in the middle (//) -- this can be controlled _inside_ the
        #???TODO: Sequence recursion (i.e. not only on the resulting string) and be fastened easyly (probably).
        #???TODO: SequenceRules class with check_start, check_end, check_middle methods?

    def generate(self):
        item = self.storage.update(self.id, lambda data: {'value': self.increment(data.get('value', None))},
            field = 'value',#!!! this should be somehow removed
            retries = self.retries)
        return item.get('value', None)

    def increment(self, old_value):
        result = None
        while result is None or (self.prohibit and self.prohibit.search(result)):
            if old_value:
                pos = self.letters.find(old_value[-1])
                if pos == -1:
#                    raise SequenceLettersError("Unsupported character in the initial value of the sequence.")
                    pos = len(self.letters) - 1

                if pos < len(self.letters) - 1:
                    result = old_value[:-1] + self.letters[pos+1]
                else:
                    result = self.increment(old_value[:-1] or self.letters[0]) + self.letters[0]
                    if len(result) > self.max_length:
                        raise DepletedError("Sequence %s is depleted." % self.id)
            else:
                result = self.letters[0] * self.min_length
        return result

