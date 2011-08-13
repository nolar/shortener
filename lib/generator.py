# coding: utf-8

class Generator(object):
    """
    Instances of this class are able to produce the ids for the urls. IDs are
    string with all the characters possible in URLs without being encoded.
    The main generation method is generate(). The generator can also be used
    with the built-in next(generator_instance) method.
    """
    
    #!!!TODO: as for now we just design th API rather than scalability features,
    #!!!TODO: so the generator is non-persistent and as simple as possible,
    #!!!TODO: just a placeholder for the future algirythms.
    counter = 0
    
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
        
        Generator.counter += 1
        return unicode(Generator.counter)

