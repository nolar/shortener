# coding: utf-8

class Item(dict):
    """
    Item is a base class for all items that can be stored in the storages & queues.
    Used mostly as a syntax sugar to support both dict-like and object-like access,
    where item['field'] and item.field are equivalent. Descendants of this class
    can define stricter set of fields, with all others raising an error as usually.

    This class intentionally inherits from built-in dict class for ease of type-casting
    and serialization (e.g., to JSON or to internal representations of storages/queues)
    with no additional use of serializers or encoders. Note that constructor is not
    dict-compatible and varies for different classes of items.

    Also note that this item class is only for grouping field values and using them
    as a single entity/object. It has no any specific behavior, such as saving itself
    to a storage or validating its fields, etc. This is usually done in other classes.
    """

    def __init__(self):
        super(Item, self).__init__()

    def __iter__(self):
        return iter(self.items())

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("Item %s has no attribute '%s'." % (self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name in self:
            self[name] = value
        else:
            raise AttributeError("Item %s has no attribute '%s'." % (self.__class__.__name__, name))
