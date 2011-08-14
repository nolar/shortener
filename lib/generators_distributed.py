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


class DistributedGenerator(Generator):
    def __init__(self, host, storage):
        super(DistributeGenerator, self).__init__()
        self.storage = storage
        self.host = host
    
    def generate(self):
        root = DistributedRoot(self.storage, self.host)
        return root.generate()


"""
CONCEPTS:

The id is built as a concatenation of cluster ids.
Each cluster id is a 1-2-3 chars string (fixed per-host configurable length).
The last cluster is of variable length (1..1-2-3 chars).

For example, if the cluster length is 2, then:
"" -> no parent clusters and own cluster id is ''
"a" -> no parent, own="a"
"ab" -> no parent, own="ab"
"abc" -> parent="ab", own="x" <---- NOTE HERE
"abcd" -> parent="ab", own="cd"
"abcde" -> grandparent="ab", parent="cd", own="e"
"abcdef" -> grandparent="ab", parent="cd", own="ef"
OPTION: own length is 0..N-1, so we always have a parent

This way we always avoid collisions (["aa", "a"] vs ["a", "aa"]), but still
utilize all the space fo chars (i.e. no gaps in the lengths).

Once the cluster is full (all children possible combinations are used),
the next id is generated for the cluster. Cluster of the same level can
exist in parallel. When we need to generate new id for the cluster, we lock
inside its parent cluster (so, each cluster is a sequental generator of ids
for its children).

THE KEY POINTS (HERE IS THE MAGIC):
Each cluster holds its own state. So we avoid having one single lock for the whole
system, and lock only inside the cluster. The higher level the cluster has,
the more rarely its lock is used.

General algorythm for the cluster id generation:
* Input: child or an external service, who asked us for an id
* Input: parent cluster with id field
* Generate the lock item id as 'lock_'+''.join(parent_ids)
* Lock the parent cluster for id generation
  * (now we are safe within parent)
  * Fetch parent's state and the "last" field
  * Increment according to the algo
  * If that was the last combination:
    * Ask you own parent to generate next id (so, we change the parent so far)
    * Repeat inside that parent (it has the very starting id in "last")
  * Store the parent's new "last" field (verify it has not changed)
  * If it has been changed [no-lock, but expect_value algo] - then repeat again from "fetch" step
  * Unlock the parent cluster for id generation
* (Now we have new cluster id, and it is guaranted not to b duplicated)
* ??? Store own own state, unless we are the last level cluser


In the per-request basis, we have to find a set of available clusters to join.
Otherwise, all the request will go to the "tail" cluster, and there will be a bottleneck.
So, we have to know which clusters are ready to generate the ids.




==== STARTING FROM SCRATCH AGAIN


The simplest and straightforward way to geenrate the unique but sequental ids is:
Create a single "last" counter, aquire a lock each time, increment the value, then unlock.
In case of expect_value algorythm: read, calculate, try to write, if failed - then repeat.

When we start a flow of such requests, all the threads/processes are either wait for a lock
or are repeating the read-calc-trywrite cycle (equivalent to a lock, but with no "block").

To resolve this bottlenect, we can distribute the requesters among few locks, and
each of the locks will implement the same basic algorythm with lock-incr-unlock
(read-incr-trywrite). But since a much fewer amount of requesters a stuck at
one single lock, the workflow in common will become more smooth.

So, to build this distribution mechanism, we create a parent node, which is used
mostly for read. It contains information on which actual child locks to use.
The requesters select their lock randomly from the pool of available locks in
that parent node; maybe weighted - depends on what information the root node provide.

Each of the children locks has a limited number of allowed values to generate (a sequence).
When that sequence is exhausted, this lock reports to the root node that it is dead
and could not accept the requesters anymore. The root node removes that sequence
from the pool, and tries to restore the pool capacity with the next generated lock.

If the parent node cannot generate the next child lock, since it has been depleted too,
it reports to the its own parent (grand-parent for the original node), to do
the same re-distribution of the requesters.

If it was the very root node, then it creates the new root node, makes itself as
a child of that node, asks the node to populate more children, and shifts the root
pointer to that node. This operation happens really rare, when the id switches to
the next length when the current length capacity is going to end.

Thus, we have three stages and algorythms for the generator to work:
* Selection of the node to ask for a sequence.
* Sequence generation itself (with a lock or expect_value protection).
* Re-distribution of the tree by signals from the children nodes to the parent ones.

The greater the depth of the tree, the more read operation the requester must do
do retrieve the id (going down the tree). But the less and less of requesters
ends with the same sequence and same lock.

The whole tree is parametrized with these options:
* The amount of allowed ids per sequence (the length of the part).
* The size of the pool of the children in each node.

To make the tree probabilistic:
* The step of the depletion percentage when the node reports to its parent to restructure the tree.
* The threshold of depletions when the root node creates new root node and switches to it.

The overall structure of the id can be as follows:
<empty_root_part> + <level_1_part> + ... + <level_N_part>



"""

class Sequence(object):
    def __init__(self, storage, id, length=2, letters='abc', retries=3):
        super(Sequence, self).__init__()
        self.storage = stroage
        self.retries = retries
        self.letters = letters
        self.length = length
        self.id = id
    
    def generate(self):
        key = 'sq_' + '_'.join(self.id)
        retries = self.retries
        while retries > 0:
            try:
                item = self.storage.fetch(host, key) #!!!consistent!
                old_value = item.get('value', '')
                new_value = self.increment(old_value)
                self.storage.store_expect(host, key, {'value': new_value}, expect={'value': old_value})
                return new_value
            except StorageExpectError, e:
                retries = retries - 1
                if retries == 0:
                    raise e
    
    def increment(self, old_value):
        border = 1
        if old_value:
            last_char = old_value[-1]
            pos = self.letters.find(last_char)
            if pos == -1:
                raise SequenceLettersError("Unsupported character in the initial value of the sequence.")
            elif pos < len(self.letters) - 1:
                return old_value[:-1] + self.letters[pos+1]
            else:
                return self.increment(old_value[:-1]) + self.increment('')
        else:
            raise DepletedError("Sequence %s is depleted." % self.id)


class DChildren(object):
    """ List of the children. """
    def __init__(self, storage, host, id):
        pass # id is an id of the node with this list of the children
    
    def choose(self):
            # First, consistently load the children (even if changed in other process).
            item = self.storage.fetch(self.host, self.id)
            children = item.get('children', '')
            children = children.split('+')#!!!
            child_id = children[random()] if children else None
            return child_id
    
    def append(self, id):
        pass #+save
        item['children'] = '+'.join(children)
        self.storage.store_expect(self.host, self.id, item, expect={'children':old_children})
    
    def remove(self, id):
        pass #+save


class DNode(object):
    def generate(self):
        if is_leaf:
            # The leaf node generates directly from its sequence, with no tree logic.
            # If the sequence of the leaf node is depleted, we have no way to recover.
            # So we just pass-through the depletion error to the caller (parent or root).
            return Sequence(self.storage, self.host, self.id).generate()
        else:
            # Non-leaf nodes use their sequence for generation of children ids only.
            # The generation of id is delegated to one of the children, until the leafs.
            children = DChildren(self.storage, self.host, self.id)
            child_id = children.choose()
            
            # In case we have no children to choose from, it means this node has been depleted.
            #??? try to populate for the new node? Or every node is pre-populated?
            if child_id is None:
                raise DepletedError()
            
            # If we have children, then choose the random one, and delegate the request to it.
            try:
                return DNode(self.storage, self.host, child_id).generate()
            except DepletedError, e:
                # If the children has been depleted, try to restructure the node
                # by removing that children out of sight and adding a new one.
                # If there are no more children to add, call to generate() will
                # raise a depletion error again, and we are not going to handle it.
                self.restruct(child)
                return self.generate()
    
    def restruct(self, child, retries=3):
        #!!! all this try-write logic should be moved to storage layer/methods: add_child(id)/remove_child(id)
        while True:# actually, while retries > 0
            try:
                children = DChildren(self.storage, self.host, self.id)
                children.remove(child.id)
                
                children.populate()#???!!!
                #children.append()
            except StorageExpectationError, e:
                retries -= 1
                if retries <= 0:
                    raise e
    
    def populate(self):
        #while True:#??? or until the len(children) < desired_children_count
        #??? except DepletionError: pass/break?
        new_child_id = Sequence(self.storage, self.host, self.id).generate()
        new_child = DNode(self.storage, self.host, new_child_id)
        new_child.populate()
        item['children'].append(new_child_id)


class DistributedNode(object):
    def __init__(self, storage, host, id):
        super(DistributedNode, self).__init__()
        self.storage = storage
        self.host = host
        self.id = id
    
    def generate(self):
        # restore the state of the node by id
        #   read the pool of avilable children
        item = self.storage.fetch(self.host, self.id)
        children = item.get('children')
        if is_leaf:#!!! make as different classes? how to distinguish? is it possible?
            try:
                sequence = Sequence(self.daal.sequences, self.host, self.id)
                return sequence.generate()
            except DepletedError, e:
                # pass-throu the exception to the parent; maybe change the type
                raise e
        else:
            #select one randomly (weighted)
            child_id = children[random.rand(0, len(children))]
            child_node = self.__class__(self.storage, self.host, child_id)
            
            #ask the child to generate()
            try:
                return child_node.generate()
            except DepletedError, e:
                #remove depleted child from the pool
                children.remove(child_id)
                
                #try to add new child to the pool
                #    generate new child id from our own sequence
                sequence = Sequence(self.daal.sequences, self.host, self.id)
                ##if children generation sequence is depleted && no more children to chose from:
                ##    raise DepletedError
                ##    but if have no parent, then create it (or in the DsitributedGenerator/DistributedRoot?)
                child_id = sequence.generate()
                    
                    #else:
                    #    # we are the root node, we have no parent, we have nowhere to rollback.
                    #    # so, we create the new parent and switch the root pointer there.
                    #    root_node = self.__class__(self.storage, self.host, id=???)#??? root sequence? but it should be reset each time new root is intriduced
                    #    SOMEWHERE.switch_root_node_to(self.host, root_node.id)#???
                    #    return root_node.generate() # delegate there
                    
                child_node = self.__class__(self.storage, self.host, child_id)
                children.append(child_id)
                item['children'] = children
                
                #save the new child (re-read, if it was changed by the other instance of the node)
                self.storage.store_expect(self.host, self.id, item, expect={'children':old_children})#???!!!
                
                #re-select another one from node's own children
                return self.generate(parent=self)
    
    def update_usage(self):
        old_usage =
        new_usage = 
        if own_usage % 10 != old_usage % 10:# if changed over the step barrier
            self.parent.report(self, new_usage)
    
    def report(self, child, usage):
        index = self.children.find(child)
        if index == -1:
            raise ValueError("Unexistent child has been depleted. Something wrong.")
        
        # Check if it is depleted or just updated.
        if usage >= 100:
            self.children.remove(child)
        else:
            self.children[index].weight = 100 - usage
        
        # If case of removal, re-append new child or just re-populate the children structure.
        try:
            new_id = self.sequence.generate()
            new_child = DistributedNode(self.storage, self.host)
            self.children.append((new_child, 100))
        except DepletedError, e:
            # if the children sequence has been depleted, do nothing, this will make our own usage high.
            pass
            
        # save our own state of children; iterate until saved if necessary.
        
        
        # Recalculate our own usage and report to the parent if necessary.
        old_usage = 0#???!!!
        own_usage = self.calculate_usage()
        if own_usage % 10 != old_usage % 10:# if changed over the barrier
            self.parent.report(self, own_usage)

class DistributedRoot(DistributedNode):
    def __init__(self, storage, host):
        super(DistributedRoot, self).__init__(storage, host, id='')




"""

The system gets too complicated with this algorythm. Must be simplified and separated.
There is forth-propagation (generation) and back-propagation (restructuring).
As for now, we will make simple back-prop as an DepletedError, and when cought,
it calls parent.report(child, depleted=True/False). Later the child could call its
parent directly with analog values of usage (or as part of the result).

But the whole logic of generation and restructiuring should be STRICTLY separated.




"""