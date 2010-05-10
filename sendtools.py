#from tvl.common.pipeutils import consumer
import types
from collections import MutableSet, MutableSequence, MutableMapping
import numpy
from operator import itemgetter as _itemgetter

class PipelineError(Exception):
    pass


class NoError(Exception):
    """Should never be raised"""
    pass


def make_node(func):
    """A decorator to initialise a generator for use in 
    a push-based pipeline
    """
    def wrapper(target, *args, **kwds):
        target = check(target)
        out = target.next()
        g = func(target, *args, **kwds)
        g.next()
        def gen():
            while True:
                g.send((yield out))
        return gen()
    return wrapper


def check(target):
    """
    check(target) -> wrapped target
    
    If target is a list, wrap it with the append generator,
    If target is a tuple, wrap it with the split generator,
    other return target unaltered
    """
    if isinstance(target, MutableSequence):
        return append(target)
    elif isinstance(target, MutableSet):
        return add(target)
    elif isinstance(target, types.TupleType):
        return split(*target)
    return target

        
def append(l):
    """
    Generator wrapper for a list
    """
    while True:
        l.append((yield l))
        

def add(s):
    """
    Generator wrapping a set or other mutable mapping
    """
    while True:
        s.add((yield s))
        
        
def gmap(func, target, catch=NoError):
    """
    gmap(func, target) -> Consumer generator
    
    Data sent into the returned generator is passed to func and the result
    is sent on to the target
    """
    target = check(target)
    out = target.next()
    while True:
        try:
            while True:
                out = target.send(func((yield out)))
        except catch:
            pass
        
def getter(idx, target):
    """
    This is such a common operation, it gets its own consumer
    """
    target = check(target)
    out = target.next()
    get = _itemgetter(idx)
    while True:
        out = target.send(get((yield out)))
        
        
def pull(itr, target):
    """
    pull(iterator, target) -> Consumer generator
    
    When send() is called on the generator, it calls .next() on the supplied
    iterator and the result from this is sent on to the target. The original
    input data is discarded
    """
    itr = iter(itr)
    target = check(target)
    out = target.next()
    try:
        while True:
            data = (yield out)
            out = target.send(itr.next())
    except StopIteration:
        raise PipelineError("Iterator exhausted before pipeline complete")
        
        
def group(pred, target, factory=list):
    """
    group(predicate, target, factory=list) -> Consumer generator
    
    Items sent into the generator are grouped by calling factory to
    create a particular group. Items are sent in to the group
    until predicate(item) returns True, when when the group is sent into the
    target and factory is called to create the next group.
    """
    target = check(target)
    out = target.next()
    
    test = factory()
    if type(check(test)) != type(test):
        f = lambda : check(factory())
    else:
        f = factory
    
    while True:
        grp = f()
        gout = grp.next()
        while True:
            data = (yield out)
            gout = grp.send(data)
            if pred(data):
                break
        out = target.send(gout)
        
        
def group_by_n(n, target, factory=list):
    """
    group_by_n(n, target, factory=list) -> Consumer generator
    
    Groups items by number, n. Groups are created by calling factory
    with no argument (a list is used by default). Once n items have been
    sent into a group, the group is sent into the target and factory
    is called again to create the next group.
    
    Note, only length-n groups are sent to the target, so if the final 
    group is not completed, values already sent to it will be lost.
    """
    target = check(target)
    out = target.next()
    
    test = factory()
    if type(check(test)) != type(test):
        f = lambda : check(factory())
    else:
        f = factory
    
    while True:
        grp = f()
        gout = grp.next()
        for i in xrange(n):
            data = (yield out)
            gout = grp.send(data)
        out = target.send(gout)
        
        
def group_by_key(pred, target, factory=list):
    """
    group_by_key(keyfunc, target, factory=list) -> Consumer generator
    
    Items sent into this object are grouped according to keyfunc(item). Groups
    are created by calling factory with no arguments. Groups are finalised
    when keyfunc(item) returns a different value. The completed group is sent on 
    to the target and factory called to create the next group.
    
    Note, when the group_by_key generator goes out of scope, items remaining
    in the final group will be passed to the target.
    """
    target = check(target)
    out = target.next()
    
    test = factory()
    if type(check(test)) != type(test):
        f = lambda : check(factory())
    else:
        f = factory
    
    data = (yield out)
    key = pred(data)
    while True:
        grp = f()
        gout = grp.next()
        last_key = key 
        try:
            while key==last_key:
                gout = grp.send(data)
                data = (yield out)
                key = pred(data)
        finally:
            out = target.send(gout)
    
        
def gfilter(pred, target):
    """
    gfilter(func, target) -> Consumer generator
    
    Items sent in to the generator are passed to func. If func(item) evaluates
    True, item is sent to target, otherwise it is discarded.
    """
    target = check(target)
    out = target.next()
    if pred is None:
        pred = bool
    while True:
        data = (yield out)
        if pred(data):
            out = target.send(data)
        
        
def split(*targets):
    """
    split(target1, target2, target3, ...) -> Consumer generator
    
    Items sent into the generator are sent on to each of the 
    targets. StopIteration exceptions are handled until all targets
    raise them, when the exception is propagated.
    """
    targets = [check(t) for t in targets]
    out = [t.next() for t in targets]
    actives = list(enumerate(targets))
    while actives:
        data = (yield tuple(out))
        dead = []
        for i,t in actives:
            try:
                out[i] = t.send(data)
            except StopIteration:
                dead.append((i,t))
        for v in dead:
            actives.remove(v)
        
        
def limit(n, target):
    """
    limit(n, target) -> Consumer generator
    
    Forwards up to at most n items to the target.
    """
    target = check(target)
    out = target.next()
    for i in xrange(n):
        out = target.send((yield out))
        
        
def unique(target):
    """
    unique(target) -> Consumer generator
    
    Values passed in previously are dropped.
    """
    target = check(target)
    out = target.next()
    seen = set()
    while True:
        data = (yield out)
        if data not in seen:
            seen.add(data)
            out = target.send(data)
        

def switch(test, *targets):
    """
    switch(func, *targets) -> Consumer generator
    
    func(item) is called for each item passed in to the generator which
    returns idx, an integer. The item is sent to targets[idx]
    """
    targets = [check(t) for t in targets]
    out = [t.next() for t in targets]
    while True:
        data = (yield tuple(out))
        idx = int(test(data))
        out[idx] = targets[idx].send(data)
        
        
def switch_by_key(test, init={}, factory=lambda :[]):
    out = dict((k,init[k].next()) for k in init)
    while True:
        data = (yield out)
        key = test(data)
        try:
            val = init[key].send(data)
            out[key] = val
        except KeyError:
            t = check(factory())
            t.next()
            init[key] = t
            val = t.send(data)
            out[key] = val
            
        
###re-merging doesn't work reliably###
def merge(target):
    target = check(target)
    out = target.next()
    data = (yield out)
    while data is None:
        data = (yield out)
    while True:
        out = target.send(data)
        data = (yield out)


def send(itr, target):
    """Consumes the given iterator and directs the result
    to the target pipeline
    
    params: itr - an iterator which supplies data
            target - a pipeline generator or a tuple of such items
            
    returns: a value, list or tuple of such items with structure corresponding
           to the target pipeline
    """
    target = check(target)
    out = target.next()
    try:
        for item in itr:
            out = target.send(item)
    except StopIteration:
        pass
    return out


def divert(itr, target):
    target = check(target)
    out = target.next()
    for item in itr:
        out = target.send(item)
        yield (item, out)


class Nan(object):
    def __gt__(self, a):
        return True
    
    def __ge__(self, a):
        return True
    
    def __lt__(self, a):
        return True
    
    def __le__(self, a):
        return True

###################################
### Various aggregate functions ###
###################################

def ave():
    i = 1
    val = 0
    while True:
        input = (yield val)
        val += (input-val)/float(i)
        i += 1
        

def gmax():
    val = Nan()
    while True:
        input = (yield val)
        if val < input:
            val = input
        
        
def gmin():
    val = Nan()
    while True:
        input = (yield val)
        if val > input:
            val = input
            
            
def gsum():
    val = 0.0
    while True:
        val += (yield val)
        
        
def count():
    i=0
    while True:
        yield i
        i += 1
        

def first():
    i = (yield None)
    while True:
        yield i
        

def last():
    i = (yield None)
    while True:
        i = (yield i)
        
        
def select(n, transform=lambda x:x):
    """
    select(n, transform=lambda x:x) -> Generator
    
    Select the nth item sent in to the generator. The selected item
    is passed to transform (an identity transform by default) and
    the result is yielded back as an aggregated result
    """
    i = (yield None)
    for j in xrange(n):
        i = (yield None)
    i = transform(i)
    while True:
        yield i


                    
if __name__=="__main__":
    a = range(20)
    
#    pipe(a) | (append(b), 
#               apply(lambda x:x**2) | apply(lambda x:x-5) | append(c))
    op1 = lambda x:x**2
    op2 = lambda x:x-5
    
#    b, (c,d) = send(a) | ([],
#                          gmap(op1) | ( gmap(op2) | [],
#                                        group(lambda x:x%2) | []
#                                        )
#                        )

    from functools import partial
    from operator import gt, mod
    from itertools import cycle
    
    gp = merge(gmap(lambda x:x*2, [])) #group_by_n(3, [])
    
    b, c,d, e, f, g = send(a, ( 
                               (gp, pull(cycle(['a','b']),gp)) ,
                               group_by_n(4, []),
                               group_by_n(4, [], factory=lambda :select(0)),
                               group_by_key(lambda x: int(x/4), [], factory=last),
                               pull(cycle(['a','b']),[]),
                               group_by_n(4, [], factory=lambda :([], gsum()))
                        ) 
                    )

         
    print a
    print "coav", b
    print c
    print d
    print e
    print f
    print g
    
    a = send(xrange(1000), (limit(10, []),
                                limit(20, [])))
    print "limit", a
    
    b = send(xrange(30), ([],switch_by_key(lambda x:bool(x%4))))
    
    print b
    
    c = send("hello world", limit(5, unique((first(),last()))))
    print c