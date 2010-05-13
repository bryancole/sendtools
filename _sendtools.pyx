"""
A cython implementation of the sendtools API
"""
from collections import MutableSequence, MutableSet, Callable
from types import TupleType


cdef class Consumer(object):
    cdef int _alive
    
    def __cinit__(self, *args, **kwds):
        self._alive = 1
    
    cdef send_(self, object item):
        raise NotImplementedError
    
    def send(self, object item):
        self.send_(item)
    
    cdef close_(self):
        self._alive = 0
        
    def close(self):
        self.close_()
    
    
cdef class ConsumerSink(Consumer):
    cdef object output
    
    def __cinit__(self, output, *args, **kdws):
        self.output = output
        
    def __next__(self):
        return self.output
    
    cdef close_(self):
        self._alive = 0
    
    
cdef class ConsumerNode(Consumer):
    cdef Consumer target
        
    def __next__(self):
        return self.target.next()
    
    cdef close_(self):
        if self._alive:
            self.target.close_()
        self._alive = 0
    
    
    
cdef class Append(ConsumerSink):
    def __cinit__(self, output, *args, **kdws):
        assert isinstance(output, list)
        
    cdef send_(self, object item):
        (<list>self.output).append(item)
    
    cdef close_(self):
        self._alive = 0
    
    
cdef class Split(ConsumerNode):
    cdef:
        list targets, output

    def __cinit__(self, *targets):
        self.targets = [check(t) for t in targets]
        self.output = [t.next() for t in self.targets]
        
    def __next__(self):
        return tuple(self.output)
        
    cdef send_(self, object item):
        cdef:
            int i=0,alive=0, size=len(self.targets)
            Consumer t
        for i in xrange(size):
            t = self.targets[i]
            if t._alive:
                try:
                    self.output[i] = t.send_(item)
                    alive = 1
                except StopIteration:
                    t._alive = 0
        if alive==0:
            raise StopIteration
    
    cdef close_(self):
        cdef Consumer t
        if self._alive:
            for t in self.targets:
                t.close_()


cdef class limit(ConsumerNode):
    cdef:
        unsigned int count, total
        
    def __cinit__(self, unsigned int n, target):
        self.target = check(target)
        self.total = n
        self.count = 0
        
    cdef send_(self, object item):
        if self.count >= self.total:
            self._alive = 0
            raise StopIteration
        else:
            self.target.send_(item)
            self.count += 1
        
    cdef close_(self):
        if self._alive:
            self.target.close_()
        self._alive = 0


cdef class gmap(ConsumerNode):
    cdef:
        object func
        object exc
        
    def __cinit__(self, func, target, catch=None):
        assert isinstance(func, Callable)
        if catch is not None:
            assert issubclass(catch, BaseException)
        self.func = func
        self.target = check(target)
        self.exc = catch
        
    cdef send_(self, object item):
        cdef object out
        if not self._alive:
            raise StopIteration
        try:
            self.target.send_(self.func(item))
        except self.exc:
            pass
        except:
            self._alive = 0
            raise
        
    cdef close_(self):
        if self._alive:
            self.target.close_()
        self._alive = 0
        
        
cdef class getter(ConsumerNode):
    cdef object selector
    
    def __cinit__(self, target, idx):
        self.selector = idx
        self.target = check(target)
        
    cdef send_(self, object item):
        self.target.send_(item[self.selector])
    
    
cdef class attr(ConsumerNode):
    cdef object attrname
    
    def __cinit__(self, target, name):
        self.attrname = str(name)
        self.target = check(target)
        
    cdef send_(self, object item):
        self.target.send_(getattr(item, self.attrname))
        
        
cdef class Factory(object):
    cdef object factory
    
    def __cinit__(self, factory):
        assert isinstance(factory, Callable)
        self.factory = factory
        
    def __call__(self):
        return check(self.factory())
        
        
cdef class group_by_n(ConsumerNode):
    cdef:
        unsigned int n, count
        object factory, output
        Consumer this_grp
        
    def __cinit__(self, unsigned int n, target, factory=list):
        self.target = check(target)
        self.n = n
        self.factory = factory
        self.count = 0
        
        first = factory()
        checked = check(first)
        if type(checked) != type(first):
            self.factory = Factory(factory)
        else:
            self.factory = factory
            
        self.this_grp = checked
        self.output = self.target.next()
        
        
    cdef send_(self, object item):
        cdef:
            object gout, out, output
    
        gout = self.this_grp.send_(item)
        self.count += 1
        if self.count >= self.n:
            self.target.send_(gout)
            self.count = 0
            self.this_grp = self.factory()
    
    cdef close_(self):
        if self._alive:
            self.target.close_()
        self._alive = 0
    
    
cdef class NULL_OBJ(object):
    def __richcmp__(self, other, op):
        return True


cdef class group_by_key(ConsumerNode):
    cdef:
        object factory, keyfunc, thiskey, grp_output, output
        Consumer this_grp
        
    def __cinit__(self, target, keyfunc=None, factory=list):
        if keyfunc is not None:
            assert isinstance(keyfunc, Callable)
        assert isinstance(factory, Callable)
        self.target = check(target)
        self.keyfunc = keyfunc
        self.factory = factory
        
        first = factory()
        checked = check(first)
        if type(checked) != type(first):
            self.factory = Factory(factory)
        else:
            self.factory = factory
        self.this_grp = checked
        self.grp_output = checked.next()
        self.output = self.target.next()
        self.thiskey = NULL_OBJ()
        
    cdef send_(self, item):
        if not self._alive:
            raise StopIteration
        
        if self.keyfunc is None:
            key = item
        else:
            key = self.keyfunc(item)
            
        if key==self.thiskey:
            pass
        else:
            self.target.send_(self.this_grp.next())
            self.this_grp = self.factory()
        self.this_grp.send_(item)
        self.thiskey = key
        return self.output

    cdef close_(self):
        self.target.send_(self.this_grp.next())
        self._alive = 0
        
##############################################################################
###Aggregate functions: min, max, sum, count, ave, std, first, last, select###
##############################################################################


cdef check(target):
    """
    check(target) -> wrapped target
    
    If target is a list, wrap it with the append generator,
    If target is a tuple, wrap it with the split generator,
    other return target unaltered
    """
    if isinstance(target, Consumer):
        return target
    elif isinstance(target, MutableSequence):
        return Append(target)
#    elif isinstance(target, MutableSet):
#        return add(target)
    elif isinstance(target, TupleType):
        return Split(*target)
    else:
        raise TypeError("Can't convert %s to Consumer"%repr(target))


def send(object itr, object target_in):
    """Consumes the given iterator and directs the result
    to the target pipeline
    
    params: itr - an iterator which supplies data
            target - a pipeline generator or a tuple of such items
            
    returns: a value, list or tuple of such items with structure corresponding
           to the target pipeline
    """
    cdef Consumer target
    
    target = check(target_in)
    out = target.next()
    try:
        for item in itr:
            target.send_(item)
    except StopIteration:
        pass
    out = target.next()
    target.close_()
    return out
