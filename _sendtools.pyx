"""
A cython implementation of the sendtools API
"""
from collections import MutableSequence, MutableSet, Callable
from types import TupleType


cdef class Consumer(object):
    cdef object _output
    cdef int _alive
    
    def __cinit__(self, *args, **kwds):
        self._alive = 1

    def __next__(self):
        return self._output
    
    cdef object send_(self, object item):
        pass
    
    def send(self, object item):
        return self.send_(item)
    
    
cdef class Append(Consumer):
    def __cinit__(self, output):
        assert isinstance(output, list)
        self._output = output
        
    cdef object send_(self, object item):
        <list>self._output.append(item)
        return self._output
    
    
cdef class Split(Consumer):
    cdef:
        list _targets

    def __cinit__(self, *targets):
        self._targets = [check(t) for t in targets]
        self._output = [t.next() for t in self._targets]
        
    cdef object send_(self, object item):
        cdef:
            int i=0,alive=0
            Consumer t
            list out
        for i in xrange(len(self._targets)):
            t = self._targets[i]
            if t._alive:
                try:
                    self._output[i] = t.send_(item)
                    alive = 1
                except StopIteration:
                    t._alive = 0
        if alive==0:
            raise StopIteration
        return tuple(self._output)


cdef class limit(Consumer):
    cdef:
        unsigned int count, total
        Consumer target
        
    def __cinit__(self, unsigned int n, target):
        self.target = check(target)
        self._output = self.target.next()
        self.total = n
        self.count = 0
        
    cdef object send_(self, object item):
        if self.count >= self.total:
            raise StopIteration
        else:
            output = self.target.send_(item)
            self.count += 1
            return output

cdef class gmap(Consumer):
    cdef:
        Consumer target
        object func
        object exc
        
    def __cinit__(self, func, target, catch=None):
        assert isinstance(func, Callable)
        if catch is not None:
            assert issubclass(catch, BaseException)
        self.func = func
        self.target = check(target)
        self._output = self.target.next()
        self.exc = catch
        
    cdef object send_(self, object item):
        cdef object out
        try:
            out = self.target.send_(self.func(item))
            self._output = out
            return out
        except self.exc:
            pass
        
        
cdef class Factory(object):
    cdef object factory
    
    def __cinit__(self, factory):
        assert isinstance(factory, Callable)
        self.factory = factory
        
    def __call__(self):
        return check(self.factory())
        
        
cdef class group_by_n(Consumer):
    cdef:
        unsigned int n, count
        Consumer target
        object factory
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
            self.this_grp = checked
        else:
            self.factory = factory
            
        self._output = self.target.next()
        
    cdef object send_(self, object item):
        cdef:
            object gout, out
    
        gout = self.this_grp.send_(item)
        self.count += 1
        if self.count >= self.n:
            self._output = self.target.send_(gout)
            self.count = 0
            self.this_grp = self.factory()
        return self._output


def check(target):
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
            out = target.send_(item)
    except StopIteration:
        pass
    return out
