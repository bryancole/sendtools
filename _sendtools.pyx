"""
A cython implementation of the sendtools API
"""
from collections import MutableSequence, MutableSet, Callable, defaultdict,\
            MutableMapping
from types import TupleType


cdef class Consumer(object):
    cdef int _alive
    
    def __cinit__(self, *args, **kwds):
        self._alive = 1
        
    property is_alive:
        def __get__(self):
            return bool(self._alive)
        
    cdef object result_(self):
        raise NotImplementedError
    
    def result(self):
        return self.result_()
    
    cdef void send_(self, object item) except *:
        raise NotImplementedError
    
    def send(self, object item):
        self.send_(item)
    
    cdef void close_(self):
        self._alive = 0
        
    def close(self):
        self.close_()
    
    
cdef class ConsumerSink(Consumer):
    cdef object output
    
    def __cinit__(self, output, *args, **kdws):
        self.output = output
        
    cdef object result_(self):
        return self.output
    
    
cdef class ConsumerNode(Consumer):
    cdef Consumer target
        
    cdef object result_(self):
        return self.target.result_()
    
    cdef void close_(self):
        if self._alive:
            self.target.close_()
        self._alive = 0
    
    
cdef class Append(ConsumerSink):
    def __cinit__(self, output, *args, **kdws):
        assert isinstance(output, MutableSequence)
        
    cdef void send_(self, object item) except *:
        self.output.append(item)
        
    
cdef class ListAppend(Append):
    def __cinit__(self, output, *args, **kdws):
        assert isinstance(output, list)
        
    cdef void send_(self, object item) except *:
        (<list>self.output).append(item)
    
    
cdef class AddToSet(ConsumerSink):
    def __cinit__(self, output, *args, **kdws):
        assert isinstance(output, MutableSet)
        
    cdef void send_(self, object item) except *:
        self.output.add(item)
    
    
cdef class Split(ConsumerNode):
    cdef:
        list targets

    def __cinit__(self, *targets):
        self.targets = [check(t) for t in targets]
        
    cdef object result_(self):
        cdef Consumer t
        return tuple([t.result_() for t in self.targets])
        
    cdef void send_(self, object item) except *:
        cdef:
            int alive=0
            Consumer t
        for t in self.targets:
            if t._alive:
                try:
                    t.send_(item)
                    alive = 1
                except StopIteration:
                    t._alive = 0
        if alive==0:
            self._alive = 0
            raise StopIteration
    
    cdef void close_(self):
        cdef Consumer t
        if self._alive:
            for t in self.targets:
                t.close_()
        self._alive = 0


cdef class Limit(ConsumerNode):
    cdef:
        unsigned int count, total
        
    def __cinit__(self, unsigned int n, target):
        self.target = check(target)
        self.total = n
        self.count = 0
        
    cdef void send_(self, object item) except *:
        if self.count >= self.total:
            self._alive = 0
            raise StopIteration
        else:
            self.target.send_(item)
            self.count += 1


cdef class Map(ConsumerNode):
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
        
    cdef void send_(self, object item) except *:
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
        
        
cdef class Get(ConsumerNode):
    cdef object selector
    
    def __cinit__(self, idx, target):
        self.selector = idx
        self.target = check(target)
        
    cdef void send_(self, object item) except *:
        self.target.send_(item[self.selector])
    
    
cdef class Attr(ConsumerNode):
    cdef object attrname
    
    def __cinit__(self, name, target):
        self.attrname = str(name)
        self.target = check(target)
        
    cdef void send_(self, object item) except *:
        self.target.send_(getattr(item, self.attrname))
        
        
cdef class Factory(object):
    cdef object factory
    
    def __cinit__(self, factory):
        assert isinstance(factory, Callable)
        self.factory = factory
        
    def __call__(self):
        return check(self.factory())
        
        
cdef class GroupByN(ConsumerNode):
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
        self.output = self.target.result_()
        
        
    cdef void send_(self, object item) except *:
        cdef:
            object gout, out, output
    
        self.this_grp.send_(item)
        self.count += 1
        if self.count >= self.n:
            self.target.send_(self.this_grp.result_())
            self.count = 0
            self.this_grp = self.factory()
    
    
cdef class NULL_OBJ(object):
    def __richcmp__(self, other, op):
        return True
    
    def __add__(x, y):
        if not isinstance(y, NULL_OBJ):
            return y
        elif not isinstance(x, NULL_OBJ):
            return x
        else:
            return NotImplemented


cdef class GroupByKey(ConsumerNode):
    cdef:
        object factory, keyfunc, thiskey, grp_output, output
        Consumer this_grp
        
    def __cinit__(self, target, keyfunc=None, factory=list):
        cdef Consumer checked
        
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
        self.grp_output = checked.result_()
        self.output = <Consumer>self.target.result_()
        self.thiskey = NULL_OBJ()
        
    cdef void send_(self, item) except *:
        if not self._alive:
            raise StopIteration
        
        if self.keyfunc is None:
            key = item
        else:
            key = self.keyfunc(item)
            
        if key==self.thiskey:
            pass
        else:
            self.target.send_(self.this_grp.result_())
            self.this_grp = self.factory()
        self.this_grp.send_(item)
        self.thiskey = key

    cdef void close_(self):
        self.target.send_(self.this_grp.result_())
        self._alive = 0
        
        
cdef class Switch(Consumer):
    cdef:
        tuple targets
        object func
        
    def __cinit__(self, func, *targets):
        if not isinstance(func, Callable):
            raise TypeError("Fist argument must be a callable returning an int")
        self.func = func
        self.targets = tuple([check(t) for t in targets])
        
    cdef object result_(self):
        cdef Consumer t
        return tuple([t.result_() for t in self.targets])
    
    cdef void send_(self, item) except *:
        cdef int i
        i = self.func(item)
        <Consumer>self.targets[i].send_(item)
        
        
cdef class SwitchByKey(Consumer):
    cdef:
        object output, func

    def __cinit__(self, func=None, init=None, factory=list):
        if func is not None and not isinstance(func, Callable):
            raise TypeError("1st argument, func must be a callable")
        self.func = func
        factory = Factory(factory)
        if init is None:
            self.output = defaultdict(factory)
        else:
            if isinstance(init, MutableMapping):
                self.output = defaultdict(factory, [(k,check(init[k])) for k in init])
            else:
                raise TypeError("init parameter must be a mapping type")
        
    cdef object result_(self):
        return dict([(k,(<Consumer>self.output[k]).result_()) for k in self.output])
        
    cdef void send_(self, item) except *:
        if self.func is None:
            (<Consumer>self.output[item]).send_(item)
        else:
            (<Consumer>self.output[self.func(item)]).send_(item)
    
    
##############################################################################
###Aggregate functions: min, max, sum, count, ave, std, first, last, select###
##############################################################################

cdef class Aggregate(Consumer):
    cdef object output
    
    def __cinit__(self):
        self.output = NULL_OBJ()
    
    cdef object result_(self):
        return self.output


cdef class Min(Aggregate):
    cdef void send_(self, item) except *:
        if item < self.output:
            self.output = item


cdef class Max(Aggregate):
    cdef void send_(self, item) except *:
        if item > self.output:
            self.output = item
            
            
cdef class Sum(Aggregate):
    cdef void send_(self, item) except *:
        self.output += item
        
        
cdef class Count(Aggregate):
    cdef unsigned int count
    
    def __cinit__(self):
        self.count = 0
    
    cdef void send_(self, item) except *:
        self.count += 1
            
    cdef object result_(self):
        return self.count
    
    
cdef class Ave(Aggregate):
    cdef unsigned int count
    
    def __cinit__(self):
        self.count = 0
        self.output = 0.0
    
    cdef void send_(self, item) except *:
        self.count += 1
        self.output += (item-self.output)/self.count
    
    
cdef class First(Aggregate):
    cdef void send_(self, item) except *:
        if self._alive != 1:
            self.output = item
            self._alive = 0
            
            
cdef class Last(Aggregate):
    cdef void send_(self, item) except *:
        self.output = item
        

cdef class Select(Aggregate):
    cdef:
        unsigned int n, count
        object transform 
    
    def __cinit__(self, n, transform=None):
        if transform is not None and not isinstance(transform, Callable):
            raise TypeError("transform parameter must be a callable")
        self.n = n
        self.count = 0
        self.transform = transform
            
    cdef void send_(self, item) except *:
        if self._alive==1:
            if self.n == self.count:
                if self.transform is not None:
                    self.output = item
                else:
                    self.output = self.transform(item)
                self._alive = 0
            self.count += 1
        
            

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
        if isinstance(target, list):
            return ListAppend(target)
        else:
            return Append(target)
    elif isinstance(target, MutableSet):
        return AddToSet(target)
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
    try:
        for item in itr:
            target.send_(item)
    except StopIteration:
        pass
    out = target.result_()
    target.close_()
    return out
