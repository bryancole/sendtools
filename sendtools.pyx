#!python
"""
A cython implementation of the sendtools API
"""
from collections import MutableSequence, MutableSet, Callable, defaultdict,\
            MutableMapping
from types import TupleType, GeneratorType
from math import sqrt


cdef class Consumer(object):
    """
    (Abstract) base class for Consumer objects. Not intended to be instantiated
    directly.
    """
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
    """
    Abstract base class for Consumers forming the terminating nodes in a chain
    """
    cdef object output
    
    def __cinit__(self, output, *args, **kdws):
        self.output = output
        
    cdef object result_(self):
        return self.output
    
    
cdef class ConsumerNode(Consumer):
    """
    Abstract base class for Consumers which pass on data to a target Consumer
    """
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
    """
    Limit(n, target) -> Consumer object
    
    Passes up to n items sent in to the consumer on to it's target. Sending
    further items raise StopIteration
    """
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
            

cdef class Slice(ConsumerNode):
    """
    Slice([start,] stop[, step], target) -> Consumer
    
    Acts like builtin slice, but for consumers. 
    """
    cdef:
        unsigned int count, nxt
        unsigned int start, stop, step
        
    def __cinit__(self, *args):
        cdef unsigned int i, nargs=len(args)
        if nargs>4:
            raise TypeError("Slice expects at most 4 arguments, got %d"%nargs)
        if nargs<2:
            raise TypeError("Slice requires at least 2 arguments, got %d"%nargs)
        self.target = check(args[-1])
        if nargs==2:
            self.stop = args[0]
            self.start = 0
            self.step = 1
        else:
            self.start = args[0]
            self.stop = args[1]
            if nargs==4:
                self.step = args[2]
            else:
                self.step = 1
        if self.stop < 0:
            raise TypeError("stop value may not be None")
        self.count = 0
        self.nxt = self.start
        
    cdef void send_(self, object item) except *:
        if self.nxt >= self.stop:
            self._alive = 0
            raise StopIteration
        if self.count == self.nxt:
            self.target.send_(item)
            self.nxt += self.step
        self.count += 1
        
        
cdef class Filter(ConsumerNode):
    """
    Filter(func, target) -> Consumer
    
    For each item sent into a Filter instance, func(item) is called and if the
    result evaluates to True, the item is send to the target. Otherwise, the item
    is dropped.
    """
    cdef:
        object func
        
    def __cinit__(self, func, target):
        if not isinstance(func, Callable):
            raise TypeError("first argument must be a callable")
        self.func = func
        self.target = check(target)
        
    cdef void send_(self, object item) except *:
        if not self._alive:
            raise StopIteration
        try:
            if self.func(item):
                self.target.send_(item)
        except:
            self._alive = 0
            raise
        

cdef class Map(ConsumerNode):
    """
    Map(func, target, catch=None) -> Consumer
    
    For each item send into this consumer, func is called with the item as
    it's argument. The result is send on to target. catch may be an exception
    or tuple of exception. If func raises one of these specified exceptions, 
    they are handled by the Map consumer (i.e. do not propagate) so the consumer
    remains alive to receive further items.
    """
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
    """
    Get(idx, target) -> Consumer
    
    Items sent into this consumer are sliced using idx as the slicing object. The
    result is passed on to target.
    
    Should I have called this Item?
    """
    cdef object selector
    
    def __cinit__(self, idx, target):
        self.selector = idx
        self.target = check(target)
        
    cdef void send_(self, object item) except *:
        self.target.send_(item[self.selector])
    
    
cdef class Attr(ConsumerNode):
    """
    Attr(name, target) -> Consumer
    
    Retrieves the named attribute from objects send into this object and passes
    them on to the target
    """
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
    """
    GroupByN(n, target, factory=list) -> Consumer
    
    Items sent in to this object are partitioned into groups of size n. The
    groups are consumers too. factory, if given, is a function called to create
    each group (a list, by default).
    
    An example should help:
    >>> data = range(15)
    >>> send(data, GroupByN(4, []))
    [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]
    >>> send(data, GroupByN(4, [], factory=Sum))
    [6, 22, 38]
    
    Note, incomplete groups are *never* passed on.
    """
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
    """
    GroupByKey(target, keyfunc=None, factory=list) -> Consumer
    
    For each item passed in keyfunc is called with the item as it's argument.
    If the result is not equal to that of the previous item, a new group is created
    by calling factory and the current item sent into that group.
    
    When a group is finalised (either by starting a new group or when the GroupByKey
    object goes out-of-scope), it is sent on to the target.
    
    If keyfunc is not specified, the item is used directly.
    
    For example:
    >>> data = [3,3,3,3,3,5,5,2,2,2,2,3,3,3]
    >>> send(data, GroupByKey([]))
    [[3, 3, 3, 3, 3], [5, 5], [2, 2, 2, 2], [3, 3, 3]]
    """
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
        
        
cdef class Stats(Aggregate):
    """
    Aggregate Consumer. Computes a running count, average and 
    (unbiased) standard deviation.
    
    The output is a tuple -> (count, mean, std)
    """
    cdef: 
        unsigned int n
        double mean, M2
        
    def __cinit__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0
        
    cdef void send_(self, item) except *:
        cdef double delta
        self.n += 1
        delta = item - self.mean
        self.mean += delta/self.n
        self.M2 += delta*(item - self.mean)
    
    cdef object result_(self):
        return (self.n, self.mean, sqrt(self.M2/(self.n - 1)))
    
    
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



cdef class GeneratorConsumer(Consumer):
    """
    A class for creating a Consumer from a generator. 
    
    Not usually instantiated directly, but is created by a consumer 
    decorator.
    """
    cdef: 
        object gen
        object out
    
    def __cinit__(self, gen):
        self.gen = gen
        self.out = gen.next()
        
    cdef void send_(self, item) except *:
        self.out = self.gen.send(item)
        
    cdef object result_(self):
        return self.out


cdef class consumer(object):
    """
    A decorator for building custom Consumer objects from a generator function
    """
    cdef:
        object func
    
    def __cinit__(self, func):
        if not isinstance(func, Callable):
            raise TypeError("first argument must be callable")
        self.func = func
        self.__doc__ = func.__doc__
        
    def __call__(self, *args, **kwds):
        gen = self.func(*args, **kwds)
        if not isinstance(gen, GeneratorType):
            raise TypeError("wrapped function must return a generator")
        return GeneratorConsumer(gen)