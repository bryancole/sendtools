==============================
Installing and Using Sendtools
==============================

.. contents:: **Table of Contents**

--------
Overview
--------

Sendtools is a collections of classes for efficiently consuming iterators into 
one or more data structures. It compliments the itertools module and the other 
excellent facilities python offers for iteration. Sendtools is useful when:

 * Your source iterator is too big to fit in memory
 * Your data source is I/O-bound so you don't want to make more than one pass
 * You want to collect data into two or more lists (or other collections)
 * You want to group, filter, transform or otherwise aggregate the data

Such situations occur when you're analysing query-sets from large databases or 
datafiles (HDF5-files, for example).

Sendtools is written using Cython to produce a 100% compiled module, for maximum 
performance.

Ideas and further discussion on sendtools can be posted to 
http://groups.google.com/group/python-sendtools

------------
Requirements
------------

There are no dependencies outside of python to compile and install sendtools (although
you will need a compiler obviously).

If you want to hack on the Cython code, you'll need Cython-0.12.1 or later.

------------
Installation
------------

Sendtools is installed from source using distutils in the usual way - run::

    #python setup.py install

to install it site-wide

If you have Cython installed, you can also import the sendtools.pyx file directly
using the pyximport module (part of Cython). This is handy for development, as used
in the unittest script.

-----
Usage
-----

Sendtools is built on the concept of "Consumer" objects. These were inspired by 
python's generators (an early version of sendtools was implemented in python 
using generators). Consumer objects can have data "sent" into them. Unlike 
generators, Consumers do not produce data iteratively (no ``next`` method), 
but they do produce a result which can be accessed at any time using the ``.result()`` 
method.

Data is typically sent into a Consumer using the ``sendtools.send()`` function, 
which takes the form::

    output = send(source, target)

where ``source`` is an iterator producing data. ``target`` is a Consumer object into 
which the data is sent. ``output`` is the Consumer's result, returned after the 
source has been fully consumed, or the Consumer indicates it's complete (by 
raising StopIteration), whichever happens first. Basically, the send function 
is a shortcut for writing a for-loop. It's equivalent to (but faster than)::

    def send(source, target):
        try:
            for item in source:
                target.send(item)
        except StopIteration:
            pass
        return target.result()
        
Note, StopIteration can be raised by ``target.send(...)`` to exit the loop (as 
well as by the source), so we handle it explicitly.

The target may be list or set, representing the data structure you want to 
collect the data into. These are implicitly converted to Consumer objects by 
the send function. The input list (or set) is returned by the send function 
having been filled with data. 

``target`` can also be a (multiply nested) tuple of consumers. In this case the 
result will be a tuple which matches the structure of the target tuple, 
containing the results for each consumer. In this way, data from a source 
iterator can be collected into multiple lists in a single iteration pass.

Sendtools defines many aggregation consumers. These do not produce a list or 
other collection as their result, but a scalar value.

--------
Examples
--------

Let's start with basic usage of the ``send()`` function::

    >>> from sendtools import send
    >>> data = range(10)
    >>> out=[]
    >>> result = send(data, out)
    >>> result
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> out
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> result is out
    True

The source 'data' is copied into the target 'out' and this is returned.

Now lets see how to send data into multiple targets::

    >>> a, (b,c) = send(data, ([], ([],[]) ) )
    >>> a is b; b is c; a is c
    False
    False
    False
    >>> a == b; b == c; a == c
    True
    True
    True

The data is copied into three different lists.

Data can be collected into sets as well as lists:: 

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,"moose",4,2]
    >>> send(data, set())
    set([1, 2, 3, 4, 5, 6, 8, 'moose'])

In fact, any MutableSequence or MutableSet (the Abstract Base Class) will do. 
Sadly, the std-lib array.array object is not registered as a MutableSequence 
out-the-box, but we can do this ourselves::

    >>> from array import array
    >>> from collections import MutableSequence
    >>> MutableSequence.register(array)
    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> target = array("f") #an empty array
    >>> send(data, target)
    array('f', [1.0, 2.0, 3.0, 5.0, 4.0, 2.0, 6.0, 3.0, 4.0, 8.0, 5.0, 6.0, 3.0, 
    1.0, 5.0, 3.0, 6.0, 3.0, 6.0, 4.0, 2.0])


Aggregation
-----------

Now let's see some aggregation::

    >>> send(data, ([], (Max(), Min(), Sum(), Ave())))
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], (9, 0, 45, 4.5))

All the aggregation functions found in SQL are available: Sum, Max, Min, Ave, 
First, Last, Count.

There are a few more besides these: 

 * All, Any - like the builtin ``all`` and ``any``, but for consumers
 * Select - Picks the n'th item in a sequence
 * Stats - Computes an incremental standard deviation, mean and count of it's input. 
 
This last one only works with numerical input and returns a length-3 tuple as it's result.

Here's a (somewhat pointless) example of Select and Stats::

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> targets = tuple([Select(i) for i in xrange(0,10,3)])
    >>> send(data, targets)
    (1, 5, 6, 8)
    >>> send(data, Stats())
    (21L, 3.9047619047619047, 1.868281614338746)

Obviously, a better way to pick out every 3rd item from a series from 0 to 10 
would be to use the Slice object (see below).

Transformations and Filtering
-----------------------------

Data can be filtered using Filter::

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> send(data, Filter(lambda x:x%2==0, []))
    [2, 4, 2, 6, 4, 8, 6, 6, 6, 4, 2]

Data can be transformed using Map::

    >>> send(data, ([], Map(lambda x:x**2, [])))
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 1, 4, 9, 16, 25, 36, 49, 64, 81])

One important use-case is splitting a sequence of tuples or other 
compound objects into multiple lists. Although this can be done with Map,
this is such a common operation, we have a dedicated Get object for this
purpose. eg.::

    >>> tups = [(x,x**2) for x in range(10)]
    >>> print tups
    [(0, 0), (1, 1), (2, 4), (3, 9), (4, 16), (5, 25), (6, 36), (7, 49), 
    (8, 64), (9, 81)]
    >>> a,b = send(tups, (Get(0,[]), Get(1,[])))
    >>> a
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> b
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

This works for any suitable indexing object. For example, columns from a database
query can be collected into some lists using this method. Object attributes
can also be retrieved in a similar manner using the Attr object.

Sequence/iterable unpacking has a further simplification, using the Unzip object. The
above example can be rewritten as::

    >>> a,b = send(tups, Unzip([],[]))
    >>> a
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> b
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

The Unzip object takes any number of Consumers as it's arguments. Sequences or iterables
can be sent into it. There must be at least enough items in the input container 
as output targets, otherwise TypeError is raised. Excess input items are discarded.

Grouping and Switching
----------------------

Data can be grouped in a variety of ways. The grouping objects take a factory 
function as a keyword argument. This is called to create each group. By default, 
a list group is created, but more complex group-types are possible: aggregates, 
tuples of targets or even other grouping objects. Any valid target object can 
be used.

Here's an example of simple grouping by number into sublists::

    >>> data
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    >>> send(data, GroupByN(3,[]))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11], [12, 13, 14], [15, 16, 17]]

Now let's use a more complex group factory for get the mean of each group,
as well as the group list::

    >>> send(data, GroupByN(3, [], factory=lambda :([],Ave())))
    [([0, 1, 2], 1.0), ([3, 4, 5], 4.0), ([6, 7, 8], 7.0), ([9, 10, 11], 10.0), 
    ([12, 13, 14], 13.0), ([15, 16, 17], 16.0)]

Groups can also be created using a key-function, with the GroupByKey object::

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> send(data, GroupByKey(lambda x:x==5, []))
    [[1, 2, 3], [5], [4, 2, 6, 3, 4, 8], [5], [6, 3, 1], [5], [3, 6, 3, 6, 4, 2]]

Note, new groups are created whenever the key-function returns a different 
result to the previous item, regardless of whether that result has been used to
create previous groups.
    
Switching is a very close relative to grouping. The Switch object passes it's
input to a key-function which must return an int. The input is passed to one
of N outputs according to this int. I.e.::

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> send(data, Switch(lambda x:int(x<5), [],[]))
    ([5, 6, 8, 5, 6, 5, 6, 6], [1, 2, 3, 4, 2, 3, 4, 3, 1, 3, 3, 4, 2])
    
The Switch object can take any number of target Consumers.

If you want to collect objects into groups according a key, without preserving
the order, you need SwitchByKey. This object outputs a dictionary of groups.:: 

    >>> data = [1,2,3,5,4,2,6,3,4,8,5,6,3,1,5,3,6,3,6,4,2]
    >>> func = lambda item: "low" if item<5 else "high"
    >>> send(data, SwitchByKey(func, init={"low":['foo']}))
    {'high': [5, 6, 8, 5, 6, 5, 6, 6], 
    'low': ['foo', 1, 2, 3, 4, 2, 3, 4, 3, 1, 3, 3, 4, 2]}
    >>> send(data, SwitchByKey(func, factory=Sum))
    {'high': 47, 'low': 35}

The init keyword specifies a dictionary of groups with which to initialise
the object (an empty dict by default). When a new key is encountered (that does 
not already exist in the dict), the factory function is called to create a new 
group for this key. 

Slicing
-------

The Slice object works analogously to the builtin slice function, but for 
Consumers. Like builtin slice, it takes one to three arguments specifying the
start, stop and step values for selection::

    >>> data = range(20)
    >>> send(data, Slice(None,15,3, []))
    [0, 3, 6, 9, 12]
    >>> send(data, Slice(5,None,3, []))
    [5, 8, 11, 14, 17]

Slice follows a similar call-signature overloading as used by built-in slice, where
the step or step and start arguments may be left out. It differs from the built-in 
slice object in that the stop-index is not required.

