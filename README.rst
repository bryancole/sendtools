==============================
Installing and Using Sendtools
==============================

.. contents:: **Table of Contents**

--------
Overview
--------

Sendtools is a collections of classes for efficiently consuming iterators into one or more data structures. It compliments the itertools module and other the excellent facilities python offers for iteration. Sendtools is useful when:

 * Your source iterator is too big to fit in memory
 * Your data source is I/O bounds so you don't want to make more than one pass
 * You want to collect data into two or more lists (or other collection)
 * You want to group, filter, transform or otherwise aggregate the data

Sendtools is written using Cython to produce a 100% compiled module, for maximum performance.

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

Sendtools is built on the concept of "Consumer" objects. These were inspired by python's generators (an early version of sendtools was implemented in python using generators). Consumer objects can have data "sent" into them. Unlike generators, Consumers do not produce data iteratively (no 'next' method), but they do produce a result which can be accessed at any time using the .result() method.

Data is typically sent into a Consumer using the sendtools.send() function, which takes the form::

    output = send(source, target)

where source is an iterator producing data. target is a Consumer object into which the data is sent. output is the Consumer's result, returned after the source has been fully consumed, or the Consumer indicates it's complete (by raising StopIteration), which ever happens first. Basically, the send function if a shortcut for writing a for-loop.

The target may be list or set, representing the data structure you want to collect the data into. These are implicitly converted to Consumer objects by the send function. The input list (or set) is returned by the send function having been filled with data. 

Target can also be a (multiply nested) tuple of consumers. In this case the result will be a tuple which matches the structure of the target tuple, containing the results for each consumer. In this way, data from a source iterator can be collected into multiple lists in a single iteration pass.

Sendtools defines many aggregation consumers. These do not produce a list or other collection as their result, but a scalar value.

--------
Examples
--------

Let's start with basic usage of the send() function::

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

    >>> a, (b,c) = send(data, ([], ([],[])))
    >>> a is b; b is c; a is c
    False
    False
    False
    >>> a == b; b == c; a == c
    True
    True
    True

The data is copied into three different lists.

Aggregation
-----------

Now add some aggregation::

    >>> send(data, ([], (Max(), Min(), Sum(), Ave())))
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], (9, 0, 45, 4.5))

All the aggregation functions found in SQL are available, along with a couple more: 

 . Slice - analogous to the slice builtin
 . Select - Picks the n'th item in a sequence
 . Stats - Computes an incremental standard deviation, mean and count of it's input. This one only works with numerical input and returns a length-3 tuple as it's result.

Transformations and Filtering
-----------------------------

Data can be transformed using Map::

    >>> send(data, ([], Map(lambda x:x**2, [])))
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0, 1, 4, 9, 16, 25, 36, 49, 64, 81])

One important use-case is splitting a sequence of tuples or other 
compound objects into multiple lists. Although this can be done with Map,
this is such a common operation, we have a dedicated Get object for this
purpose. eg.::

    >>> tups = [(x,x**2) for x in range(10)]
    >>> a,b = send(tups, (Get(0,[]), Get(1,[])))
    >>> a
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> b
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

This works for any suitable indexing object. For example, columns from a database
query can be collected into some lists using this method. Object attributes
can also be retrieved in a similar manner using the Attr object.

Grouping
--------

Data can be grouped in a variety of ways. The grouping objects take a factory function as a keyword argument. This is called to create each group. By default, a list group is created, but more complex group-types are possible: aggregates, tuples of targets or even other grouping objects. Any valid target object can be used.

Here's an example of simple grouping by number into sublists::

    >>> data
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    >>> send(data, GroupByN(3,[]))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11], [12, 13, 14], [15, 16, 17]]

Now let's get the mean of each group::

    >>> send(data, GroupByN(3, [], factory=lambda :([],Ave())))
    [([0, 1, 2], 1.0), ([3, 4, 5], 4.0), ([6, 7, 8], 7.0), ([9, 10, 11], 10.0), 
    ([12, 13, 14], 13.0), ([15, 16, 17], 16.0)]

