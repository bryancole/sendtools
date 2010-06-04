Sendtools - Overview
--------------------

Sendtools is a collections of utlities for efficiently consuming iterators to create one or more data structure. It compliments the itertools module and other the excellent facilities python offers for iteration. Sendtools is useful when:

 * Your source iterator is too big to fit in memory
 * Your data source is I/O bounds so you don't want to make more than one pass
 * You want to collect data into two or more lists (or other collection)
 * You want to group, filter, transform or otherwise aggregate the data

Sendtools is written using Cython to produce a 100% compiled module, for maximum performance.

Requirements
------------

There are no dependencies outside of python to compile and install sendtools (although
you will need a compiler obviously).

If you want to hack on the Cython code, you'll need Cython-0.12.1 or later.

Installation
------------

Sendtools is installed from source using distutils in the usual way - run
{{{
#python setup.py install
}}}
to install it site-wide

If you have Cython installed, you can also import the sendtools.pyx file directly
using the pyximport module (part of Cython). This is handy for development, as used
in the unittest script.

Usage
-----

Sendtools is built on the concept of "Consumer" objects. These were inspired by python's generators (an early version of sendtools was implemented in python using generators). Consumer objects can have data "sent" into them. Unlike generators, Consumers do not produce data iteratively (no 'next' method), but they do produce a result which can be accessed at any time using the .result() method.

Data is typically sent into a Consumer using the sendtools.send() function, which takes the form
{{{
output = send(source, target)
}}}
where source is an iterator producing data. target is a Consumer object into which the data is sent. output is the Consumer's result, returned after the source has been fully consumed, or the Consumer indicates it's complete (by raising StopIteration), which ever happens first. Basically, the send function if a shortcut for writing a for-loop.

The target may be list or set, representing the data structure you want to collect the data into. These are implicitly converted to Consumer objects by the send function. The input list (or set) is returned by the send function having been filled with data. 

Target can also be a (multiply nested) tuple of consumers. In this case the result will be a tuple which matches the structure of the target tuple, containing the results for each consumer. In this way, data from a source iterator can be collected into multiple lists in a single iteration pass.

Sendtools defines many aggregation consumers. These do not produce a list or other collection as their result, but a scalar value.
