#!/usr/bin/env python

import pyximport
pyximport.install()

import _sendtools as st1
import sendtools as st2
import itertools
import timeit

def source():
    #itr = itertools.cycle("hello world")
    for i in xrange(100000):
        yield (i, ("b",i))
        
def test1(m):
    a,b = m.send(source(), (m.getter([],0), 
                            m.getter(m.gmap(len, []),1)) )
    
def test_for_loop():
    a,b = [], []
    for i,j in source():
        a.append(i)
        b.append(len(j))
    return a,b
        
    
t1 = timeit.Timer("test1(st1)", "from __main__ import st1, test1")
T1 = t1.timeit(5)
print "C version:", T1

#t2 = timeit.Timer("test1(st2)", "from __main__ import st2, test1")
#T2 = t2.timeit(5)
#print "Py version:", T2

t3 = timeit.Timer("test_for_loop()", "from __main__ import test_for_loop")
T3 = t3.timeit(5)
print "For-loop version:", T3

print "ratio", T1/T3
