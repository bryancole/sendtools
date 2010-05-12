#!/usr/bin/env python
import sys
sys.path.append('..')
import unittest

import pyximport
pyximport.install()

import _sendtools as st
import itertools

class TestSendtools(unittest.TestCase):
    def test_send(self):
        a = range(5)
        b = []
        c = st.send(iter(a), b)
        self.assertTrue(a==c)
        self.assertTrue(b==c)
        self.assertFalse(a is c)
        self.assertTrue(b is c)
        
    def test_split(self):
        a = range(5)
        b,c = st.send(a, ([], []))
        
        self.assertTrue(a==b)
        self.assertTrue(a==c)
        self.assertFalse(b is a)
        self.assertFalse(c is a)
        self.assertFalse(b is c)
        
    def test_limit(self):
        a = range(10)
        b = st.send(a, st.limit(5,[]))
        self.assertEquals(len(b), 5)
        self.assertEquals(b, a[:5])
        
    def test_split_stop(self):
        a = itertools.count()
        b,c,d = st.send(a, (st.limit(5,[]),
                            st.limit(7,[]),
                            st.limit(9,[])))
        self.assertEquals(a.next(), 10)
        
    def test_gmap(self):
        a = range(10)
        b = st.send(a, st.gmap(lambda x:x*2, []))
        self.assertEquals(b, [x*2 for x in a])
        
    def test_gmap_exc(self):
        a = range(10)
        a[5] = "moo"
        b = st.send(a, st.gmap(lambda x:x/2., [], catch=TypeError))
        del a[5]
        self.assertEquals(b,[x/2. for x in a])
        
    def test_group_by_n(self):
        a = xrange(100)
        b = st.send(a, st.group_by_n(10, []))
        self.assertTrue(all(len(x) for x in b))
        
    def test_null_obj(self):
        self.assertTrue(12.5 == st.NULL_OBJ())
        self.assertTrue(st.NULL_OBJ() == "moo")
        
        
class TestGroupByKey(unittest.TestCase):
    def test_key_group(self):
        data = ([1]*5) + ([4]*9) + ([3]*7)
        result = st.send(data, st.group_by_key([]) )
        self.assertTrue(result==[[1]*5,[4]*9,[3]*7])
        
        
if __name__=="__main__":
    unittest.main()
    