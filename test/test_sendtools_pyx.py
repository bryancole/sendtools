#!/usr/bin/env python
import sys
sys.path.append('..')
import unittest

import pyximport
pyximport.install()

import _sendtools as st
import itertools
import random

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
        b = st.send(a, st.Limit(5,[]))
        self.assertEquals(len(b), 5)
        self.assertEquals(b, a[:5])
        
    def test_split_stop(self):
        a = itertools.count()
        b,c,d = st.send(a, (st.Limit(5,[]),
                            st.Limit(7,[]),
                            st.Limit(9,[])))
        self.assertEquals(a.next(), 10)
        
    def test_gmap(self):
        a = range(10)
        b = st.send(a, st.Map(lambda x:x*2, []))
        self.assertEquals(b, [x*2 for x in a])
        
    def test_gmap_exc(self):
        a = range(10)
        a[5] = "moo"
        b = st.send(a, st.Map(lambda x:x/2., [], catch=TypeError))
        del a[5]
        self.assertEquals(b,[x/2. for x in a])
        
    def test_group_by_n(self):
        a = xrange(100)
        b = st.send(a, st.GroupByN(10, []))
        self.assertTrue(all(len(x) for x in b))
        
    def test_null_obj(self):
        self.assertTrue(12.5 == st.NULL_OBJ())
        self.assertTrue(st.NULL_OBJ() == "moo")
        
        
class TestGroupByKey(unittest.TestCase):
    def test_key_group(self):
        data = ([1]*5) + ([4]*9) + ([3]*7)
        result = st.send(data, st.GroupByKey([]) )
        self.assertTrue(result==[[1]*5,[4]*9,[3]*7])
        
        
class TestAggregates(unittest.TestCase):
    def setUp(self):
        self.data = [random.random() for i in xrange(50)]
    
    def test_max(self):
        result = st.send(self.data, st.Max())
        self.assertEquals(max(self.data), result)
        
    def test_min(self):
        result = st.send(self.data, st.Min())
        self.assertEquals(min(self.data), result)
        
    def test_sum(self):
        result = st.send(self.data, st.Sum())
        self.assertEquals(sum(self.data), result)
        
    def test_count(self):
        result = st.send(self.data, st.Count())
        self.assertEquals(len(self.data), result)
        
    def test_ave(self):
        result = st.send(self.data, st.Ave())
        self.assertEquals(sum(self.data)/len(self.data), result)
        
        
if __name__=="__main__":
    unittest.main()
    