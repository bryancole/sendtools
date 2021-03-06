#!/usr/bin/env python3
import sys
sys.path.append('..')
import unittest

import pyximport
pyximport.install()

import sendtools as st
import itertools
import random
from collections import defaultdict
from math import sqrt

class TestSendtools(unittest.TestCase):
    def test_send(self):
        a = list(range(5))
        b = []
        c = st.send(iter(a), b)
        self.assertTrue(a==c)
        self.assertTrue(b==c)
        self.assertFalse(a is c)
        self.assertTrue(b is c)
        
    def test_split(self):
        a = list(range(5))
        b,c = st.send(a, ([], []))
        
        self.assertTrue(a==b)
        self.assertTrue(a==c)
        self.assertFalse(b is a)
        self.assertFalse(c is a)
        self.assertFalse(b is c)
        
    def test_limit(self):
        a = list(range(10))
        b = st.send(a, st.Limit(5,[]))
        self.assertEqual(len(b), 5)
        self.assertEqual(b, a[:5])
        
    def test_split_stop(self):
        a = itertools.count()
        b,c,d = st.send(a, (st.Limit(5,[]),
                            st.Limit(7,[]),
                            st.Limit(9,[])))
        self.assertEqual(next(a), 10)
        
    def test_gmap(self):
        a = range(10)
        b = st.send(a, st.Map(lambda x:x*2, []))
        self.assertEqual(b, [x*2 for x in a])
        
    def test_gmap_exc(self):
        a = list(range(10))
        a[5] = "moo"
        b = st.send(a, st.Map(lambda x:x/2., [], catch=TypeError))
        del a[5]
        self.assertEqual(b,[x/2. for x in a])
        
    def test_group_by_n(self):
        a = range(100)
        b = st.send(a, st.GroupByN(10, []))
        self.assertTrue(all(len(x) for x in b))
        
    def test_null_obj(self):
        self.assertTrue(12.5 == st.NULL_OBJ())
        self.assertTrue(st.NULL_OBJ() == "moo")
        
        
class TestAddToSet(unittest.TestCase):
    def test_set(self):
        data = range(10)
        result = st.send(data, set())
        self.assertEqual(set(data), result)
        
        
class TestGroupByKey(unittest.TestCase):
    def test_key_group(self):
        data = ([1]*5) + ([4]*9) + ([3]*7)
        result = st.send(data, st.GroupByKey(None, []) )
        self.assertTrue(result==[[1]*5,[4]*9,[3]*7])
        
        
class TestSwitchByKey(unittest.TestCase):
    def test_no_init_no_factory_no_test(self):
        data = [1,2,4,3,2,2,2,3,2,2,3,1,2,4,4,4]
        result = st.send(data, st.SwitchByKey())
        vals = defaultdict(list)
        for item in data:
            vals[item].append(item)
        self.assertEqual(result, vals)
        
    def test_no_init_no_factory(self):
        data = [1,2,4,3,2,2,2,3,2,2,3,1,2,4,4,4]
        result = st.send(data, st.SwitchByKey(lambda x:"hello"[x]))
        vals = defaultdict(list)
        for item in data:
            vals["hello"[item]].append(item)
        self.assertEqual(result, vals)
        
    def test_no_init(self):
        data = [1,2,4,3,2,2,2,3,2,2,3,1,2,4,4,4]
        data = [(a,i) for i,a in enumerate(data)]
        result = st.send(data, st.SwitchByKey(lambda x:x[0],
                                    factory=lambda :st.Map(lambda x:x[1], set())))
        vals = defaultdict(set)
        for item in data:
            vals[item[0]].add(item[1])
        self.assertEqual(result, vals)
        
    def test_gbk(self):
        data = [1,2,4,3,2,2,2,3,2,2,3,1,2,4,4,4]
        data = [(a,i) for i,a in enumerate(data)]
        result = st.send(data, st.SwitchByKey(lambda x:x[0], 
                                    init={1:st.Get(1,[]),
                                        2:st.Get(1, set()),
                                        3:st.Get(1, st.Sum())},
                                    factory=lambda : st.Get(1,[])
                                    ))
        vals = {}
        vals[1] = [i for a,i in data if a==1]
        vals[2] = set([i for a,i in data if a==2])
        vals[3] = sum(i for a,i in data if a==3)
        vals[4] = [i for a,i in data if a==4]
        self.assertEqual(result, vals)
        
        
class TestSlice(unittest.TestCase):
    def test_slice_1(self):
        data = list(range(10))
        ret = st.send(data, st.Slice(7, []))
        self.assertEqual(ret, data[slice(7)])
        
    def test_slice_2(self):
        data = list(range(20))
        ret = st.send(data, st.Slice(7,13, []))
        self.assertEqual(ret, data[slice(7,13)])
        
    def test_slice_3(self):
        data = list(range(30))
        ret = st.send(data, st.Slice(7,23,3, []))
        self.assertEqual(ret, data[slice(7,23,3)])
        
    def test_slice_4(self):
        data = list(range(30))
        ret = st.send(data, st.Slice(None,None,3, []))
        self.assertEqual(ret, data[slice(None,None,3)])
        
        
class TestFilter(unittest.TestCase):
    def test_filter(self):
        data = range(30)
        func = lambda x:not x%3
        ret = st.send(data, st.Filter(func, []))
        self.assertEqual(ret, list(filter(func, data)))
        
        
class TestSwitch(unittest.TestCase):
    def test_simple_switch(self):
        data = list(range(10))
        func = lambda x:int(x<5)
        ret = st.send(data, st.Switch(func, [],[]))
        self.assertEqual(ret[1], data[:5])
        self.assertEqual(ret[0], data[5:])
        
        
class TestUnzip(unittest.TestCase):
    def test_unpack_tuple(self):
        data = (("a", "b") for i in range(20))
        a,b = st.send(data, st.Unzip([],[]))
        self.assertEqual(a, ["a"]*20)
        self.assertEqual(b, ["b"]*20)
        
    def test_insufficient(self):
        self.assertRaises(TypeError, self.insufficient)
        
    def insufficient(self):
        data = [(1,2,3),
                (4,5,6),
                (7,8),
                (9,10,11)
                ]
        a,b,c = st.send(data, st.Unzip([],[],[]))
        
    def test_excess(self):
        data = [(1,2,3),
                iter((4,5,6)),
                (7,8,9,10,11),
                (12,13,14)]
        a,b,c = st.send(data, st.Unzip([],[],[]))
        self.assertEqual(a, [1,4,7,12])
        self.assertEqual(b, [2,5,8,13])
        self.assertEqual(c, [3,6,9,14])
        
        
class TestAggregates(unittest.TestCase):
    def setUp(self):
        self.data = [random.random() for i in range(50)]
    
    def test_max(self):
        result = st.send(self.data, st.Max())
        self.assertEqual(max(self.data), result)
        
    def test_min(self):
        result = st.send(self.data, st.Min())
        self.assertEqual(min(self.data), result)
        
    def test_sum(self):
        result = st.send(self.data, st.Sum())
        self.assertEqual(sum(self.data), result)
        
    def test_count(self):
        result = st.send(self.data, st.Count())
        self.assertEqual(len(self.data), result)
        
    def test_all(self):
        data = [0]*50
        self.assertEqual(st.send(data,st.All()), all(data))
        data[33] = 1
        self.assertEqual(st.send(data,st.All()), all(data))
        data = [1]*50
        self.assertEqual(st.send(data,st.All()), all(data))
        data[33] = 0
        self.assertEqual(st.send(data,st.All()), all(data))
        
    def test_any(self):
        data = [0]*50
        self.assertEqual(st.send(data,st.Any()), any(data))
        data[33] = 1
        self.assertEqual(st.send(data,st.Any()), any(data))
        data = [1]*50
        self.assertEqual(st.send(data,st.Any()), any(data))
        data[33] = 0
        self.assertEqual(st.send(data,st.Any()), any(data))
        
    def test_ave(self):
        result = st.send(self.data, st.Ave())
        self.assertAlmostEqual(sum(self.data)/len(self.data), result)
        
    def test_stats(self):
        result = st.send(self.data, st.Stats())
        N = len(self.data)
        mean = sum(self.data)/N
        self.assertAlmostEqual(mean, result[1])
        std = sqrt(sum((x-mean)**2 for x in self.data)/(N-1))
        self.assertAlmostEqual(std, result[2])
        self.assertEqual(N, result[0])
        
    def test_select(self):
        data = range(10)
        for i in data:
            val = st.send(data, st.Select(i))
            self.assertEqual(val, i)
            
    def test_select_trans(self):
        data = range(10)
        for i in data:
            val = st.send(data, st.Select(i,transform=lambda x:2*x))
            self.assertEqual(val, i*2)
            
        
        
        
if __name__=="__main__":
    unittest.main()
    