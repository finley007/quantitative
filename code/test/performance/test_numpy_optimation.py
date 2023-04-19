#! /usr/bin/env python
# -*- coding:utf8 -*-
from array import array
import numpy
from common.aop import timing

@timing
def norm_square_list(vector):
     """
     >>> vector = range(1000000)
     >>> %timeit norm_square_list(vector_list)
     1000 loops, best of 3: 1.16 ms per loop
     """
     norm = 0
     for v in vector:
        norm += v*v
     return norm

@timing
def norm_square_list_comprehension(vector):
     """
     >>> vector = range(1000000)
     >>> %timeit norm_square_list_comprehension(vector_list)
     1000 loops, best of 3: 913 μs per loop
     """
     return sum([v*v for v in vector])

@timing
def norm_squared_generator_comprehension(vector):
     """
     >>> vector = range(1000000)
     >>> %timeit norm_square_generator_comprehension(vector_list)
     1000 loops, best of 3: 747 μs per loop
     """
     return sum(v*v for v in vector)

@timing
def norm_square_array(vector):
     """
     >>> vector = array('l', range(1000000))
     >>> %timeit norm_square_array(vector_array)
     1000 loops, best of 3: 1.44 ms per loop
     """
     norm = 0
     for v in vector:
        norm += v*v
     return norm

@timing
def norm_square_numpy(vector):
     """
     >>> vector = numpy.arange(1000000)
     >>> %timeit norm_square_numpy(vector_numpy)
     10000 loops, best of 3: 30.9 μs per loop
     """
     return numpy.sum(vector * vector) # X

@timing
def norm_square_numpy_dot(vector):
     """
     >>> vector = numpy.arange(1000000)
    图矩阵和矢量计算 109
     >>> %timeit norm_square_numpy_dot(vector_numpy)
     10000 loops, best of 3: 21.8 μs per loop
     """
     return numpy.dot(vector, vector)

if __name__ == '__main__':
    print(norm_square_list(range(1000000)))
    print(norm_square_list_comprehension(range(1000000)))
    print(norm_squared_generator_comprehension(range(1000000)))
    print(norm_square_array(array('l', range(1000000))))
    print(norm_square_numpy(numpy.arange(1000000)))
    print(norm_square_numpy_dot(numpy.arange(1000000)))

