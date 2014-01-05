# vim: set fileencoding=utf-8 :
#
# Copyright (c) 2013 Daniel Truemper <truemped at googlemail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
"""An implementation of the count-min sketching Cormode and Muthukrishnan 2005.

Based on the two implementations:

https://tech.shareaholic.com/2012/12/03/the-count-min-sketch-how-to-count-over-
                               large-keyspaces-when-about-right-is-good-enough/
http://www.nightmare.com/rushing/python/countmin.py
"""
from __future__ import (absolute_import, division, print_function,
                        with_statement)

import math
import random
import sys

from streamingds.heap import Heap


int_size = len(bin(sys.maxint)) - 1
int_mask = (1 << int_size) - 1
int_ceil = lambda x: int(math.ceil(x))


log2 = lambda x: math.log(x) / math.log(2.0)
multiply_shift = lambda m, a, x: ((a * x) & int_mask) >> (int_size - m)
random_odd_int = lambda: (int(random.getrandbits(int_size - 2))) << 1 | 1


def median(values):
    values.sort()
    if len(values) % 2 == 1:
        return values[(len(values) + 1) // 2 - 1]
    else:
        lower = values[len(values) // 2 - 1]
        upper = values[len(values) // 2]
        return (float(lower + upper)) // 2


class CountMinSketch(object):
    """A count-min sketch to track counts of keys in a stream.
    """

    def __init__(self, delta, epsilon, k):
        """Setup a new count-min sketch with parameters delta, epsilon and k

        The parameters delta and epsilon control the accuracy of the
        estimates of the sketch

        Cormode and Muthukrishnan prove that for an item i with count a_i, the
        estimate from the sketch a_i_hat will satisfy the relation

        a_hat_i <= a_i + epsilon * ||a||_1

        with probability at least 1 - delta, where a is the the vector of all
        all counts and ||x||_1 is the L1 norm of a vector x

        Parameters
        ----------
        delta : float
            A value in the unit interval that sets the precision of the sketch
        epsilon : float
            A value in the unit interval that sets the precision of the sketch
        k : int
            A positive integer that sets the number of top items counted

        Examples
        --------
        >>> s = CountMinSketch(10**-7, 0.005, 40)

        Raises
        ------
        ValueError
            If delta or epsilon are not in the unit interval, or if k is
            not a positive integer
        """
        if not 0 <= delta <= 1:
            raise ValueError('delta must be betweet 0 and 1')
        if not 0.001 <= epsilon <= 1:
            raise ValueError('epsilon must be between 0.001 and 1')
        if k < 1 or k != int(k):
            raise ValueError('k must be a positive integer')

        self.k = k
        self._width = int_ceil(math.e / epsilon)
        self._depth = int_ceil(math.log(1.0 / delta))
        self.lg_width = int(math.ceil(log2(float(self._width))))

        self.known_keys = {}
        self.top_est = {}

    @property
    def count(self):
        """A simple property that can be changed in more specific
        implementations.

        The `count` property contains the matrix of counts for each hash
        function.
        """
        if not hasattr(self, '_count'):
            rounded_width = 1 << self.lg_width
            self._count = [[0] * rounded_width for _ in range(self._depth)]
        return self._count

    @property
    def hash_functions(self):
        """A simple property that can be changed in order to provide some kind
        of persistence.
        """
        if not hasattr(self, '_hash_functions'):
            self._hash_functions = [random_odd_int()
                                    for _ in range(self._depth)]
        return self._hash_functions

    @property
    def heap(self):
        """A simple heap property hiding spezialized implementations."""
        if not hasattr(self, '_heap'):
            self._heap = Heap()
        return self._heap

    def update(self, key, increment=1):
        """Updates the sketch for the item with name of key by the amount
        specified in increment

        Parameters
        ----------
        key : string
            The item to update the value of in the sketch
        increment : integer
            The amount to update the sketch by for the given key

        Examples
        --------
        >>> s = CountMinSketch(10**-7, 0.005, 40)
        >>> s.update('http://www.cnn.com/', 1)
        """
        ix = abs(hash(key))
        est = sys.maxint
        for i in range(len(self.hash_functions)):
            hf = self.hash_functions[i]
            j = multiply_shift(self.lg_width, hf, ix)
            self.count[i][j] = (self.count[i][j] + increment)
            est = min(est, self.count[i][j])
        self.update_heap(key, self.get(key))

    def get(self, key):
        """Fetches the sketch estimate for the given key

        Parameters
        ----------
        key : string
            The item to produce an estimate for

        Returns
        -------
        estimate : int
            The best estimate of the count for the given key based on the
            sketch

        Examples
        --------
        >>> s = CountMinSketch(10**-7, 0.005, 40)
        >>> s.update('http://www.cnn.com/', 1)
        >>> s.get('http://www.cnn.com/')
        1
        """
        ix = abs(hash(key))
        r = sys.maxint
        for i in range(len(self.hash_functions)):
            hf = self.hash_functions[i]
            j = multiply_shift(self.lg_width, hf, ix)
            r = min(r, self.count[i][j])
        return r

    def update_heap(self, key, est):
        """Updates the class's heap that keeps track of the top k items for a
        given key

        For the given key, it checks whether the key is present in the heap,
        updating accordingly if so, and adding it to the heap if it is
        absent

        Parameters
        ----------
        key : string
            The item to check against the heap
        est : integer
            The best estimate of the count for the given key
        """
        if len(self.heap) < self.k or est >= self.heap.min():
            if key in self.known_keys:
                # we already know the key, i.e. it already exists in top_est so
                # we remove it from the it's presumably old estimation
                key_est = self.known_keys[key]
                if key in self.top_est[key_est]:
                    self.top_est[key_est].remove(key)
                    if len(self.top_est[key_est]) == 0:
                        del self.top_est[key_est]
                        self.heap.remove(key_est)
                del self.known_keys[key]

            if est in self.top_est:
                # we already know the estimate, add the key to the list
                self.top_est[est].append(key)
                self.known_keys[key] = est
            else:
                if len(self.top_est) < self.k:
                    self.heap.push(est)
                    self.top_est[est] = [key]
                    self.known_keys[key] = est
                else:
                    oest = self.heap.pushpop(est)
                    if oest in self.top_est:
                        for k in self.top_est[oest]:
                            del self.known_keys[k]
                        del self.top_est[oest]
                    self.top_est[est] = [key]
                    self.known_keys[key] = est

    def get_ranking(self):
        """Convinience method to return a dictionary with the ranking and
        estimations.
        """
        vals = self.top_est.items()
        vals.sort()
        vals.reverse()
        r = dict([(i, vals[i]) for i in range(len(vals))])
        return r


class CountMeanSketch(CountMinSketch):
    """A `Count-Mean Sketch`."""

    def __init__(self, delta, epsilon, k):
        """Setup a new count-mean sketch."""
        super(CountMeanSketch, self).__init__(delta, epsilon, k)
        self.n = 0

    def update(self, key, increment=1):
        self.n += 1
        super(CountMeanSketch, self).update(key, increment=increment)

    def get(self, key):
        """Fetches the sketch estimate for the given key

        Parameters
        ----------
        key : string
            The item to produce an estimate for

        Returns
        -------
        estimate : int
            The best estimate of the count for the given key based on the
            sketch

        Examples
        --------
        >>> s = CountMinSketch(10**-7, 0.005, 40)
        >>> s.update('http://www.cnn.com/', 1)
        >>> s.get('http://www.cnn.com/')
        1
        """
        ix = abs(hash(key))
        e = [0] * len(self.hash_functions)
        for i in range(len(self.hash_functions)):
            hf = self.hash_functions[i]
            j = multiply_shift(self.lg_width, hf, ix)
            sketchCounter = self.count[i][j]
            noiseEstimation = (self.n - sketchCounter) / (self._width - 1)
            e[i] = sketchCounter - noiseEstimation
        return median(e)
