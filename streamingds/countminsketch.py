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
import sys

from streamingds.hashing import Hashing
from streamingds.heap import Heap


int_ceil = lambda x: int(math.ceil(x))
log2 = lambda x: math.log(x) / math.log(2.0)


class CountMinSketch(Hashing):
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
        w = int(math.ceil(math.e / epsilon))
        d = int(math.ceil(math.log(1.0 / delta)))

        super(CountMinSketch, self).__init__(w, d)

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
            self._count = [[0] * self.slices
                           for _ in range(self.bits_per_slice)]
        return self._count

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
        hashes = self.hash_values(key)
        est = sys.maxint
        for i, h in enumerate(hashes):
            self.count[i][h] = self.count[i][h] + increment
            est = min(est, self.count[i][h])
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
        hashes = self.hash_values(key)
        r = sys.maxint
        for i, h in enumerate(hashes):
            r = min(r, self.count[i][h])
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

    def merge(self, other):
        """merge the other CountMinSketch into the current instance."""
        if self.bits_per_slice != other.bits_per_slice or self.slices != other.slices:
            raise ValueError("Dimensions must be equal for merge.")

        if self.k != other.k:
            raise ValueError("k must be equal for merge.")

        for i, k in enumerate(other.count):
            for j, l in enumerate(k):
                self.count[i][j] += l
