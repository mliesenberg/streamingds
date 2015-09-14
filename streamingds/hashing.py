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
from __future__ import (absolute_import, division, print_function,
                        with_statement)

import random

from cytoolz import juxt
from pyhashxx import hashxx


class Hashing(object):
    """Simple class to help developing hash based datastructures like the
    count-min sketch or bloomfilters.
    """

    def __init__(self, num_slices, num_bits_per_slice):
        """Initialize the array of `hashfunctions`."""
        self.bits_per_slice = num_bits_per_slice
        self.slices = num_slices

    @property
    def bits_per_slice(self):
        return self._num_bits_per_slice

    @bits_per_slice.setter
    def bits_per_slice(self, value):
        self._num_bits_per_slice = value

    @property
    def slices(self):
        return self._num_slices

    @slices.setter
    def slices(self, value):
        self._num_slices = value

    @property
    def seeds(self):
        if not hasattr(self, '_seeds'):
            random.seed("hashing-seed-for-reproducibility")
            self._seeds = [random.randint(0, 2 ** 32)
                           for _ in range(self.bits_per_slice)]
        return self._seeds

    def _gen_hash_fn(self, seed, slices):
        return lambda x: hashxx(str(x), seed=seed) % slices

    @property
    def hash_functions(self):
        if not hasattr(self, '_hash_functions'):
            self._hash_functions = [
                self._gen_hash_fn(s, self.slices)
                for s in self.seeds]
        return self._hash_functions

    def hash_values(self, key):
        """Return the hashes for the given key."""
        return juxt(self.hash_functions)(key)
