# vim: set fileencoding=utf-8 :
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
import math
from hashlib import sha1

_ALPHA_VALUES = {
    4 : 0.673,
    5 : 0.697,
    6 : 0.709
}

def get_alpha(p):
    return _ALPHA_VALUES.get(p, 0.7213 / (1.0 + 1.079 / (1 << p)))

def get_rho(w, max_width):
    return max_width - w.bit_length() + 1


class HyperLogLog(object):
    """
        A HyperLogLog++ counter implementation as published in
        http://research.google.com/pubs/pub40671.html
        (without the threshold approximations)

        and discussed at length in
        http://research.neustar.biz/2012/10/25/sketch-of-the-day-hyperloglog-cornerstone-of-a-big-data-infrastructure/
    """

    def __init__(self, p):
        if not (p >= 4 and p <= 16):
            raise ValueError("p must be in range 4 to 16")
        self._p = p
        self._alpha = get_alpha(p)
        self._m = 1 << self._p
        self._registers = [0] * self._m

        self._max_bits = 64

    def add(self, *elements):
        """
            Adds all elements to the counter.
        """

        for element in elements:
            # convert to bytes, hash it, convert hash object to hexadecimal
            # convert from hexadecimal to 64bit long.
            x = long(sha1(bytes(element)).hexdigest()[:16], 16)

            j = x & (self._m - 1)
            w = x >> self._p

            # update the register if applicable
            self._registers[j] = max(self._registers[j],
                                     get_rho(w, self._max_bits - self._p))

    def merge(self, other):
        """
            Merge the other HLL instance into the current one by
            updating the register.
        """
        if self._m != other._m:
            raise ValueError("Can't merge HLLs with different precisions.")
        else:
            self._registers = [max(*x) for x in zip(self._registers,
                                                    other._registers)]

    def cardinality(self):
        """
            Return the estimated number of unique elements the counter
            has seen so far.
        """
        estimate = self._alpha * math.pow(self._m, 2) / sum(math.pow(2, -x) for x in self._registers)

        if estimate <= 2.5 * self._m:
            # get number of registers equal to zero
            empty_registers = self._registers.count(0)
            if empty_registers != 0:
                return self._linear_count(empty_registers)
            else:
                return estimate
        elif estimate <= ((1 << 32) / 30):
            return estimate
        else:
            return self._large_range_correction(estimate)

    @property
    def error_rate(self):
        return  1.04 / math.sqrt(self._m)

    def _linear_count(self, empty_registers):
        """
            bias correction for small range values
        """
        return self._m * math.log(self._m / empty_registers)

    def _large_range_correction(self, old_estimate):
        """
            bias correction for large range values
        """
        return -(1 << 32) * math.log(1 - (old_estimate / (1 << 32)))

    def __eq__(self, other):
        if not isinstance(other, HyperLogLog):
            return False
        else:
            if self._p != other._p:
                raise ValueError("")
            else:
                return self._registers == other._registers

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return round(self.cardinality())
