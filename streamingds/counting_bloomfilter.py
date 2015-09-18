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

from streamingds.bloomfilter import BloomFilter


class CountingBloomFilter(BloomFilter):
    """
        BloomFilter supporting removal.
    """

    def __init__(self, capacity, error_rate=0.001):
        super(CountingBloomFilter, self).__init__(capacity, error_rate)

    def add(self, key):
        """Add a key to this filter."""
        for val in self.hash_values(key):
            self.filter[val] += 1

    def remove(self, key):
        for val in self.hash_values(key):
            self.filter[val] -= 1

    def __contains__(self, key):
        """Check membership of a key in this filter."""
        return all(map(lambda x: self.filter[x] >= 1, self.hash_values(key)))

    @property
    def filter(self):
        if not hasattr(self, '_countingfilter'):
            self._countingfilter = [0] * self.slices
        return self._countingfilter
