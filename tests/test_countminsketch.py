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

from collections import defaultdict
import heapq
import pytest
from random import randint

from streamingds.countminsketch import CountMinSketch


def test_simple_count_min_sketch():
    c = CountMinSketch(10 ** -7, 0.01, 50)

    c.update('www.google.de', 10)
    assert c.get('www.google.de') == 10

    exp = {0: (10, ['www.google.de'])}
    assert c.get_ranking() == exp

    c.update('www.bing.com', 12)
    assert c.get('www.bing.com') == 12

    exp = {0: (12, ['www.bing.com']),
           1: (10, ['www.google.de'])}
    assert c.get_ranking() == exp

    c.update('www.yahoo.com', 28)
    assert c.get('www.yahoo.com') == 28

    exp = {0: (28, ['www.yahoo.com']),
           1: (12, ['www.bing.com']),
           2: (10, ['www.google.de'])}
    assert c.get_ranking() == exp

    c.update('www.google.de', 2)
    exp = {0: (28, ['www.yahoo.com']),
           1: (12, ['www.bing.com', 'www.google.de'])}
    assert c.get_ranking() == exp

    c.update('www.google.de', 2)
    exp = {0: (28, ['www.yahoo.com']),
           2: (12, ['www.bing.com']),
           1: (14, ['www.google.de'])}
    assert c.get_ranking() == exp


@pytest.mark.parametrize("k,amount_keys,replays",
                         [(randint(10, 50),
                           randint(80, 100),
                           randint(2, 8)) for _ in range(200)
                          ]
                         )
def test_random_sketch(k, amount_keys, replays):
    tests = []
    results = defaultdict(int)

    cms = CountMinSketch(10 ** -7, 0.01, k)
    for i in range(replays):
        for i in range(amount_keys):
            key = 'random-key-%s' % i
            n = randint(10, 1000000)
            tests.append((i, n))
            results[key] += n
            cms.update(key, n)

    exp_results = dict([(v, kk) for (kk, v) in results.iteritems()])
    exp = heapq.nlargest(k, exp_results.items())
    ranking = cms.get_ranking()
    try:
        assert len(exp) == len(ranking)
        for i, (amnt, key) in enumerate(exp):
            (a_amnt, keys) = ranking[i]
            assert key in keys
            assert amnt == a_amnt
    except Exception:
        from datetime import datetime
        import pickle
        import traceback

        traceback.print_exc()
        print('k=%s, amount_keys=%s, replays=%s' % (k, amount_keys,
                                                    replays))
        print("exp", exp)
        print("ranking", ranking)
        test = {'k': k, 'amount_keys': amount_keys, 'replays': replays,
                'tests': tests, 'exp': exp, 'ranking': ranking}
        fn = 'failed-tests/failed-test-%s.pickle' % datetime.now()
        with open(fn, 'wb') as f:
            pickle.dump(test, f)
        raise


def test_merge_non_matching_parameters():
    with pytest.raises(ValueError):
        c1 = CountMinSketch(10 ** -7, 0.01, 50)
        c2 = CountMinSketch(10 ** -8, 0.11, 50)

        c1.merge(c2)


def test_merge_matching_parameters():
    c1 = CountMinSketch(10 ** -7, 0.01, 50)
    c2 = CountMinSketch(10 ** -7, 0.01, 50)

    c1.update('www.google.de', 10)
    c1.update('www.linkedin.com', 5)

    c2.update('www.bing.com', 12)
    c2.update('www.google.de', 10)

    c1.merge(c2)

    assert c1.get('www.google.de') == 20
    assert c1.get('www.linkedin.com') == 5
    assert c1.get('www.bing.com') == 12
