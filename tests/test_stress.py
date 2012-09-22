import os
from redis import Redis
from nose.tools import eq_
from .context import hyperion

h = None


def setup():
    global h
    db = 10
    h = hyperion.Graph(Redis(db=db), 'testing')
    if h._r.dbsize() > 0:
        raise RuntimeError("Redis DB %d is not empty" % db)


def teardown():
    global h
    h._r.flushdb()


def dataset_load_test():
    h.load_tsv(os.path.dirname(__file__) + "/data/epinions.txt")
    eq_(h.order(), 75879)
    eq_(len(h.v('4').out_v), 76)
    eq_(len(h.v('4').in_v), 125)
