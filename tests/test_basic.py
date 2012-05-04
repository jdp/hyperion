from redis import Redis
from nose.tools import with_setup, raises, eq_, ok_
from .context import hyperion

h = None


def setup():
    global h
    db = 9
    h = hyperion.Graph(Redis(db=db))
    if len(h.r.keys('*')) > 0:
        raise RuntimeError("Redis DB %d is not empty" % db)


def teardown():
    global h
    h.r.flushdb()


@with_setup(teardown=teardown)
def add_vertex_test():
    v1 = h.add_vertex()
    ok_(h.has_vertex(v1))
    v2 = h.add_vertex()
    ok_(h.has_vertex(v2))
    eq_(len(list(h.vertices())), 2)


@with_setup(teardown=teardown)
def order_test():
    h.add_vertex()
    h.add_vertex()
    h.add_vertex()
    eq_(h.order(), 3)


@with_setup(teardown=teardown)
def add_named_vertex_test():
    foo = h.add_vertex('foo')
    ok_(h.has_vertex(foo))
    ok_(h.has_vertex('foo'))
    eq_(len(list(h.vertices())), 1)


@with_setup(teardown=teardown)
def add_edge_test():
    v1 = h.add_vertex()
    v2 = h.add_vertex()
    ok_(isinstance(h.add_edge(v1, v2), hyperion.Edge))
    eq_(len(list(v1.edges_out())), 1)
    eq_(len(list(v2.edges_in())), 1)
    eq_(list(v1.edges_out())[0], list(v2.edges_in())[0])


@with_setup(teardown=teardown)
def remove_vertex_test():
    v1 = h.add_vertex()
    v2 = h.add_vertex()
    v3 = h.add_vertex()
    h.add_edge(v1, v2)
    h.add_edge(v1, v3)
    h.add_edge(v2, v1)
    h.add_edge(v2, v3)
    h.remove_vertex(v1)
    eq_(len(list(h.vertices())), 2)
    eq_(len(list(v2.edges_in())), 0)
    eq_(len(list(v2.edges_out())), 1)
    eq_(len(list(v3.edges_in())), 1)


@with_setup(teardown=teardown)
def outward_traversal_test():
    vs = [h.add_vertex() for _ in range(5)]
    h.add_edge(vs[0], vs[1])
    h.add_edge(vs[0], vs[2])
    h.add_edge(vs[0], vs[3])
    eq_(len(vs[0].out_e), 3)
    h.add_edge(vs[2], vs[4])
    eq_(len(vs[0].out_e.in_v.out_e), 1)
    eq_(len(vs[0].out_v.out_v), 1)


@with_setup(teardown=teardown)
def inward_traversal_test():
    vs = [h.add_vertex() for _ in range(5)]
    h.add_edge(vs[1], vs[0])
    h.add_edge(vs[2], vs[0])
    h.add_edge(vs[3], vs[0])
    eq_(len(vs[0].in_e), 3)
    h.add_edge(vs[4], vs[1])
    eq_(len(vs[0].in_e.out_v.in_e), 1)
    eq_(len(vs[0].in_v.in_v), 1)
