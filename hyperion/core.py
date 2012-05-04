from functools import partial
from itertools import chain

__all__ = ['Graph', 'Vertex', 'Edge', 'VertexSet', 'EdgeSet']


class Graph(object):
    def __init__(self, r, key_prefix='hyp'):
        self.r = r
        self.key_prefix = key_prefix
        self.counter_key = self.make_key('|V|')
        self.vertices_key = self.make_key('V')
        self.edge_out_key = partial(self.make_key, 'outE')
        self.edge_in_key = partial(self.make_key, 'inE')

    def make_key(self, *parts):
        return ':'.join([self.key_prefix] + list(parts))

    def get_vertex(self, v):
        if isinstance(v, Vertex):
            return v
        v = Vertex(self, v)
        if not self.has_vertex(v):
            raise NameError("vertex %s does not exist" % v)
        return v

    def add_vertex(self, name=None):
        if name is None:
            name = str(self.r.incr(self.counter_key))
        if self.r.sadd(self.vertices_key, name) != 1:
            raise NameError("vertex %s already exists" % name)
        return Vertex(self, name)

    def remove_vertex(self, v):
        v = self.get_vertex(v)
        for e in v.edges_out():
            self.remove_edge(e)
        for e in v.edges_in():
            self.remove_edge(e)
        return self.r.srem(self.vertices_key, v.name)

    def order(self):
        return self.r.scard(self.vertices_key)

    def vertices(self):
        vs = self.r.smembers(self.vertices_key)
        for v in vs:
            yield Vertex(self, v)

    def has_vertex(self, v):
        v = self.get_vertex(v)
        return self.r.sismember(self.vertices_key, v.name)

    def add_edge(self, fromv, tov, label=None, weight=1.0):
        fromv = self.get_vertex(fromv)
        tov = self.get_vertex(tov)

        def labeled(v):
            if label is None:
                return v.name
            return "%s/%s" % (v.name, str(label))

        with self.r.pipeline() as pipe:
            pipe.zadd(self.edge_out_key(fromv.name), labeled(tov), weight)
            pipe.zadd(self.edge_in_key(tov.name), labeled(fromv), weight)
            pipe.execute()
        return Edge(self, fromv, tov)

    def remove_edge(self, e):
        with self.r.pipeline() as pipe:
            pipe.zrem(self.edge_in_key(e.tov.name), e.fromv.name)
            pipe.zrem(self.edge_out_key(e.fromv.name), e.tov.name)
            pipe.execute()
        return True

    def edges_from(self, v):
        v = self.get_vertex(v)
        oute = self.r.zrange(self.edge_out_key(v.name), 0, -1)
        for e in oute:
            einfo = e.partition("/")
            edge = partial(Edge, self, v, Vertex(self, einfo[0]))
            if einfo[1] == '':
                yield edge()
            else:
                yield edge(label=einfo[2])

    def edges_to(self, v):
        v = self.get_vertex(v)
        ine = self.r.zrange(self.edge_in_key(v.name), 0, -1)
        for e in ine:
            einfo = e.partition("/")
            edge = partial(Edge, self, Vertex(self, einfo[0]), v)
            if einfo[1] == '':
                yield edge()
            else:
                yield edge(label=einfo[2])


class Vertex(object):
    def __init__(self, hyp, name):
        self.hyp = hyp
        self.name = name

    def edges_out(self):
        return self.hyp.edges_from(self)

    def edges_in(self):
        return self.hyp.edges_to(self)

    @property
    def in_e(self):
        return EdgeSet(list(self.edges_in()))

    @property
    def out_e(self):
        return EdgeSet(list(self.edges_out()))

    @property
    def in_v(self):
        return VertexSet([self]).in_v

    @property
    def out_v(self):
        return VertexSet([self]).out_v

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return "<Vertex %s>" % self.name


class VertexSet(set):
    @property
    def in_e(self):
        return EdgeSet(list(chain(*[v.edges_in() for v in self])))

    @property
    def out_e(self):
        return EdgeSet(list(chain(*[v.edges_out() for v in self])))

    @property
    def in_v(self):
        return self.in_e.out_v

    @property
    def out_v(self):
        return self.out_e.in_v


class Edge(object):
    def __init__(self, hyp, fromv, tov, label=None):
        self.hyp = hyp
        self.fromv = fromv
        self.tov = tov
        self.label = label

    def is_loop(self):
        return self.fromv == self.tov

    def __eq__(self, other):
        if self.fromv.name != other.fromv.name:
            return False
        if self.tov.name != other.tov.name:
            return False
        if self.label != other.label:
            return False
        return True

    def __str__(self):
        if self.label is None:
            arrow = "%s->%s" % (self.fromv.name, self.tov.name)
        else:
            arrow = "%s-%s->%s" % (self.fromv.name, self.label, self.tov.name)
        return "<Edge %s>" % arrow


class EdgeSet(set):
    @property
    def in_v(self):
        return VertexSet(e.tov for e in self)

    @property
    def out_v(self):
        return VertexSet(e.fromv for e in self)
