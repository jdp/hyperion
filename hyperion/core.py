from functools import partial
from itertools import chain

__all__ = ['Graph', 'Vertex', 'Edge', 'VertexSet', 'EdgeSet']


class Graph(object):
    def __init__(self, r, name):
        self.r = r
        self.name = name
        self.counter_key = self.make_key('|V|')
        self.vertex_key = partial(self.make_key, 'V')
        self.vertices_key = self.make_key('V(G)')
        self.edges_key = self.make_key('V(E)')
        self.edge_out_key = partial(self.make_key, 'outE')
        self.edge_in_key = partial(self.make_key, 'inE')

    def make_key(self, *parts):
        return ':'.join(chain([self.name], parts))

    def get_vertex(self, v):
        if isinstance(v, Vertex):
            return v
        v = Vertex(self, v)
        if not self.has_vertex(v):
            raise NameError("vertex %s does not exist" % v)
        return v

    def get_vertex_property(self, v, name):
        self.r.hget(self.vertex_key(v.name), name)

    def set_vertex_property(self, v, name, value):
        return self.r.hset(self.vertex_key(v.name), name, value)

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
            pipe.sadd(self.edges_key, '%s:%s' % (labeled(fromv), tov.name))
            pipe.zadd(self.edge_out_key(fromv.name), labeled(tov), weight)
            pipe.zadd(self.edge_in_key(tov.name), labeled(fromv), weight)
            pipe.execute()
        return Edge(self, fromv, tov, label=label)

    def remove_edge(self, e):
        def labeled(v):
            if e.label is None:
                return v.name
            return '%s/%s' % (v.name, str(e.label))

        with self.r.pipeline() as pipe:
            pipe.srem(self.edges_key, '%s:%s' % (labeled(e.fromv), e.tov.name))
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

    def edges(self):
        for e in self.r.smembers(self.edges_key):
            vertices = e.split(":")
            einfo = vertices[0].partition("/")
            edge = partial(Edge, self, Vertex(self, einfo[0]), Vertex(self, vertices[1]))
            if einfo[1] == '':
                yield edge()
            else:
                yield edge(label=einfo[2])


class Vertex(object):
    def __init__(self, g, name):
        self.g = g
        self.name = name

    def edges_out(self):
        return self.g.edges_from(self)

    def edges_in(self):
        return self.g.edges_to(self)

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

    def __getitem__(self, key):
        return self.g.get_vertex_property(self, key)

    def __setitem__(self, key, value):
        return self.g.set_vertex_property(self, key, value)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return "<Vertex %s>" % self.name


class Edge(object):
    def __init__(self, g, fromv, tov, label=None):
        self.g = g
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


class ComponentSet(set):
    def filter(self, fn):
        return self.__class__(filter(fn, self))

    def map(self, fn):
        return self.__class__(map(fn, self))

    def reduce(self, fn):
        return self.__class__(reduce(fn, self))


class VertexSet(ComponentSet):
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


class EdgeSet(ComponentSet):
    @property
    def in_v(self):
        return VertexSet(e.tov for e in self)

    @property
    def out_v(self):
        return VertexSet(e.fromv for e in self)
