import json
from functools import partial
from itertools import chain, ifilterfalse, imap, izip_longest

__all__ = ['Graph', 'Vertex', 'Edge', 'VertexSet', 'EdgeSet']


class Graph(object):
    def __init__(self, r, name, encoder=json.dumps, decoder=json.loads):
        self._r = r
        self._name = name
        make_key = lambda *parts: ':'.join(chain([self._name], parts))
        self._counter_key = make_key('|V|')
        self._vertex_key = partial(make_key, 'V')
        self._vertices_key = make_key('V(G)')
        labels_key = partial(make_key, 'L(E(V))')
        self._labels_out_key = partial(labels_key, 'out')
        self._labels_in_key = partial(labels_key, 'in')
        edges_key = partial(make_key, 'E(V)')
        self._edges_out_key = partial(edges_key, 'out')
        self._edges_in_key = partial(edges_key, 'in')
        self._encode = encoder
        self._decode = decoder

    @property
    def name(self):
        return self._name

    def get_vertex(self, vertex_or_name):
        v = vertex_or_name
        if not isinstance(v, Vertex):
            v = Vertex(self, vertex_or_name)
        if not self.has_vertex(v):
            raise LookupError("%s does not exist" % v)
        return v

    def v(self, vertex_or_name):
        return self.get_vertex(vertex_or_name)

    def has_vertex(self, v):
        return self._r.sismember(self._vertices_key, v.name)

    def add_vertex(self, name=None):
        if name is None:
            name = str(self._r.incr(self._counter_key))
        if self._store_vertex(self._r, name) != 1:
            raise LookupError("vertex named %s already exists" % name)
        return Vertex(self, name)

    def _store_vertex(self, r, name):
        return r.sadd(self._vertices_key, name)

    def get_vertex_property(self, v, name):
        value = self._r.hget(self._vertex_key(v.name), name)
        if not value:
            raise KeyError(name)
        return self._decode(value)

    def set_vertex_property(self, v, name, value):
        return self._r.hset(self._vertex_key(v.name), name, self._encode(value))

    def remove_vertex(self, v):
        v = self.get_vertex(v)
        for e in v.edges_out():
            self.remove_edge(e)
        for e in v.edges_in():
            self.remove_edge(e)
        return self._r.srem(self._vertices_key, v.name)

    def order(self):
        return self._r.scard(self._vertices_key)

    def vertices(self):
        vs = self._r.smembers(self._vertices_key)
        for v in vs:
            yield Vertex(self, v)

    def add_edge(self, fromv, tov, label=None, weight=1.0):
        fromv = self.get_vertex(fromv)
        tov = self.get_vertex(tov)
        with self._r.pipeline() as pipe:
            encoded_label = self._encode(label)
            self._store_edge(pipe, fromv.name, tov.name, encoded_label, weight)
            pipe.execute()
        return Edge(self, fromv, tov, label=label, weight=weight)

    def _store_edge(self, r, from_name, to_name, encoded_label, weight):
        r.zincrby(self._labels_out_key(from_name), encoded_label, 1)
        r.zadd(self._edges_out_key(from_name, encoded_label), to_name, weight)
        r.zincrby(self._labels_in_key(to_name), encoded_label, 1)
        r.zadd(self._edges_in_key(to_name, encoded_label), from_name, weight)

    def remove_edge(self, e):
        with self._r.pipeline() as pipe:
            encoded_label = self._encode(e.label)
            pipe.zincrby(self._labels_out_key(e.fromv.name), encoded_label, -1)
            pipe.zrem(self._edges_out_key(e.fromv.name, encoded_label), e.tov.name)
            pipe.zincrby(self._labels_in_key(e.tov.name), encoded_label, -1)
            pipe.zrem(self._edges_in_key(e.tov.name, encoded_label), e.fromv.name)
            pipe.execute()
        return True

    def edges_from(self, v):
        v = self.get_vertex(v)
        labels = self._r.zrangebyscore(self._labels_out_key(v.name), 1, '+inf')
        for label in labels:
            for name, weight in self._r.zrangebyscore(self._edges_out_key(v.name, label), '-inf', '+inf', withscores=True):
                yield Edge(self, v, Vertex(self, name), label=self._decode(label), weight=weight)

    def edges_to(self, v):
        v = self.get_vertex(v)
        labels = self._r.zrangebyscore(self._labels_in_key(v.name), 1, '+inf')
        for label in labels:
            for name, weight in self._r.zrangebyscore(self._edges_in_key(v.name, label), '-inf', '+inf', withscores=True):
                yield Edge(self, Vertex(self, name), v, label=self._decode(label), weight=weight)

    def edge_between(self, v1, v2, label=None):
        weight = self._r.zscore(self._edges_out_key(v1.name, self._encode(label)), v2.name)
        if weight is not None:
            return Edge(self, v1, v2, label, weight)
        weight = self._r.zscore(self._edges_out_key(v2.name, self._encode(label)), v1.name)
        if weight is not None:
            return Edge(self, v2, v1, label, weight)
        return None

    def edges(self):
        for v in self.vertices():
            for e in self.edges_from(v):
                yield e

    def load_csv(self, filename, chunksize=2000):
        return self._load_file(filename, chunksize, ',')

    def load_tsv(self, filename, chunksize=2000):
        return self._load_file(filename, chunksize, '\t')

    def _load_file(self, filename, chunksize, delim):
        def chunker(iterable, n):
            class Filler(object):
                pass
            return (ifilterfalse(lambda x: x is Filler, chunk) for chunk in (izip_longest(*[iter(iterable)] * n, fillvalue=Filler)))
        for chunk in imap(tuple, chunker(open(filename), chunksize)):
            with self._r.pipeline() as pipe:
                for line in chunk:
                    line = line.strip()
                    if line.startswith('#') or not line:
                        continue
                    fields = map(str.strip, line.split(delim))
                    label = None
                    weight = 1.0
                    if len(fields) == 2:
                        from_id, to_id = fields
                    elif len(fields) == 3:
                        from_id, label, to_id = fields
                    elif len(fields) == 4:
                        from_id, label, to_id, weight = fields
                        weight = float(weight)
                    else:
                        raise RuntimeError("invalid record: %s" % line)
                    self._store_vertex(pipe, from_id)
                    self._store_vertex(pipe, to_id)
                    self._store_edge(pipe, from_id, to_id, self._encode(label), weight)
                pipe.execute()

    def __str__(self):
        return "<%s %r>" % (self.__class__.__name__, self.name)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self._r, self.name)


class Vertex(object):
    def __init__(self, g, name):
        self._g = g
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def extant(self):
        return self._g.has_vertex(self)

    def edges_out(self):
        return self._g.edges_from(self)

    def edges_in(self):
        return self._g.edges_to(self)

    @property
    def in_e(self):
        return EdgeSet(self.edges_in())

    @property
    def out_e(self):
        return EdgeSet(self.edges_out())

    @property
    def in_v(self):
        return VertexSet([self]).in_v

    @property
    def out_v(self):
        return VertexSet([self]).out_v

    def __getitem__(self, key):
        return self._g.get_vertex_property(self, key)

    def __setitem__(self, key, value):
        return self._g.set_vertex_property(self, key, value)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return "<%s %r>" % (self.__class__.__name__, self.name)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self._g, self.name)


class Edge(object):
    def __init__(self, g, fromv, tov, label=None, weight=1.0):
        self._g = g
        self._fromv = fromv
        self._tov = tov
        self._label = label
        self._weight = weight

    @property
    def fromv(self):
        return self._fromv

    @property
    def tov(self):
        return self._tov

    @property
    def label(self):
        return self._label

    @property
    def weight(self):
        return self._weight

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

    def __repr__(self):
        return "%s(%r, %r, %r, %r)" % (self.__class__.__name__, self._g, self.fromv, self.tov, self.label)

    def __str__(self):
        if self.label is None:
            arrow = "%s->%s" % (self.fromv.name, self.tov.name)
        else:
            arrow = "%s-%s->%s" % (self.fromv.name, self.label, self.tov.name)
        return "<%s %s>" % (self.__class__.__name__, arrow)


class ComponentSet(set):
    def filter(self, fn):
        return self.__class__(filter(fn, self))

    def map(self, fn):
        return self.__class__(map(fn, self))

    def reduce(self, fn):
        return self.__class__(reduce(fn, self))

    def __str__(self):
        return "<" + self.__class__.__name__ + " " + ", ".join(map(str, self)) + ">"


class VertexSet(ComponentSet):
    @property
    def in_e(self):
        return EdgeSet(chain(*[v.edges_in() for v in self]))

    @property
    def out_e(self):
        return EdgeSet(chain(*[v.edges_out() for v in self]))

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
