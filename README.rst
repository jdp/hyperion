========
Hyperion
========

Hyperion is a graph database built in Python using Redis as the backing data store. It depends on the `redis-py`_ library for interfacing with Redis.

.. _`redis-py`: http://github.com/andymccurdy/redis-py

Basic Graph Operations
======================

Hyperion can be used directly in your Python applications. The API intends to be small, focused, and flexible to make graph building and traversal straightforward to perform.

Loading a Graph
---------------

Graphs are stored in Redis, and as little metadata is stored as possible. Hyperion makes no distinction between a new and an extant graph. Here is how to instantiate a ``testing`` graph to work with::

    >>> from redis import Redis
    >>> from hyperion import Graph
    >>> g = Graph(Redis(), "testing")

From here, vertices and nodes can be added to the graph, and traversals and lookups can be performed.

Adding Vertices
---------------

Vertices represent single data points within the graph. Each should have a unique identifier, and they can have arbitrary sets of name-value properties. To create and return a new vertex, use the ``add_vertex`` method::

    >>> bulbasaur = g.add_vertex("Bulbasaur")

If you don't provide an id, Hyperion will use an internal counter to assign it a numeric id::

    >>> v1 = g.add_vertex()
    >>> v1.name
    "2"

The ``add_vertex`` method returns a ``Vertex`` object. It has properties that will allow you to find its incoming and outgoing edges and vertices, as well as read and write name-value properties.

Looking up Vertices
-------------------

Vertices can be quickly looked up by their id after they have been added. Use the ``get_vertex`` method to retrieve a vertex::

    >>> g.add_vertex("Squirtle")
    >>> v = g.get_vertex("Squirtle")

As a convenience, the ``v`` can be used as an alias for ``get_vertex``.

Adding Edges
------------

Relationships between vertices are created by adding edges. Edges in Hyperion are always directed, and can be given a label and a weight. Multiple edges in the same direction can be created between two vertices, provided they have different labels. Adding an edge with an existing label will replace an old edge. The first argument to the ``add_edge`` method is the source vertex, and the second is the destination vertex::

    >>> ivysaur = g.add_vertex("Ivysaur")
    >>> e = g.add_edge(g.v("bulbasaur"), ivysaur, label="evolves-into")
    >>> print e
    <Edge Bulbasaur-evolves-into->Ivsysaur>

The ``add_edge`` method returns an ``Edge`` object, which contains properties detailing the the vertices it connects and the relationship between them.

Traversing the Graph
--------------------

A vertex is aware of all its inward and outward edges. This is what is known as *index-free adjacency*: an entity in a graph is aware of its immediate relationships without having to reference a global index structure. Given a ``Vertex`` object, you can find its outward edges like so::

    >>> bulbasaur = g.v("Bulbasaur")
    >>> print bulbasaur.out_e
    <EdgeSet <Edge bulbasaur-evolves-into->Ivysaur>>

The ``out_e`` method returns an ``EdgeSet`` object containing the set of outward edges from a vertex. To get the destination vertices from an ``EdgeSet`` object, use the ``in_v`` property::

    >>> print bulbasaur.out_e.in_v
    <VertexSet <Vertex Ivysaur>>

Because it is such a common operation, ``Vertex`` and ``VertexSet`` objects have an ``out_v`` property which is equivalent to ``edge.out_e.in_v``. The complement operations also exist, and given a vertex you can also find its incoming edges::

    >>> ivysaur = g.v("Ivysaur")
    >>> print ivysaur.in_e
    <Edge Bulbsaur-evolves-into->Ivysaur>
    >>> print ivysaur.in_e.out_v
    <Vertex Bulbasaur>
    >>> print ivysaur.in_v
    <Vertex Bulbasaur>

As a rule, ``Vertex``, ``Edge``, ``VertexSet``, and ``EdgeSet`` objects can be traversed with the ``in_e``, ``out_e``, ``in_v``, and ``out_v`` properties, which return ``EdgeSet`` and ``VertexSet`` objects.

======== ===========
Property Description
======== ===========
in_e     A ``EdgeSet`` containing all incoming edges to the vertex, or all incoming edges to all vertices in the case of ``VertexSet``
out_e    A ``EdgeSet`` containing all outgoing edges from the vertex, or all outgoing edges from all vertices in the case of ``VertexSet``
in_v     A ``VertexSet`` containing all destination vertices from all edges associated with the object
out_v    A ``VertexSet`` containing all source vertices from all edges associated with the object
======== ===========
