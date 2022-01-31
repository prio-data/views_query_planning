
# Views Query Planning

This package exposes a class `views_query_planning.QueryComposer` that makes it
possible to generate queries against a relational database using a network
representation of the database. Such networks can be inferred using the
`views_query_planning.join_network` function that takes a dictionary of
`sqlalchemy` tables and returns a `networkx.DiGraph` that can be passed to the
composer.

For an example service that uses the QueryComposer class to expose columns in a
relational DB RESTfully, see
[base_data_retriever](https://github.com/prio-data/base_data_retriever).
