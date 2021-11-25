"""
querying
========

This module contains code for computing SQL queries. Functionality is primarily
exposed via the QueryComposer class.

"""
import logging
from typing import Deque, Tuple, TypeVar
from collections import deque
from sqlalchemy import Table, Column
from sqlalchemy import sql
from sqlalchemy.sql.selectable import Select, Join
from toolz.functoolz import curry
from networkx import NetworkXNoPath, DiGraph
from networkx.algorithms.shortest_paths import shortest_path
from pymonad.either import Left, Right, Either
from pymonad.maybe import Maybe, Just, Nothing

logger = logging.getLogger(__name__)

T = TypeVar("T")

class QueryComposer():

    def __init__(self, network: DiGraph, loa_name: str, time_index: str, unit_index: str):
        """
        QueryComposer
        =============

        arguments:
            network (networkx.DiGraph): A network as produced by views_query_planning.join_network
            loa_name (str):             The name of the table to use as the level of analysis.
            time_index (str):           The name of the column in loa_name to use as the time-index
            unit_index (str):           The name of the column in loa_name to use as the unit-index

        The query composer lets you compute SQL queries for retrieving data in
        a normalized database, through joining and aggregating.

        This is exposed through the .expression method, which lets you get an
        expression selecting a column from a table, at the level of analysis
        that the composer was instantiated with.

        """
        self.network = network
        self._tables = {tbl.name: tbl for tbl in self.network}
        self.loa_name = loa_name
        self._time_index = time_index
        self._unit_index = unit_index

    def joins(self, join_path: Deque[Table])-> Join:
        """
        joins
        =====

        arguments:
            join_path (Deque[Table]): A deque of potentially joinable tables

        returns:
            Join: A join expression that can be used for futher query composition.

        """
        a = join_path.popleft()
        expression = a
        for b in join_path:
            try:
                edge = self.network[a][b]
                condition = (edge["reference"] == edge["referent"])
            except KeyError:
                edge = self.network[b][a]
                condition = (edge["referent"] == edge["reference"])

            expression = sql.join(expression, b, condition)
            a = b
        return expression


    def expression(self, table: str, column: str, aggregation_function: str = "sum")-> Either[str, str]:
        """
        expression
        ==========

        arguments:
            table (str):                The name of a table to join to
            column (str):               The name of a column to select from the table.
            aggregation_function (str): The name of a function to use to aggregate if applicable.

        returns:
            Either[str, str]

        Computes an SQL expression that joins and selects a desired
        table.column to the self.level_of_analysis, including the LOAs index
        columns.

        The expression will aggregate, grouping on the LOA table and applying
        the aggregation function, if necessary.

        Requires there to be _a_ valid join-path between table and LOA. This
        means that to join table_a and table_b there _either_ needs to be a
        relationship like this:

        (one-to-many)
        ┌───────┐     ┌───────┐     ┌───────┐
        │table_a│─fk─►│table_b│─fk─►│table_c│
        └───────┘     └───────┘     └───────┘

        Or a relationship like this:

        (aggregates, many-to-one)
        ┌───────┐     ┌───────┐     ┌───────┐
        │table_a│◄─fk─│table_b│◄─fk─│table_c│
        └───────┘     └───────┘     └───────┘

        Across any number of intermediate tables.

        """

        to_select = (self._column(table,column)
                .maybe(Left(f"Column {table}.{column} doesn't exist"), Right)
                .then(lambda c: c.label(c.name)))

        index_columns = (self.index_columns
                .maybe(Left("Index columns not found."), Right)
                .then(lambda columns: (c.label(c.name) for c in columns)))

        tables = [self.loa_table, self._table(table)]

        forwards_path = Maybe.apply(curry(path, True)).to_arguments(Just(self.network), *tables).join()

        join_path, aggregates = forwards_path.maybe(
                (Maybe.apply(curry(path, False)).to_arguments(Just(self.network), *tables).join(), True),
                lambda x: (Just(x), False))

        joins = (join_path.maybe(Left("Failed to find path"), Right)
            .then(self.joins))

        select_from = (Either.apply(curry(lambda idx_col, joins: sql.select(*idx_col).select_from(joins)))
            .to_arguments(index_columns, joins))

        if aggregates:
            selection_function = curry(aggregate, aggregation_function, self.loa_table.value)
        else:
            selection_function = lambda to_select, select_from: Right(select_from.add_columns(to_select))

        return Either.apply(curry(selection_function)).to_arguments(to_select, select_from).join().then(str)

    def _table(self, name)-> Maybe[Table]:
        """
        _table
        ======

        arguments:
            name (str): name of the table to get

        returns:
            Maybe[Table]: table, if present.

        """
        try:
            return Just(self._tables[name])
        except KeyError:
            return Nothing

    def _column(self, table, column)-> Maybe[Column]:
        """
        _column
        ======

        arguments:
            table (str):  name of the table to get
            column (str): name of the column to get

        returns:
            Maybe[Column]: column, if table and column are present.

        """
        def get_key(lookup, key):
            try:
                return Just(lookup[key])
            except KeyError:
                return Nothing

        return self._table(table).then(lambda tbl: get_key(tbl.c, column))

    @property
    def loa_table(self) -> Maybe[Table]:
        return self._table(self.loa_name)

    @property
    def index_columns(self) -> Maybe[Tuple[Column, Column]]:
        time, unit = (self._column(self.loa_name, col) for col in (self._time_index, self._unit_index))
        return Maybe.apply(curry(lambda t,u: Just((t,u)))).to_arguments(time, unit).join()

def path(forwards: bool, network: DiGraph, a: T, b: T)-> Maybe[Deque[T]]:
    """
    path
    ====

    arguments:
        forwards (bool):               Find path with or against direction in network.
        network (networkx.Digraph[T]): A network of nodes of type T to find a path in
        a (T):                         A node in the network
        b (T):                         A node in the network

    returns:
        Maybe[Deque[T]]: A deque of steps from a to b

    """

    if forwards:
        x,y = a,b
        post = lambda x:x
    else:
        x,y = b,a
        post = reversed

    try:
        return Just(deque(post(shortest_path(network, x, y))))
    except NetworkXNoPath:
        logger.debug(f"No path between {a} and {b} in {network}! ({'forwards' if forwards else 'backwards'})")
        return Nothing

def aggregate(aggregation_function_name: str, aggregate_to: Table, column: Column, select: Select)-> Select:
    """
    aggregate
    =========

    arguments:
        aggregation_function_name (str): Name of agg. fn
        aggregate_to (Table):            Table to aggregate to
        column (Column):                 Column to aggregate
        select (Select):                 Select statement to mutate

    returns:
        Select: Select statement with added select of aggregated column.

    Adds selection of an aggregated column to a select statement.

    """

    _allowed_aggregation_functions = [
            "sum",
            "max",
            "min",
            "avg",
        ]
    if aggregation_function_name in _allowed_aggregation_functions:
        aggregation_function = getattr(sql.func, aggregation_function_name)
        return Right(select
                .add_columns(aggregation_function(column).label(column.name + "_" + aggregation_function_name))
                .group_by(*set(aggregate_to.primary_key)))
    else:
        return Left(f"Aggregation function {aggregation_function_name} does not exist.")
