import logging
from typing import Dict
import enum
from itertools import chain

import networkx as nx
from networkx.exception import NetworkXNoPath
from networkx.algorithms.shortest_paths import shortest_path

import sqlalchemy as sa

from . import exceptions, definitions

logger = logging.getLogger(__name__)

def class_partial(method,*args,**kwargs):
    def undotted(instance):
        return getattr(instance,method)(*args,**kwargs)
    return undotted

class Direction(enum.Enum):
    forwards = 1
    backwards = 2
    equal = 3

def join_network(tables:Dict[str,sa.Table])->nx.DiGraph:
    """
    Creates a directed graph of the FK->Referent relationships present in a
    list of tables. This graph can then be traversed to figure out how to
    perform a join when retrieving data from this collection of tables.
    """
    digraph = nx.DiGraph()
    def get_fk_tables(fk):
        return [tbl for tbl in tables.values() if fk.references(tbl)]

    for table in tables.values():
        for fk in table.foreign_keys:
            try:
                ref_table = get_fk_tables(fk)[-1]
            except IndexError:
                logger.debug("No table found for fk: %s",str(fk))
                continue

            digraph.add_edge(
                    table,
                    ref_table,
                    reference=fk.parent,
                    referent=fk.column
                )

            if {fk.parent} == set(table.primary_key):
                digraph.add_edge(
                        ref_table,
                        table,
                        reference=fk.column,
                        referent=fk.parent,
                    )

    logger.debug("Database is a digraph with %s nodes",len(digraph.nodes))
    return digraph

def compose_join(network,loa_name,table_name,column_name,loa_index_columns,agg_fn="avg"):
    """
    compose_join

    Creates a list of operations that can be applied to a Query object, making
    it retrieve data at a certain LOA

    :param network: A networkx graph representing a database
    :param loa_name: The name of the LOA to retrieve data at
    :param table_name: The name of the table from which to retrieve the data 
    :param column_name: The name of the column to retrieve 
    :param agg_fn: If the retrieval op. needs to aggregate data, if will do so with this function
    :returns: An iterator containing functions to be applied to a Query object 
    :raises QueryError: If table_name.column_name does not exist in the DB 
    :raises ConfigError: If the requested loa is not configured  
    """

    lookup = {tbl.name:tbl for tbl in network}
    get_col_ref = lambda tbl,name: lookup[tbl].c[name]#.label(tbl+"_"+name)

    loa_table = lookup[loa_name]

    try:
        column_ref = get_col_ref(table_name,column_name)
    except KeyError as ke:
        raise exceptions.QueryError(f"{table_name}.{column_name} not found") from ke

    index_columns_ref = []
    for idx_table_name,idx_column_name in loa_index_columns:
        try:
            index_columns_ref.append(get_col_ref(idx_table_name,idx_column_name))
        except KeyError as ke:
            raise exceptions.ConfigError(f"Index column for {loa_table}"
                    f" {idx_table_name}.{idx_column_name} not found")

    all_columns = list(set(index_columns_ref+[column_ref]))
    names = [c.table.name + "_" + c.name for c in all_columns]
    aggregates = False


    # Compute joins (+ agg)
    join = []
    joined = set()
    for idx,c in enumerate(all_columns):
        try:
            path = shortest_path(network, loa_table, c.table)
        except NetworkXNoPath:
            try:
                assert agg_fn in definitions.AGGREGATION_FUNCTIONS
            except AssertionError:
                raise exceptions.AggregationNameError(
                        "Aggregation function {agg_fn} "
                        "is not available. "
                        "Available functions are: "
                        f"{', '.join(definitions.AGGREGATION_FUNCTIONS)}."
                        )

            aggregates = True
            path = shortest_path(network,c.table, loa_table)
            path.reverse()
            all_columns[idx] = getattr(sa.func,agg_fn)(c)
            names[idx] = names[idx]+"_"+agg_fn

        prev = loa_table
        for table in path[1:]:
            if table not in joined:
                try:
                    con = network[prev][table]
                    join.append(class_partial("join",table,con["reference"]==con["referent"]))
                except KeyError:
                    con = network[table][prev]
                    join.append(class_partial("join",table,con["referent"]==con["reference"]))
                joined = joined.union({table})
            else:
                pass
            prev = table

    # Selects
    all_columns = [col.label(name) for col,name in zip(all_columns,names)]
    select = [class_partial("add_columns",col) for col in all_columns]

    # Aggregation
    if aggregates:
        group_by = [class_partial("group_by",c) for c in index_columns_ref]
    else:
        group_by = []

    logger.debug("%s selects",len(select))
    logger.debug("%s joins",len(join))
    logger.debug("%s groupbys",len(group_by))

    return chain(select,[class_partial("select_from",loa_table)],join,group_by)

def query_with_ops(query,op_composer,*args,**kwargs):
    for op in op_composer(*args,**kwargs):
        query = op(query)
    return query
