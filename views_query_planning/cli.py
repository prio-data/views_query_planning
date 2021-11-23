
import io
import pickle
from contextlib import closing
import click
from matplotlib import pyplot as plt
import networkx as nx
import pydot
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session 
from views_query_planning import query_planning, compose_join, query_with_ops

@click.group(name = "vqp")
def vqp():
    """
    ViEWS query planning
    ====================

    This CLI exposes core functionality from the ViEWS query planner, making it
    possible to inspect and understand the query planning operations taking
    place with the library.

    Examples:

        > Generate a join network and output it to a plot
        vqp with postgresql://user@database/dbname join-network - | vqp dot-to-plot - out.png

    """
    pass

@vqp.group(name = "with")
@click.argument("connection-string")
@click.option("--schema", default = "public")
@click.pass_context
def with_network(ctx: click.Context, connection_string: str, schema: str):
    ctx.ensure_object(dict)
    ctx.obj["engine"] = create_engine(connection_string)
    ctx.obj["session"] = Session(bind = ctx.obj["engine"])

    metadata = MetaData(bind = ctx.obj["engine"], schema = schema)
    metadata.reflect()

    ctx.obj["metadata"] = metadata
    ctx.obj["network"] = query_planning.join_network(ctx.obj["metadata"].tables)
    ctx.obj["schema"] = schema

@with_network.command(name = "join-network")
@click.pass_context
@click.argument("file", type = click.File("w"))
def join_network(ctx: click.Context, file: io.BufferedWriter):
    file.write(nx.nx_pydot.to_pydot(ctx.obj["network"]).to_string())

@with_network.command(name = "query")
@click.pass_context
@click.argument("level-of-analysis", type = str)
@click.argument("table", type = str)
@click.argument("column-name", type = str)
@click.argument("file", type = click.File("w"))
@click.option("-a","--aggregation-function", type = str, default = "avg")
def query(
        ctx: click.Context, 
        level_of_analysis: str,
        table: str,
        column_name: str,
        aggregation_function: str,
        file: io.BufferedWriter):
    
    index_columns = ["id"]

    query = query_with_ops(
            ctx.obj["session"].query(),
            compose_join,
            ctx.obj["network"],
            level_of_analysis,
            table,
            column_name,
            index_columns,
            aggregation_function
            )

    click.echo(str(query))

@with_network.command(name = "dump")
@click.argument("file", type = click.File("wb"))
@click.pass_context
def dump_db_reflection(ctx: click.Context, file: io.BufferedWriter):
    pickle.dump(ctx.obj["metadata"], file)

@vqp.command(name = "dot-to-plot")
@click.argument("dotfile", type = click.File("r"))
@click.argument("outfile", type = click.File("wb"))
def dot_to_plot(dotfile = io.BufferedReader, outfile = io.BufferedWriter):
    data = dotfile.read()
    click.echo(data)
    pydot_graph = pydot.graph_from_dot_data(data)
    if pydot_graph is None:
        click.echo("failed to read dot graph from data:")
        click.echo(data)
        return 1
    else:
        pydot_graph, *_ = pydot_graph

    graph = nx.nx_pydot.from_pydot(pydot_graph)
    fig,ax = plt.subplots(figsize = (20,20))
    nx.draw(graph, with_labels = True, ax = ax)
    fig.savefig(outfile)



