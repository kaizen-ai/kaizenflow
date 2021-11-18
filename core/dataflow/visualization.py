"""
Helper functions to visualize a graph in a notebook or save the plot to file.

Import as:

import core.dataflow.visualization as cdtfvisu
"""

import IPython
import networkx as networ
import pygraphviz

import core.dataflow as dtf
import helpers.dbg as hdbg
import helpers.io_ as hio


def draw(dag: dtf.DAG) -> IPython.core.display.Image:
    """
    Render DAG in a notebook.
    """
    agraph = _extract_agraph_from_dag(dag)
    image = IPython.display.Image(agraph.draw(format="png", prog="dot"))
    return image


def draw_to_file(dag: dtf.DAG, file_name: str = "graph.png") -> str:
    """
    Save DAG rendering to a file.
    """
    agraph = _extract_agraph_from_dag(dag)
    # Save to file.
    hio.create_enclosing_dir(file_name)
    agraph.draw(file_name, prog="dot")
    return file_name


def _extract_agraph_from_dag(dag: dtf.DAG) -> pygraphviz.agraph.AGraph:
    """
    Extract a pygraphviz `agraph` from a DAG.
    """
    # Extract networkx DAG.
    hdbg.dassert_isinstance(dag, dtf.DAG)
    graph = dag.dag
    hdbg.dassert_isinstance(graph, networ.Graph)
    # Convert the DAG into a pygraphviz graph.
    agraph = networ.nx_agraph.to_agraph(graph)
    return agraph
