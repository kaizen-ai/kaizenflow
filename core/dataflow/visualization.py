import networkx as nx
from IPython.display import Image


# TODO(Paul): Add type hints.
def draw(graph):
    """
    Render NetworkX graphs.
    """
    agraph = nx.nx_agraph.to_agraph(graph)
    return Image(agraph.draw(format="jpg", prog="dot"))
