import networkx as networ

# TODO(Paul): Update import according to rule.
from IPython.display import Image


# TODO(Paul): Add type hint for return value.
def draw(graph: networ.Graph):
    """
    Render NetworkX graph.
    """
    agraph = networ.nx_agraph.to_agraph(graph)
    return Image(agraph.draw(format="png", prog="dot"))
