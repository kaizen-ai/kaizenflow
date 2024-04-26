from gremlin_python.structure.graph import Graph
from gremlin_python.process.traversal import T

graph = Graph()
g = graph.traversal()

# Add vertices (adding .next() is in all examples, but the codes does not work with it)
v1 = g.addV('person').property('name', 'Alpha')
v2 = g.addV('person').property('name', 'Beta')
v3 = g.addV('person').property('name', 'Delta')

# Print the list of Vertices to test
print('Vertices: ', v1, '&', v2, '&', v3)
'''# Add edges (assuming no weight property for now)
e1 = g.V(v1).addE('knows').to(v2)
e2 = g.V(v1).addE('knows').to(v3)

# Traverse the graph to get all vertices
vertices = g.V().valueMap(True).toList()
print("Vertices:", vertices)

# Traverse the graph to get all edges
edges = g.E().valueMap().toList()
print("Edges:", edges)

# Example traversal: Find all people known by Alice
result = g.V().has('name', 'Alice').out('knows').valueMap().toList()
print("People known by Alice:", result)

# Example traversal: Find all vertices of type 'person'
people = g.V().hasLabel('person').valueMap().toList()
print("People:", people)

# Example traversal: Find all edges with weight greater than 5
# Adjust this according to your actual data model
# high_weight_edges = g.E().has('weight', T.gt(5)).valueMap().toList()
# print("Edges with weight > 5:", high_weight_edges)'''

print('Oh Hell yes! This is working.')
