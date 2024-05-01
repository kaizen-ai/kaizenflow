from gremlin_python import statics
from gremlin_python.structure.graph import Graph
import nest_asyncio 
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __

nest_asyncio.apply()  # to support gremlin event loop in jupyter lab

# set the graph traversal from the local machine:
connection = DriverRemoteConnection("ws://gremlin-server:8182/gremlin", "g")  # Connect it to your local server
g = traversal().withRemote(connection)

# Add vertices
v1 = g.addV('person').property('name', 'Alpha').property('age', 22).next()
v2 = g.addV('person').property('name', 'Beta').property('age', 34).next()
v3 = g.addV('person').property('name', 'Gamma').property('age', 28).next()
v4 = g.addV('person').property('name', 'Delta').property('age', 40).next()
v5 = g.addV('person').property('name', 'Epsilon').property('age', 45).next()
v6 = g.addV('dog').property('name', 'Zeta').property('age', 5).next()
v7 = g.addV('dog').property('name', 'Eta').property('age', 2).next()
v8 = g.addV('dog').property('name', 'Theta').property('age', 9).next()

# Add edges with weights
e1 = g.V(v1).addE('knows').to(__.V(v2)).property('weight', 5).next()
e2 = g.V(v1).addE('knows').to(__.V(v3)).property('weight', 3).next()
e3 = g.V(v1).addE('knows').to(__.V(v5)).property('weight', 2).next()
e4 = g.V(v2).addE('knows').to(__.V(v3)).property('weight', 7).next()
e5 = g.V(v4).addE('knows').to(__.V(v5)).property('weight', 4).next()
e6 = g.V(v4).addE('knows').to(__.V(v6)).property('weight', 6).next()
e7 = g.V(v5).addE('knows').to(__.V(v1)).property('weight', 9).next()
e8 = g.V(v5).addE('knows').to(__.V(v1)).property('weight', 10).next()
e9 = g.V(v5).addE('knows').to(__.V(v2)).property('weight', 8).next()
e10 = g.V(v5).addE('knows').to(__.V(v3)).property('weight', 6).next()
e11 = g.V(v5).addE('knows').to(__.V(v4)).property('weight', 3).next()
e12 = g.V(v5).addE('knows').to(__.V(v6)).property('weight', 5).next()

# Traverse the graph to get all vertices
vertices = g.V().toList()
print("Vertices:", vertices)

# Traverse the graph to get the names of all vertices
names = g.V().values('name').toList()
print("Vertex names:")
# Print the names of all vertices
for name in names:
    print(name)

# Traverse the graph to get all edges
edges = g.E().toList()
print("Edges:", edges)

# Example traversal: Find all people known by Alpha
result = g.V().has('name', 'Alpha').out('knows').toList()
print("People known by Alpha:", result)

# Example traversal: Find all people known by Beta
result = g.V().has('name', 'Beta').out('knows').toList()
print("People known by Beta:", result)

# Example traversal: Find all people known by Gamma
result = g.V().has('name', 'Gamma').out('knows').toList()
print("People known by Gamma:", result)

# Example traversal: Find all people known by Delta
result = g.V().has('name', 'Delta').out('knows').toList()
print("People known by Delta:", result)

# Example traversal: Find all people known by Epsilon
result = g.V().has('name', 'Epsilon').out('knows').toList()
print("People known by Epsilon:", result)

# Example traversal: Find all vertices of type 'person'
people = g.V().hasLabel('person').toList()
print("List of People:", people)

# Traverse the graph to get all vertices with age greater than 30
# age_gt_30 = g.V().has('age', __.gt(30)).toList()
# print("People above the age of 30:", age_gt_30)

# Traverse the graph to get all edges with weight greater than 5 (Figure out how traversal T works)
# high_weight_edges = g.E().has('weight', T.gt(5))
# print("Edges with weight > 5:", high_weight_edges)

# Close the connection
connection.close()

print('**TEST** This is working.')
