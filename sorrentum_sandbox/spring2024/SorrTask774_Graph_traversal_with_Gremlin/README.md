# Graph Traversal with Gremlin

This is where I put all my notes about my Spring 2024 DATA605 Final Project, Graph Traversal using Gremlin.

**Project Description:** Begin by installing Apache TinkerPop's Gremlin Python library (gremlinpython). Next, create a sample graph structure using Gremlin's graph traversal API. Define vertices and edges, and establish connections between them to form a graph. Then, use Gremlin queries to traverse the graph, retrieving vertices, edges, and their properties. Experiment with various traversal strategies and filtering techniques to explore different aspects of the graph.

Does anyone really know how Docker works? I *believe* the answer may be no.

These images from DockerHub might be needed. Figure it out!
docker pull tinkerpop/gremlin-server
docker pull tinkerpop/gremlin-console

with the image that we build, we need to use:
docker build -t rick-gremlin .

and then run our docker container:
docker run -d -p 8182:8182 rick-gremlin
