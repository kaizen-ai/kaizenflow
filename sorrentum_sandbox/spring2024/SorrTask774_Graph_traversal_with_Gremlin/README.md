# Graph Traversal with Gremlin

This is where I put all my notes about my Spring 2024 DATA605 Final Project, Graph Traversal using Gremlin.

**Project Description:** Begin by installing Apache TinkerPop's Gremlin Python library (gremlinpython). Next, create a sample graph structure using Gremlin's graph traversal API. Define vertices and edges, and establish connections between them to form a graph. Then, use Gremlin queries to traverse the graph, retrieving vertices, edges, and their properties. Experiment with various traversal strategies and filtering techniques to explore different aspects of the graph.

These images from DockerHub might be needed. Figure it out!
docker pull tinkerpop/gremlin-server
docker pull tinkerpop/gremlin-console

with the image that we build, we need to use:
docker build -t rick-gremlin .

and then run our docker container:
docker run -d -p 8182:8182 rick-gremlin

Actual steps to run:
docker-compose build
docker-compose up

docker system prune --force --all --volumes

Now the gremlin-python-1 is running before the gremlin-server-1, but I can click run again, and it will work properly. I need to check if there is a way to have the gremlin-server-1 run first, because my docker-compose does say that gremlin-python depends_on gremlin-server, but it doesn't appear to be working as I expected it to.

I am running into an issue where I am able to create a gremlin server inside the docker, but when I run the container, the python file parts are not working. I believe it has something to do with the fact I'm using the gremlin server instead of the console? But I would love to be able to run the code and get back the proper results instead of just:

2024-04-29 19:08:56 Vertices: [['V']]
2024-04-29 19:08:56 Edges: [['E']]
2024-04-29 19:08:56 People known by Alpha: [['V'], ['has', 'name', 'Alpha'], ['out', 'knows']]
2024-04-29 19:08:56 People known by Beta: [['V'], ['has', 'name', 'Beta'], ['out', 'knows']]
2024-04-29 19:08:56 People: [['V'], ['hasLabel', 'person']]

I have created a list of vertices that I can call individually using print, but I can not get the gremlinpython functions to work.