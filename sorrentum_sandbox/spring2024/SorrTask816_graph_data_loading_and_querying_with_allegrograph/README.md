# Graph Data Loading And Querying With AllegroGraph

## Project Description:

Install and configure AllegroGraph, and create a Python script to load graph data into the database. Define a simple graph schema and insert sample data representing entities and relationships. Use AllegroGraph's SPARQL endpoint to execute basic graph queries and retrieve information from the database. Explore and present an innovative project.

## Implementation

The implementation of this project will store metadata from recent academic articles published by *Crossref* using AllegroGraph, creating a graph representation connecting nodes of papers, authors, and publishers. Then the SPARQL endpoint will be used for some basic analysis.

https://www.crossref.org/blog/2023-public-data-file-now-available-with-new-and-improved-retrieval-options/

### Progress report

To date the following steps have been completed:
- A preliminary dataset has been compiled by narrowing the full Crossref database (185GB) down to a small number of variables and a narrow band of published dates, however the data will need to be narrowed further prior to implementation simply for size reasons (still about 6GB).
- A docker file containing the building of necessary containers has been written. These containers include AllegroGraph, Python, and Jupyter Notebook.
