# Graph Data Loading And Querying With Neo4j*

## Project Description:
Install and configure Neo4j, and create a Python script to load graph data into the database. Define a simple graph schema and insert sample data representing entities and relationships. Use Neo4j's endpoint to execute basic graph queries and retrieve information from the database. Explore and present an innovative project.

***Please note that this project was changed to use Neo4j rather than Allegrograph as originally intended with Professor Permission. The original project description can be found at the bottom of this page.***

## Implementation

The implementation of this project will store graph data from *The Marvel Comics character collaboration graph* originally constructed by Cesc Rosselló, Ricardo Alberich, and Joe Miro from the University of the Balearic Islands. The data pulls from Marvel's superhero comic books, linking hero's to the stories they appear in. While this is obviosuly a non-serious use, it is still demonstraive of the capabilties of a dockerized environment for doing data analysis on graph data.

This data is stored in the sh_nw (Superhero Network) subdirectory in `nodes.csv` and `edges.csv`

https://bioinfo.uib.es/~joemiro/marvel.html

## Progress report

To date the following steps have been completed:
- Dockerized environment containing Neo4j and Jupyter notebook as been successfully created
- Python code to upload data from nodes and edges CSVs is running correctly
- Preliminary Data Analysis

To Do:
- Do some further data analysis as a demonstration (some visualizations)
- Complete project writeup and record instructional video


## Original Project Description
Install and configure AllegroGraph, and create a Python script to load graph data into the database. Define a simple graph schema and insert sample data representing entities and relationships. Use AllegroGraph's SPARQL endpoint to execute basic graph queries and retrieve information from the database. Explore and present an innovative project.