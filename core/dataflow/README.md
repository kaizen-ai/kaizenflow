# Conventions

- In the following we refer to instantiated objects instead of classes


# Class diagram

- You can create the class diagram with
  ```bash
  > dev_scripts/create_class_diagram.sh amp/core/dataflow
  ```

# Code organization

- A dataflow graph represents a computation performed through a graph of nodes
  - It is implemented by a `DAG` object
- Each `DAG` object is implemented in terms of a graph of connected `Node`
  objects

## `Node` object

- Each Node object has a unique identifier, names of inputs and outputs
- `Node` store results of different computations (one per `method`) for each of
  their outputs

## `DAG` object

- `DAG`s are built using `DagBuilder` objects, which are factory classes

