<!--ts-->
   * [Brief introduction to c4](#brief-introduction-to-c4)
      * [Different levels of detail](#different-levels-of-detail)
         * [System context (Level 1)](#system-context-level-1)
         * [Container (Level 2)](#container-level-2)
         * [Component (level 3)](#component-level-3)
         * [Code (level 4)](#code-level-4)
   * [Our conventions](#our-conventions)
      * [Use classes!](#use-classes)
      * [Mapping C4 and code structure](#mapping-c4-and-code-structure)
      * [Generating class diagram](#generating-class-diagram)
   * [Brief introduction to PlantUML](#brief-introduction-to-plantuml)



<!--te-->
- We describe the high level architecture of our system using the c4 methodology
  and PlantUML

# Brief introduction to c4

- A detailed description of c4 is https://c4model.com

- C4 stands for "context, container, component, code" (the 4 Cs)

- C4 model helps developers describe software architecture
  - It maps code at various level of detail
  - It is useful for both software architects and developers

## Different levels of detail

- The 4 levels of detail are:
  1. System context system
     - How the system fits in the world
  2. Container
     - High-level technical blocks
  3. Component
     - Show the components inside a container (i.e., a high-level block)
  4. Code
     - Show how components are implemented
     - Represented in terms of UML class diagrams

### System context (Level 1)

- A system context describes something that delivers value to its users
  - Typically a system is owned by a single software development team

- System context diagram shows the big picture of how the software system
  interacts with users and other systems in the IT environment

- The focus is not on:
  - Technologies
  - Protocols
  - Low-level details

- Audience:
  - Both technical and non-technical people
  - Both inside and outside the software development team

- A system system is made up of one or more containers

### Container (Level 2)

- A container represents an application
  - E.g.,
    - Server-side web application (e.g., Tomcat running Java EE web application,
      Ruby on Rails application)
    - Client-side web application (e.g., JavaScript running in a web browser,
      e.g., using Angular)
    - Client-side desktop application (e.g., an macOS application)
    - Mobile app (e.g., an iOS or Android app)
    - Server-side console application
    - Server-less function (e.g., AWS Lambda)
    - Database (e.g., MySQL, MongoDB)
    - Content-store (e.g., AWS S3)
    - File-system (e.g., a local filesystem)
    - Shell script

- A container runs some code and store some data
  - Typically each container runs in its own process space
  - Containers communicate through inter-process communication

- A container diagram shows the high-level shape of the software architecture
  and how responsibilities are distributed across it

- A container is the sum of components
  - All components inside a container execute together
  - Components can't be deployed as separate units

- Audience:
  - Technical people
  - Inside and outside of the software development team

### Component (level 3)

- Component is a group of related functionality encapsulated behind a
  well-defined interface
  - E.g., collection of classes behind an interface

- A component diagram decomposes each container to identify major structural
  building blocks and interactions

- Audience
  - Software architects and developers

### Code (level 4)

- Code is the implementation of the software system
  - Each component can represented in terms of UML class diagrams, entity
    relationship diagrams, etc.
  - This diagram should be generated automatically from code

- Audience
  - Software architects and developers

# Our conventions

## Use classes!

- In order to be able to describe the system with C4 it is best to use classes
  to separate responsibilities and package code

- Using classes has the following advantages:
  - Organizes the code in cohesive parts
  - Makes clear what is a public interface vs a private interface
    (e.g., helpers)
  - Highlights responsibility (e.g., builder, annotation, processor, analyzer)
  - Simplifies the interface of functions by sharing state in the object

- Note that classes still allow our favorite functional style of programming
  - E.g., pandas is implemented with classes and it allows functional style
  - The difference is going from:
    ```python
    f(ton of arguments)
    ```
    to
    ```python
    o(some argument).f(other arguments)
    ```

## Mapping C4 and code structure

- To simplify, we map the 4 levels of C4 in the code structure

- Level 1
  - System context = big picture of how the system interacts with users and
    other systems
  - A system is typically mapped onto a code repository
  - E.g.,
    - `//p1` is a system providing data and analytics for commodity
    - `//pre-commit` is a system implementing a code linter

- Level 2:
  - Container = high-level software architecture and how responsibilities are
    split in the system
  - A container corresponds to the first level of directories in a repo
  - E.g., in `//p1`
    - `automl`: application for automatic machine learning for commodity
      analysis
    - `edgar`: application to handle EDGAR data
    - `etl3`: back-end db for time series with real-time and point-in-time
      semantics

- Level 3
  - Component = a group of related functionality encapsulated behind a
    well-defined interface (e.g., collection of classes behind an interface)
  - Components correspond to the second level of directory
  - E.g., in `//p1/edgar`
    - `api`: real-time system storing the data from EDGAR
    - `company_commodity_mapping`: data pipeline to process mapping between
      commodities and companies
    - `form8`: data pipeline processing form 8

- Level 4
  - Classes
  - Typically we organize multiple related classes in files
  - E.g., in `//p1/edgar/form8`
    - `analyze_results.py`: classes and functions to analyze results from the
      data pipeline
    - `extract_tables.py`: class `TableExtractor` extracting tables from Form 8
    - `filter_tables.py`: class `TableFilterer`
    - `match_targets.py`
    - `normalize_table.py`

## Generating class diagram

- To generate a class diagram (level 4 of c4), you can run
  ```
  > dev_scripts/create_class_diagram.sh
  ```

# Brief introduction to PlantUML

- Unified Modeling Language (UML) is a modeling language for software
  engineering to provide a standard way to visualize design of a system

- We use mainly Class Diagrams
  - For information on some class diagram convention see
    https://en.wikipedia.org/wiki/Class_diagram

- You can refer to the PDF guide at http://plantuml.com/guide for an extensive
  description of what PlantUML can do
  - We are mainly interested in the "Class diagram" section

- The website https://structurizr.com has lots of information on using tools for
  C4

## PlantUML is Markdown

- We use PlantUML for rendering diagrams in our documentation
  - For interactive use you can rely on online tools like:
    - [planttext](https://www.planttext.com/)
    - [liveuml](https://liveuml.com/)

- You can embed the diagrams in a `architecture.md` or a `README.md` in the
  corresponding folders

- To render PlantUML in our markdown files instead of `@startuml` you need to use
  the tag:
  ```
  \`\`\`plantuml
  ...
  \`\`\`
  ```

- We have a `render_md.py` tool to embed images after `plantuml` section.
  Typical usage to insert images to the markdowm file and to preview it:
  ```
  > documentation/scripts/render_md.py -i documentation/general/architecture.md
  ```
