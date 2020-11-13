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
  C4 and lots of [examples](https://structurizr.com/share/52804/plantuml)

## PlantUML is Markdown

- We use PlantUML for rendering diagrams in our documentation
  - For interactive use you can rely on online tools like:
    - Online editors:
      - [planttext](https://www.planttext.com/)
      - [liveuml](https://liveuml.com/)
      - [PlantUML Web Server](http://www.plantuml.com/plantuml/uml/SyfFKj2rKt3CoKnELR1Io4ZDoSa70000)
    - PyCharm plugin (create and edit `.puml` file locally):
      - [PlantUML integration](https://plugins.jetbrains.com/plugin/7017-plantuml-integration)

- We create `README.md` and `architecture.md` markdown files to document
  software. `README.md` is for general content, `architecture.md` is for code
  architecture description. You can embed the diagrams in `architecture.md` file
  in a correspondent folder.

- To render PlantUML in our markdown files instead of `@startuml` you need to use
  the tag:
  ````
  ```plantuml
  ...
  ```
  ````

- We have a `render_md.py` tool to embed images after `plantuml` section.
  Typical usage to insert images to the markdowm file and to preview it:
  ```
  > documentation/scripts/render_md.py -i documentation/general/architecture.md
  ```

- We try to let the rendering engine do its job of deciding where to put stuff
  even if sometimes it's not perfect. Otherwise, with any update of the text we
  need to iterate on making it look nice: we don't want to do that.

- `.md` files should be linted by our tools

### Our conventions

- Names
  - Each name in mappings should be exactly the same (maybe without some invalid
    chars, like `.`) to not create a cognitive burden to the reader. It's better
    to optimize for readability rather than by the number of chars. E.g.,
    ```plantuml
    [build_configs.py] as build_configs_py
    [TableExtractor] as TableExtractor
    ```

  - We keep components / classes in alphabetical order, so we can find them
    quickly in the code

- Notes
  - Put notes describing some components / classes inside the blocks they refer
    to. E.g.,
    ```plantuml
    node mapping as map {
    [CIK<->Ticker] as ctmap
    note top of ctmap: My useful note.
    [CIK<->GVKEY] as cgmap
    }
    ```

  - We use conventions for notes as for the code comments:
    - Start a note with a capital and end with `.`. In this way, it may be even
      easier to visually distinguish notes from arrow labels.
    - Put notes straight after their related component definition, so a note will
      look like a comment in the code

- Arcs
  - The direction of the arcs represents the direction of the action. E.g.,

    ```plantuml
    apple --> ground : falls to
    ```

  - We use the third person for describing actions

- We use comments as headers to organize the `architecture.md`. Note that the
  comments in `plantuml` are introduced with `'`. Some frequently used headers
  are:
  - `' Components`
  - `' Databases`
  - `' Containers`
  - `' Edge labels`
  - `' Notes`

- An example of acceptable C4 diagram plantuml snippet:

  ```plantuml
    ' Components
    component [Edgar API] as Edgar_API
    note top of Edgar_API : System storing the real-time\nand historical data from EDGAR.
    component [Headers dataset] as Headers_dataset

    ' Databases
    database "Compustat DB" as Compustat_DB
    note top of Compustat_DB : Third-party database\nwith financial data.

    ' Containers
    node Form8 as form8 {
     [analyze_results.py] as analyze_results_py
     note left of analyze_results_py: Computes matching statistics.
     [build_configs.py] as build_configs_py
     [edgar_utils.py] as edgar_utils_py
     [run_pipeline.py] as run_pipeline_py
     [TableExtractor]
     note right of TableExtractor: Extracts forms tables.
     [TableNormalizer]
     note right of TableNormalizer: Normalizes extracted tables.
     [TableFilterer]
     note right of TableFilterer: Takes only financial tables\nfrom normalized tables.
     [TargetMatcher]
     note right of TargetMatcher: Matches financial values in tables.
    }
    node mapping as mapping {
     [CIK<->Ticker] as CIK_Ticker
     [CIK<->GVKEY] as CIK_GVKEY
    }
    node universe as universe{
     [S&P400]
     [S&P500]
     [S&P600]
     [S&P1500]
     [S&P1500_ISAM]
    }
    note left of universe: Universe of companies\n as Tickers/GVKEYs.

    ' Edge labels
    Edgar_API --> edgar_utils_py: provides filings payloads to
    Compustat_DB --> run_pipeline_py: provides target\nvalues to match on to
    build_configs_py --> run_pipeline_py: provides pipeline\nparameters to
    edgar_utils_py --> TableExtractor: provides universe filings to
    analyze_results_py --> run_pipeline_py: provides functions\nto run the matching in to
    mapping --> edgar_utils_py: provides mapping to construct\n universe as CIKs to
    Headers_dataset --> analyze_results_py: provides filing\ndates to
    TableExtractor --> TableNormalizer: provides tables to be normalized to
    TableFilterer --> run_pipeline_py: provides forms\n values to be matched to
    TargetMatcher --> analyze_results_py: matches values in
    TableNormalizer --> TableFilterer: provides tables to be filtered to
    universe --> mapping: provides universe of companies to
    ```

  You can find the correspondent `architecture.md` file
  [here](https://github.com/ParticleDev/commodity_research/blob/master/edgar/forms8/architecture.md).
