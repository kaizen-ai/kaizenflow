# Ontology

- There are entities
  - Entities are represented as vertices (equivalently, nodes) in the graph
  - Entities can be
    - concrete things in the world (e.g., the company Exxon)
    - abstract concepts (e.g., "demand")
    - anything that is tracked/measured/reported, e.g.,
      - economic indices (not the values, but the concept of the index)
      - number of oil wells in the US (again, not the number, but the notion)
  - Entities may
    - be related to other entities (expressed via directed edges)
    - have stand-alone properties
  - E.g., some entities have supply, demand, inventory, e.g.,
    - "number of residential homes"
    - "interest rates"


- The graph structure expresses relationships between entities
  - Relationships are represented as directed edges
  - Edges may have properties themselves
  - Certain relationships only hold under certain assumptions 
  - There are concepts like change of a number of entities
    - E.g., one entity can influence a number of entities or the change of number
      of entities
  - Relationships have "strength"
    - We can have some priors on that
    - We can infer it from the data using our priors
  - Relationships have "velocity"
    - How quickly a relationship unfolds
    - Some relationships may be strong but subject to a time lag (e.g., impact of
        infrastructure investment, macroeconomic policy, regulatory changes, etc.)
  - Relationships can be symmetric or not
    - E.g., the rate at which something changes when positive or negative can be
      different so there are 2 coefficients
    - We can have a prior on these two coefficients, like we thing they should be
      typically the same, and then 

- For some entities what matters is the "level", for others is "change", or
  "acceleration"

- There are temporal concepts like:
  - prolonged ... causes 
  - E.g., prolonged weakness in housing starts is often a precursor of an
    economic downturn

- We can iterate on the KG by adding more details
  - We start saying "-^ house starts => -v bonds price"
  - We can expand this causal relationship in terms of all the components
    - "-^ house starts => -^ inflation => -v interest rates => -v bond yield =>
      -v bond prices"

# Why is it useful?

- Allow to judge news events and macroeconomic time series based on our
  understanding of the world
  - The usual approach is to consider events in an isolated fashion

- Allow to build models automatically based on the relationship between
  quantities existing in the real world

# Why does this work?

- Typically one (HFs, academics, banks) fits one variable at the time, without
  considering conditional independence, underlying common factors, ...
- Economic variables depend on many different quantities
  - The different quantities are related to each other
  - Building models by additivity is very suboptimal
  - We consider all the dependencies at once, so we add a new variable and refit
    the entire model, getting a global optimum and not a series of local optima

- Data triangulation
  - Relationships are very noisy
  - Through our KG we measure quantities from many different (not obvious)
    viewpoints
  - The quality of the estimation is much higher
    - It's like a GPS: if you have 2 satellites you can't measure your position,
      but with 3 satellites you have a precise position in 3 dimensions

- The current ML systems for economic quantities make predictions without
  understanding the world
  - The current models are like: "this goes up, then this goes down"
  - We believe that in economics statistical models can't reach high accuracy
    unless they understand how the world works

- https://en.wikipedia.org/wiki/Psychohistory_(fictional)

- We can fit single relationships in the graph when the KB tells us that there
  are no confounding variables and so we know that it's a causal relationship

- It is a probabilistic theorem proving
  - Given knowledge base and an inference engine, we can query the system "would
    this happen?" and achieve answers together with degrees of belief
      - E.g.,
        - Would the interest rates go up 
  - We are mechanizing the process of thinking

- AI learn from its own mistakes
  - The AI asks itself questions in the past when the future is known (and thus
    the answers)
    - Looks at its own mistakes and learn from it
    - This is similar to self-play and allows to synthesize more data points to
      learn from
        - Data scarcity is the single biggest problem in economics and finance
          for AI
        - It's like reinforcement learning for knowledge

- We transform economic books and papers in our representation
  - The system improves even when no data is added, just because it understand
    the world better
  - Asimov story "the last answer"
    - Collecting all the data, understand all the data

- People focus on how many petabyte of data are generated
  - The problem is not how much data is created, rather is understanding what the
    data means

- Could we have predicted the housing bubble?

## Examples

- House starts

- Interest rates
  - Entity

- Mortgage rates
  - Entity

- Number of residential homes
  - Entity

- Demand for residential homes
  - Entity

- Supply for residential homes
  - Entity

- Relationship
  - Overheated economy => -^ interest rates

- Relationship
  - -^ interest rates => -^ mortgage rates (belief)

- -^ mortgage rate => -v demand for residential homes

- -^ interest rates => -^ construction loan rates
- -^ construction loan rates => -v number of future constructions

### Multiplier effect

- -^ residential construction => -^ demand for steel, electricity, glass,
  ..., concrete

- -^ residential construction => -^ demand for carpenters, -^ electricians

- -^ residential construction => -^ furniture, home electronics

- Estimate: construction of 1000 single-family homes => 2500 full-time jobs,
  $100m in wages

- Home construction ~= 5% of GDP

# Refs
- 1999, Zuniga, An Ontology of Economic Objects
  http://ontology.buffalo.edu/FARBER/zuniga.html

- Ontological Problems of Economics
  http://ceur-ws.org/Vol-2205/paper3_oe1.pdf

- 2018, Philosophy of Economics
  https://plato.stanford.edu/entries/economics/

# Zuniga

- Macroeconomic phenomena such as
  - inflation
  - unemployment
  - growth;
  such as existing states of the world

- Such conditions are fact that influence individual saving and spending decision
  - A tree can be perceived as a mean for fulfilling the plan of building a house

- Consumers
- Firms
- Production, demand for a good
- Labor market
- Economic goods
- Commodity
- Money
- Price
- Value

## Economic good

- Economic good = a thing 
