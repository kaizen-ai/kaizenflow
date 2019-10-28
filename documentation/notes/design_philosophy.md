# Philosophy

## Keep it simple

Follow the [KISS principle](https://en.wikipedia.org/wiki/KISS_principle).

Pursue simple, elegant solutions. Some things are inherently complex, but even
complex systems can be broken down into simple pieces.

Designs that are simple are easier to understand, easier to modify, and
easier to debug.

# Architecture

## Use design patterns

[Design patterns](https://en.wikipedia.org/wiki/Software_design_pattern)
are idioms or recipes for solving problems that commonly appear in
software engineering across projects and even languages. The classical
introduction to design patterns is the so-called "Gang of Four"
[book](https://www.amazon.com/Design-Patterns-Object-Oriented-Addison-Wesley-Professional-ebook/dp/B000SEIBB8).
A free python-focused reference is available
[here](https://python-3-patterns-idioms-test.readthedocs.io/en/latest/).

Expanding your knowledge of design patterns is a worthwhile investment,
because design patterns
  - Capture elegant solutions that have been developed by many
    experienced programmers over a long period of time
  - Provide a framework and reference point for software architecture
  - Are widely used and well-known and therefore quickly recognized by skilled
    programmers

In other words, by using design patterns, you
  - Don't have to re-invent the wheel
  - Simplify the high-level picture of your code
  - Make it easier for other people to understand your code

# Functions

## Avoid modifying the function input

If, for example, a function `f` accepts a dataframe `df` as its (sole)
argument, then, ideally, `f(df)` will not modify `df`. If modifications are
desired, then instead one can do
```
def f(df):
    df = df.copy()
    ...
    return df
```
in the function so that `f(df)` returns the desired new dataframe without
modifying the dataframe that was passed in to the function.

In some cases the memory costs associated with the copy are prohibitive,
and so modifying in-place is appropriate. If such is the case, state it
explicitly in the docstring.

Functions that do not modify the input are especially convenient to have in
notebook settings. In particular, using them makes it easy to write blocks
of code in a notebook that will return the same results when re-executed
out of order.

## Prefer pure functions by default

[Pure functions](https://en.wikipedia.org/wiki/Pure_function)
have two key properties:
  1. If the function arguments do not change, then the return value returned
     does not change (in contrast to, e.g., functions that rely upon global
     state)
  2. Function evaluation does not have
     [side effects](https://en.wikipedia.org/wiki/Side_effect_(computer_science)
     
Some nice properties enjoyed by pure functions are:
  - They are easy to understand and easy to test
  - Using pure functions makes refactoring easier
  - They are often a natural choice for data manipulation and analysis
  - They are convenient in notebooks

Though it is good to develop an appreciation for
[functional programming](https://en.wikipedia.org/wiki/Functional_programming),
and we like to adopt that style when appropriate, we recognize that it is not
pragmatic to dogmatically insist upon a functional style (especially in our
domain and when using Python). 

