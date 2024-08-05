<!-- toc -->
<!-- tocstop -->

// From https://developers.google.com/tech-writing/one/

# Google's technical writing: Part 1

## Define new or unfamiliar

- If your document introduces a term, define the term
- If the term already exists, link to a good existing explanation

## Use terms consistently

- Don't change the name of something while talking about it
  - E.g., `Protocol Buffers` vs `protobufs`
- You can do something like:
  ```
  Protocol Buffers (or protobufs for short)
  ```

## Use acronyms properly

- On the initial use of an unfamiliar acronym spell out the full term
- E.g., `Telekinetic Tactile Network (TTN) ...`
- Acronyms take attention to be expanded in their full form
- Sometimes acronyms develop their own identity (e.g., HTML)
- An acronym should be significantly shorter than the full term
- Don't define acronyms that will be used only a few times

## Use strong verbs

- Choose precise, strong, and specific verbs
  - Weak verbs are "be", "occur", "happen"

**Good**
```
Dividing by zero raises the exception.
```

**Bad**
```
The exception occurs when dividing by zero.
```

## Use short sentences

- Each sentence should convey a single idea, thought, concept
  - Break long sentences into single-idea sentences
- Convert long sentences into bulleted list
  - E.g., "and", "or" suggest to refactor into a bulleted list

## Remove fillers

**Good**
```
This design document describes Project Frambus.
```

**Bad**
```
This design document provides a detailed description of Project Frambus.
```

## Focus each paragraph on a single topic

- A paragraph is an independent unit of logic
- Ruthlessly delete sentence that doesn't relate to the current topic

## Avoid wall-of-text

- Readers often ignore long paragraphs
- Paragraphs should contain 3 to 5 sentences

## Answer what, why, and how

- Good paragraphs answer the following questions
  - What: what are you trying to tell your reader?
  - Why: why is it important for the reader to know this?
  - How: how should the reader use this knowledge

- E.g.,
  - [What]: The `garp()` function returns the delta between a dataset's mean and
    median.
  - [Why]: Many people believe unquestioningly that a mean always holds the
    truth. However, a mean is easily influenced by a few very large or very
    small data points.
  - [How]: Call `garp()` to help determine whether a few very large or very
    small data points are influencing the mean too much. A relatively small
    `garp()` value suggests that the mean is more meaningful than when the
    `garp()` value is relatively high.

## Know your audience

- Your document needs to provide information that your audience needs but
  doesn't already have
  - Define your audience
    - E.g., software engineers vs program managers
    - E.g., graduate students vs first-year undergraduate students
  - Determine what your audience needs to learn
    - E.g.,
      ```verbatim
      After reading the documentation, the audience will know how to do the
      following tasks
      - Use ...
      - Do ...
      ...
      ```
  - Fit documentation to your audience
    - Avoid the "curse of knowledge": experts forget that novices don't know
      what you already know

## State document's scope

- A good document begins by defining its scope and its non-scope, e.g.,
  ```
  This document describes the design of Project Frambus, but not the related
  technology Froobus.
  ```

## Summarize the key points at the start

- Ensure that the start of your document answers your readers' essential
  questions
- The first page of a document determines if the readers makes it to page two

// From https://developers.google.com/tech-writing/two

# Google's technical writing: Part 2

## Adopt a style guide
- Many companies and large open source projects adopt a style guide for
  documentation
  - E.g., https://developers.google.com/style

## Think like your audience
- Step back and try to read your draft from the point of view of your audience

## Come back to it later
- After you write your first (or second or third) draft, set it aside
- Come back later and read it with fresh eyes to find things you can improve

## Organizing large docs
- You can organize a collection of information into
  - a longer standalone document; or
  - set of shorter interconnected documents (e.g., website, wiki)
    - Pros: easy to find information searching in the single back

// TODO

# Resources

- [https://developers.google.com/tech-writing/overview]
