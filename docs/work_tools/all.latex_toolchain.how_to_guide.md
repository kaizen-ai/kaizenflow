

<!-- toc -->

- [Latex Toolchain](#latex-toolchain)
  * [Running and linting Latex files](#running-and-linting-latex-files)
  * [Embedding Mermaid and PlanUML figures](#embedding-mermaid-and-planuml-figures)
  * [Finding citations](#finding-citations)
  * [TODOs](#todos)

<!-- tocstop -->

# Latex Toolchain

## Running and linting Latex files

We organize each project is in a directory (e.g., under `//papers`)

Under each dir there are two scripts:

- `run_latex.sh`
- `lint_latex.sh` that assign some variables and then call the main scripts to
  perform the actual work:
- `dev_scripts/latex/run_latex.sh`
- `dev_scripts/latex/lint_latex.sh`

Both main scripts are "dockerized" scripts, which build a Docker container with
dependencies and then run use it to process the data

To run the Latex flow we assume (as usual) that user runs from the top of the
tree

To create the PDF from the Latex files:
```
> papers/DataFlow_stream_computing_framework/run_latex.sh
...
```

To lint the Latex file:
```
> papers/DataFlow_stream_computing_framework/lint_latex.sh
...
+ docker run --rm -it --workdir /Users/saggese/src/cmamp1 --mount type=bind,source=/Users/saggese/src/cmamp1,target=/Users/saggese/src/cmamp1 lint_latex:latest sh -c ''\''./tmp.lint_latex.sh'\''' papers/DataFlow_stream_computing_framework/DataFlow_stream_computing_framework.tex
papers/DataFlow_stream_computing_framework/DataFlow_stream_computing_framework.tex 320ms (unchanged)
```

## Embedding Mermaid and PlanUML figures

Update ./dev_scripts/documentation/render_md.py

- Rename to render_figures.py
- It works on both Markdown and Latex files
- Find a mermaid/plantuml block and then add an image

%`mermaid %flowchart %  Vendor Data --> VendorDataReader --> DataReader --> User %`

## Finding citations

The simplest way is to use Google Scholar and then use the "Cite" option to get
a Bibtex entry

Some interesting links are
https://tex.stackexchange.com/questions/143/what-are-good-sites-to-find-citations-in-bibtex-format

## TODOs

- Add a script to decorate the file with separators as part of the linting
  ```
  % ################################################################################
  \section{Adapters}
  % ================================================================================
  \subsection{Adapters}
  % --------------------------------------------------------------------------------
  \subsubsection{Adapters}
  ```

- Convert the Latex toolchain into Python code

- Add a script to run a ChatGPT prompt on a certain chunk of text

- Easily create a vimfile to navigate the TOC
