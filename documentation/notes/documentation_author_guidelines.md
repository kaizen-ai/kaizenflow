<!--ts-->
   * [Guidelines for writing workflows](#guidelines-for-writing-workflows)
   * [Useful reference](#useful-reference)
   * [Style and cosmetic lints](#style-and-cosmetic-lints)
      * [Use nice 80 columns formatting](#use-nice-80-columns-formatting)
      * [Empty line after heading](#empty-line-after-heading)
      * [Style for numbered lists](#style-for-numbered-lists)
      * [Using code style](#using-code-style)
      * [Indenting code style](#indenting-code-style)
      * [Use bullet lists](#use-bullet-lists)
      * [Improve your written English](#improve-your-written-english)
      * [Make sure your markdown looks good](#make-sure-your-markdown-looks-good)
      * [Table of content (TOC)](#table-of-content-toc)
         * [Markdown TOC](#markdown-toc)
         * [To insert a TOC](#to-insert-a-toc)
         * [To update all markdown files](#to-update-all-markdown-files)
         * [Script installation](#script-installation)
   * [The team member list](#the-team-member-list)



<!--te-->

# Guidelines for writing workflows

- Make no assumptions on the user
    - Nothing is obvious to somebody who doesn't know

- How to know if the process worked
    - Add sections explaining how to verify that the process completed
      successfully

- Have a trouble-shooting procedure
    - One approach is to always start from scratch

# Useful reference
- [Markdown cheatsheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)

# Style and cosmetic lints

## Use nice 80 columns formatting
- vim has a `:gq` command to reflow the comments

## Empty line after heading
- Leave an empty line after an heading to make it more visible
    ```
    # Very important title

    - Not really important

    ## Coming through! I've big important things to do!

    - ... and his big important wheels got STUCK!
    ```

## Style for numbered lists
- We use number lists like:
    ```
    1. Foo bar!
        - hello
        - world
        
    2. Baz
    ```

## Using `code` style
- We use `code` style for
    - code
    - dirs (e.g., `/home/users`)
    - command lines (e.g., `git push`)

- When using a block of code use the write syntax highlighting
    - Bash
        ```bash
        > git push
        ```
    - Python
        ```python
        if __name__ == "__main__":
            predict_the_future()
            print("done!")
        ```

## Indenting `code` style

- GitHub / pandoc seems to render incorrectly a code block unless it's indented
  over the previous line
    - **Bad**
```bash
> git push
```
    - **Good**
        ```bash
        > git push
        ```

## Use bullet lists
- We like using bullet list since one can represent thought process more clearly,
  e.g.,
    - This is thought #1
        - This is related to thought #1
    - This is thought #2
        - Well, that was cool!
        - But this is even better

## Improve your written English
- Use English spell-checker
    - Unfortunately this is not enough
- Type somewhere where you can use several free choices:
    - [Grammarly](www.grammarly.com),
    - [LanguageTool](https://www.languagetool.org)
    - or other proof reading
  and copy-paste
- This is super-useful to improve your English since you see the error and the
  correction
    - Otherwise you will keep making the same mistakes forever

## Make sure your markdown looks good
- You can:
    - check in a branch and use GitHub to render it
    - use pycharm to edit, which also renders it side-by-side

## Table of content (TOC)

### Markdown TOC
- Unfortunately both markdown and GitHub don't support automatically generating a
  TOC for a document
- We work around this problem using a script that post-process the markdown
  adding links to create a TOC

### To insert a TOC
- Add the tags at the beginning of the markdown file
    ```markdown
    <!--ts-->
   * [Guidelines for writing workflows](#guidelines-for-writing-workflows)
   * [Useful reference](#useful-reference)
   * [Style and cosmetic lints](#style-and-cosmetic-lints)
      * [Use nice 80 columns formatting](#use-nice-80-columns-formatting)
      * [Empty line after heading](#empty-line-after-heading)
      * [Style for numbered lists](#style-for-numbered-lists)
      * [Using code style](#using-code-style)
      * [Indenting code style](#indenting-code-style)
      * [Use bullet lists](#use-bullet-lists)
      * [Improve your written English](#improve-your-written-english)
      * [Make sure your markdown looks good](#make-sure-your-markdown-looks-good)
      * [Table of content (TOC)](#table-of-content-toc)
         * [Markdown TOC](#markdown-toc)
         * [To insert a TOC](#to-insert-a-toc)
         * [To update all markdown files](#to-update-all-markdown-files)
         * [Script installation](#script-installation)
   * [The team member list](#the-team-member-list)



    <!--te-->
    ```

### To update all markdown files
- Run:
    ```bash
    > documentation/scripts/lint_md.sh
    ```

### Script installation 
- To install the script follow the instructions
  [here](https://github.com/ekalinin/github-markdown-toc):
    ```python
    > wget https://raw.githubusercontent.com/ekalinin/github-markdown-toc/master/gh-md-toc
    > chmod +x gh-md-toc
    > mv gh-md-toc documents/scripts
    ```

- We have installed it in the repo but the script might need to be updated once
  in a while

# The team member list
- In reversed alphabetical order (just to be fair)
    - [ ] Stas
    - [ ] Sonya
    - [ ] Sergey
    - [ ] Paul
    - [ ] Liza
    - [ ] Julia
    - [ ] GP
