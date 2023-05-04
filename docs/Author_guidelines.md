# Guidelines for writing workflows

- Make no assumptions on the user's knowledge
    - Nothing is obvious to somebody who doesn't know
- How to know if the process worked
    - Add sections explaining how to verify that the process completed successfully
- Have a trouble-shooting procedure
    - One approach is to always start from scratch

# Useful reference

- [Markdown cheatsheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
- [Google guide to Markdown](https://github.com/google/styleguide/blob/gh-pages/docguide/style.md)
    - TODO(gp): Make sure it's compatible with our linter

# Style and cosmetic lints

## Use nice 80 columns formatting for txt files

- Vim has a `:gq` command to reflow the comments

## Empty line after heading

- Leave an empty line after a heading to make it more visible
    ```
    # Very important title
    - Not really important

    ## Coming through! I've big important things to do!
    - ... and his big important wheels got STUCK!
    ```

## Style for numbered lists

- We use lists like:
    ```
    - Foo bar!
        - hello
        - world

    - Baz
    ```

## Using `code` style

- We use `code` style for
    - Code
    - Dirs (e.g.,` /home/users`)
    - Command lines (e.g., `git push`)
- When using a block of code use the write syntax highlighting
    - Bash
        ```
        `> git push`
        ```
    - Python

            ```
            if __name__ == "__main__":
                predict_the_future()
                print("done!")
            ```

## Indenting `code` style

- GitHub / pandoc seems to render incorrectly a code block unless it's indented over the previous line
    - Bad
        `> git push`
    - Good
    
        `> git push`

## Use bullet lists

- We like using bullet list since one can represent thought process more clearly, e.g.,
    - This is thought #1
        - This is related to thought #1
    - This is thought #2
        - Well, that was cool!
        - But this is even better

## Improve your written English

- Use English spell-checker
    - Unfortunately this is not enough
- Type somewhere where you can use several free choices:
    - [Grammarly](https://github.com/cryptokaizen/cmamp/blob/master/documentation/general/www.grammarly.com)
    - [LanguageTool](https://www.languagetool.org/)
    - Or other proofreading and copy-paste
- This is super-useful to improve your English since you see the error and the correction
    - Otherwise you will keep making the same mistakes forever

## Make sure your markdown looks good

- You can:
    - Check in a branch and use GitHub to render it
    - Use pycharm to edit, which also renders it side-by-side

## Table of content (TOC)

### Markdown TOC

- Unfortunately both markdown and GitHub don't support automatically generating a TOC for a document
- We work around this problem using a script that post-process the markdown adding links to create a TOC

### To insert a TOC

- Add the tags at the beginning of the markdown file
- Note that in the text below we interspersed spaces to avoid the TOC processor to add a table of content also here:
    ```
    &lt; ! - - t s - - >
    &lt; ! - - t e - - >
    ```

### To update all markdown files

- Run:

    `> documentation/scripts/lint_md.sh`

# Google docs style conventions

## Headings

We add N (where N is the heading level) `#` before the heading name, e.g.,

- Heading 1:
    ```
    # Heading 1
    ```
- Heading 2:
    ```
    ## Heading 2
    ```

The reason is that sometimes one doesn't have the time or the patience to format things properly, so at least there is some indication of the level of the titles.

Do not forget to convert the normal text to heading:

- Select the text
- Go to `Format`
- Go to `Paragraph style`
- Choose the heading style, e.g. `heading 2`

## Font

Normal text:
- Font: Arial
- Font size: 11

Headings:
- Font: Arial
- Style: bold
- Font size: should be adjusted automatically when one converts “Normal text” to “Heading N”, e.g., when converting some text of size 11 to “Heading 1” the font sizes becomes 20


# Convert between Gdocs and Markdown

## Gdocs -> Markdown

Approach 1:
- Use the [Docs to Markdown](https://github.com/evbacher/gd2md-html/wiki) extension
    - Install [the extension](https://gsuite.google.com/marketplace/app/docs_to_markdown/700168918607) from the G Suite marketplace
    - [User guide](https://github.com/evbacher/gd2md-html/wiki#using-docs-to-markdown) for the extension
- One needs to accept/reject all suggestions in a gdoc as the extension works poorly when a document is edited in the suggestion mode

Approach 2:
- [https://mr0grog.github.io/google-docs-to-markdown/](https://mr0grog.github.io/google-docs-to-markdown/)

Lint the markdown:
- Replace all bullet points as `-` with `-`, if needed
- Run the `linter.py`
    - Do not mix manual edits and linter runs
    - If the linter messes up the text
        - File bugs in `amp` with examples what the linter does incorrectly
- When a gdoc becomes obsolete or it’s deleted
    - Add a note at the top of a gdoc explaining what happened
        - Example: "Moved to /new_markdown_file.md"
    - Strike out the entire document
    - Move the gdoc to the [_OLD directory](https://drive.google.com/drive/u/0/folders/1J4B1vq8EwT-q_z7qSLCZ9Tug2CA9f8i7)

## Markdown -> Gdocs

Approach 1:

- `> pandoc MyFile.md -f markdown -t odt -s -o MyFile.odt`
- Download the [template](https://docs.google.com/document/d/1Z_OdO6f7VYjimgjfGPofsYHyWvyxXrtOVVcvCauJIpI/edit) in odt format
- `> pandoc code_organization.md -f markdown -t odt -s -o code_org.odt --reference-doc /Users/saggese/Downloads/Gdoc\ -\ Template.odt`
- Open it with TextEdit, copy-paste to Gdoc

Approach 2:
- Instead of copy-paste the markdown into Gdocs, you can copy the rendered markdown in a Gdoc
    - Gdocs does a good job of maintaining the formatting, levels of the headers, the links, and so on

Approach 3:
- [https://markdownlivepreview.com/](https://markdownlivepreview.com/)

TODO(gp): Check if the roundtrip works

# # Markdown vs Google Docs

Google Docs pros:
- Easy to embed figures
- Easy to collaborate
- Easy to make quick changes (instead of making a commit)
- Easy to publish (just make them public with proper permissions)
- Styling
    - [https://webapps.stackexchange.com/questions/112275/define-special-inline-styles-in-google-docs](https://webapps.stackexchange.com/questions/112275/define-special-inline-styles-in-google-docs)

Interesting add-ons:
- Enable Markdown
- Code blocks
    - Use darcula, size 10
        ```
        def hello():
        print("hello")
        ```
- Auto-latex equations

Markdown pros:
- Can use vim
- Can version control
- Easy to use verbatim (e.g., typing `foobar`)
- Easy to style using pandoc
- Easy to embed code
- Easy to add Latex equations

Rules of thumb:
- If it has images -> gdocs
- If it has mostly formulas -> md
- If they are notes from book -> md
- If it is a tutorial (especially with figures) -> gdocs