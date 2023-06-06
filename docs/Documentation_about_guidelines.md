# Documentation about guidelines

<!-- toc -->

- [Guidelines for describing workflows](#guidelines-for-describing-workflows)
- [Markdown vs Google Docs](#markdown-vs-google-docs)
    - [In general](#in-general)
    - [Markdown pros](#markdown-pros)
    - [Google Docs pros](#google-docs-pros)
    - [Rules of thumb](#rules-of-thumb)
    - [Useful references](#useful-references)
- [Style and cosmetic lints](#style-and-cosmetic-lints)
  - [Always use markdown linter](#always-use-markdown-linter)
  - [Table of content (TOC)](#table-of-content-toc)
  - [Use nice 80 columns formatting for txt files](#use-nice-80-columns-formatting-for-txt-files)
  - [Empty line after heading](#empty-line-after-heading)
  - [Bullet lists](#bullet-lists)
  - [Using `code` style](#using-code-style)
  - [Indenting `code` style](#indenting-code-style)
  - [Embedding screenshots](#embedding-screenshots)
  - [Improve your written English](#improve-your-written-english)
  - [Make sure your markdown looks good](#make-sure-your-markdown-looks-good)
- [Google docs style conventions](#google-docs-style-conventions)
  - [Headings](#headings)
  - [Font](#font)
- [Convert between Gdocs and Markdown](#convert-between-gdocs-and-markdown)
  - [Gdocs -> Markdown](#gdocs---markdown)
    - [Using `pandoc`](#using-pandoc)
    - [Using Chrome Docs to Markdown extension](#using-chrome-docs-to-markdown-extension)
    - [Cleaning up converted markdown](#cleaning-up-converted-markdown)
  - [Markdown -> Gdocs](#markdown---gdocs)

<!-- tocstop -->


# Guidelines for describing workflows

- Make no assumptions on the user's knowledge
  - Nothing is obvious to somebody who doesn't know
- Add ways to verify if a described process worked
  - E.g., "do this and that, if this and that is correct should see this"
- Have a trouble-shooting procedure
  - One approach is to always start from scratch

# Markdown vs Google Docs

### In general

- We prefer to use Markdown for technical documentation
- We use Google for notes from meetings and research

### Markdown pros

- Can use vim
- Can version control
- Easy to use verbatim (e.g., typing `foobar`)
- Easy to style using pandoc
- Easy to embed code
- Easy to add Latex equations
- Easy to grep

### Google Docs pros

- Easy to embed figures
- Easy to collaborate
- Easy to make quick changes (instead of making a commit)
- Easy to publish (just make them public with proper permissions)
- Styling
  - [https://webapps.stackexchange.com/questions/112275/define-special-inline-styles-in-google-docs](https://webapps.stackexchange.com/questions/112275/define-special-inline-styles-in-google-docs)
- Interesting add-ons:
  - Enable Markdown
  - Code blocks
    - Use darcula, size 10
      ```
      def hello():
      print("hello")
      ```
  - Auto-latex equations

### Rules of thumb

- Use Markdown
  - If doc is going to be used as a public guideline
  - If doc has mostly text, code, and formulas
  - If there are notes from a book
- Use Gdoc
  - If doc requires a lot of images that cannot be placed as text
  - If doc is a research of an analysis

### Useful references

- [Markdown cheatsheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
- [Google guide to Markdown](https://github.com/google/styleguide/blob/gh-pages/docguide/style.md)
  - TODO(gp): Make sure it's compatible with our linter

# Style and cosmetic lints

## Always use markdown linter

- Most cosmetic lints described further can be taken care automatically by our
  markdown linter, so make sure to run it after implementing the changes
- The file is `dev_scripts/lint_md.sh`, see the docstrings for more details
  - Example run:
    ```
    dev_scripts/lint_md.sh docs/Documentation_about_guidelines.md
    ```
- Do not mix manual edits and linter runs. Best practice is to run the linter
  and commit the changes it made as separate commit
- If the linter messes up the text, file an issue with examples of what the
  linter does incorrectly

## Table of content (TOC)

- Unfortunately both markdown and GitHub don't support automatically generating
  a TOC for a document
- To generate a table of content:
  - Add the following tag at the top of the markdown file below the document title:
    ```
    <!-- toc -->
    ```
  - Run the markdown linter in order to build TOC automatically

## Use nice 80 columns formatting for txt files

- Vim has a `:gq` command to reflow the comments
- There are plugins to take care of this for PyCharm
- Our markdown linter takes care of reflowing the comments as well

## Empty line after heading

- Leave an empty line after a heading to make it more visible

  - _Bad_
    ```
    # Coming through! I've big important things to do!
    - ... and his big important wheels got STUCK!
    ```
  - _Good_

    ```
    # Very important title

    - Less important text
    ```

## Bullet lists

- We like using bullet list since one can represent thought process more
  clearly, e.g.,
  - This is thought #1
    - This is related to thought #1
  - This is thought #2
    - Well, that was cool!
    - But this is even better
- We strictly use `-` instead of `*`, circles, etc.:
  - _Bad_
    ```
    * Foo bar!
      * hello
      * world
    * Baz
    ```
  - _Good_
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
  - _Bad_
  ```
  > git push
  ```
  - _Good_
    ```
    > git push
    ```

## Embedding screenshots

- [**Avoid** to use screenshots whenever possible!](https://github.com/sorrentum/sorrentum/blob/master/docs/First_review_process.md#do-not-use-screenshots)
- However, sometimes we need it (e.g., plot infographics, website inteface,
  etc.)
- To do it correctly:
  - Place your screenshot in any comment window at GitHub
    - <img width="770" alt="screenshot" src="https://github.com/sorrentum/sorrentum/assets/31514660/ade0b104-d162-40a8-9f0d-3edadf38c57e">
    - This will upload the image to the GitHub cloud
    - You DO NOT have to publish a comment, the provided link is already ready
      to use!
  - Make sure your link has no not-English symbols in `alt` section
    - <img width="778" alt="symbols" src="https://github.com/sorrentum/sorrentum/assets/31514660/6e54d66b-d45f-43f8-8bb6-b9c94217068e">
    - They sometimes appear if your native PC language is not English
    - You can avoid it by giving the picture a name in English
    - Alternatively, you can just edit the `alt` section in the generated link -
      this will not corrupt the file
  - Place the generated and edited link to the markdown file
    - <img width="461" alt="last" src="https://github.com/sorrentum/sorrentum/assets/31514660/a416e49d-2859-40d6-ad13-792e6304f402">

## Improve your written English

- Use English spell-checker
  - Unfortunately this is not enough
- Type somewhere where you can use several free choices:
  - [Grammarly](https://github.com/cryptokaizen/cmamp/blob/master/documentation/general/www.grammarly.com)
  - [LanguageTool](https://www.languagetool.org/)
  - Or other proofreading and copy-paste
- This is super-useful to improve your English since you see the error and the
  correction
  - Otherwise you will keep making the same mistakes forever

## Make sure your markdown looks good

- You can:
  - Check in a branch and use GitHub to render it
  - Use Pycharm to edit, which also renders it side-by-side
  - Compare your markdown with already published
    [documentation](/docs/README.md)

# Google docs style conventions

## Headings

- We add N (where N is the heading level) `#` before the heading name, e.g.,
  - Heading 1:
    ```
    # Heading 1
    ```
  - Heading 2:
    ```
    ## Heading 2
    ```
- The reason is that sometimes one doesn't have the time or the patience to
  format things properly, so at least there is some indication of the level of
  the titles
- Avoid having multiple `#` separatd by a space that sometimes appear in a
  process of convertion of Gdocs to Markdown files
  - _Bad_
    ```
    # # Heading 1
    ```
  - _Good_
    ```
    # Heading 1
    ```

## Font

- Normal text:
  - Font: Arial
  - Font size: 11
- Headings:
  - Font: Arial
  - Style: bold
  - Font size: should be adjusted automatically when one converts “Normal text”
    to “Heading N”, e.g., when converting some text of size 11 to “Heading 1”
    the font sizes becomes 20

# Convert between Gdocs and Markdown

## Gdocs -> Markdown

### Using `pandoc`

- In general we recommend to use this approach
- Pros
  - Best for a large document
  - Handle figures
- Cons
  - Need to remove formatting
  - Need to move files
- Process:
  - Download document as docx
  - Convert it to markdown using `pandoc`
    ```
    > pandoc --extract-media ./ -f docx -t markdown -o out.md in.docx
    ```
  - The entire conversion flow is in `dev_scripts/convert_gdoc_to_markdown.sh`

### Using Chrome Docs to Markdown extension

- Best for a large document
- Approach 1:
  - Use the [Docs to Markdown](https://github.com/evbacher/gd2md-html/wiki)
    extension
    - Install
      [the extension](https://gsuite.google.com/marketplace/app/docs_to_markdown/700168918607)
      from the G Suite marketplace
    - [User guide](https://github.com/evbacher/gd2md-html/wiki#using-docs-to-markdown)
      for the extension
  - One needs to accept/reject all suggestions in a gdoc as the extension works
    poorly when a document is edited in the suggestion mode
- Approach 2:
  - [Google-docs-to-markdown/](https://mr0grog.github.io/google-docs-to-markdown/)

### Cleaning up converted markdown

- Lint the markdown:
  - Replace all bullet points as `-` with `-`, if needed
  - Removing artifacts manually or using the script
    ```
    > dev_scripts/convert_gdoc_to_markdown.sh
    ```
  - Remove empty lines manually
    ```
    :'<,'>! perl -ne 'print if /\S/'
    ```
  - Run the `linter.py`
    - Do not mix manual edits and linter runs
    - If the linter messes up the text
      - File bugs in `amp` with examples what the linter does incorrectly
  - When a gdoc becomes obsolete or it’s deleted
    - Add a note at the top of a gdoc explaining what happened
      - Example: "Moved to /new_markdown_file.md"
    - Strike out the entire document
    - Move the gdoc to the
      [\_OLD directory](https://drive.google.com/drive/u/0/folders/1J4B1vq8EwT-q_z7qSLCZ9Tug2CA9f8i7)

## Markdown -> Gdocs

- Approach 1:
  - Run
    ```
    > pandoc MyFile.md -f markdown -t odt -s -o MyFile.odt
    ```
  - Download the
    [template](https://docs.google.com/document/d/1Z_OdO6f7VYjimgjfGPofsYHyWvyxXrtOVVcvCauJIpI/edit)
    in odt format
  - Run
    ```
    > pandoc code_organization.md -f markdown -t odt -s -o code_org.odt --reference-doc /Users/saggese/Downloads/Gdoc\ -\ Template.odt
    ```
  - Open it with TextEdit, copy-paste to Gdoc
- Approach 2:
  - Instead of copy-paste the markdown into Gdocs, you can copy the rendered
    markdown in a Gdoc
    - Gdocs does a good job of maintaining the formatting, levels of the
      headers, the links, and so on
- Approach 3:
  - [https://markdownlivepreview.com/](https://markdownlivepreview.com/)
  - TODO(gp): Check if the roundtrip works
