# Markdown vs Google Docs

## In general

- We prefer to use Markdown for technical documentation
- We use Google for notes from meetings and research

## Markdown pros

- Can use vim
- Can version control
- Easy to use verbatim (e.g., typing `foobar`)
- Easy to style using pandoc
- Easy to embed code
- Easy to add Latex equations
- Easy to grep

## Google Docs pros

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
      ```python
      def hello():
          print("hello")
      ```
  - Auto-latex equations

## Rules of thumb

- Use Markdown
  - If doc is going to be used as a public guideline
  - If doc has mostly text, code, and formulas
  - If there are notes from a book
- Use Gdoc
  - If doc requires a lot of images that cannot be placed as text
  - If doc is a research of an analysis

# Google docs style conventions

## Headings

- We add N (where N is the heading level) `#` before the heading name, e.g.,
  - Heading 1:
    ```markdown
    # Heading 1
    ```
  - Heading 2:
    ```markdown
    ## Heading 2
    ```
- The reason is that sometimes one doesn't have the time or the patience to
  format things properly, so at least there is some indication of the level of
  the titles
- Avoid having multiple `#` separatd by a space that sometimes appear in a
  process of convertion of Gdocs to Markdown files
  - _Bad_
    ```markdown
    # # Heading 1
    ```
  - _Good_
    ```markdown
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

### Using `convert_docx_to_markdown.py`

- This python script converts Docx to Markdown using Pandoc.
- In general, we recommend using this approach

- Pros
  - Removing artifacts with this python script, less manual work
  - Best for a large document
  - Handle figures
- Cons
  - Need to move files

### Process

- Download Google document as `docx`
- Move the file in place
  ```bash
  > FILE_NAME=docs/dataflow/all.best_practice_for_building_dags.explanation
  > mv /Users/saggese/Downloads/Blank.docx $FILE_NAME.docx
  > convert_docx_to_markdown.py --docx_file $FILE_NAME.docx --md_file $FILE_NAME.md
  ```
- Convert it to markdown using `convert_docx_to_markdown.py`
- Usage:
  ```bash
  > convert_docx_to_markdown.py --docx_file Tools_Docker.docx --md_file Tools_Docker.md
  ```
  - This command should be run directly under the target output directory for
    the Markdown file, in order to generate correct image links. Otherwise,
    you'll need to manually fix the image links.
  - File names can't contain any spaces. Therefore, use underscores `_` to
    replace any spaces.

### Cleaning up converted markdown

- Fix some formatting manually before running the Markdown linter.
  - Read through [Style and cosmetic lints](#style-and-cosmetic-lints) for
    Markdown formatting and fix some formatting based on the rules.
    - Summary
      - Add the following tag at the top of the markdown file below the document
        title:
        ```markdown
        <!-- toc -->
        ```
      - Use bullet lists to organize the whole Markdown for consistency with
        other docs. See
        [all.coding_style.how_to_guide.md](https://github.com/cryptokaizen/cmamp/blob/master/docs/coding/all.coding_style.how_to_guide.md)
        or any other published Markdown format as reference
      - Add missing ``` around code blocks. These could be missing in the
        original Google doc. Also adjust code block indentations if needed
      - The generated markdown may convert http links as `html` `<span>`
        objects. This hinders the readability of the `md` file. In this case,
        manually convert to a standard `http://` link:
        - `[<span class="underline">https://www.sorrentum.org/</span>](https://www.sorrentum.org/)`
          -> `https://www.sorrentum.org/`
      - Replace the `html` `<img>` tag with a markdown link:
        - `<img src="docs/work_tools/figs/visual_studio_code/image1.png"/>` ->
          `![alt_text](docs/work_tools/figs/visual_studio_code/image1.png")`

- Remove empty lines manually
  ```markdown
  :'<,'>! perl -ne 'print if /\S/'
  ```
- Run the `lint_md.sh`
  - Usage:
    ```bash
    > dev_scripts/lint_md.sh docs/documentation_meta/all.writing_docs.how_to_guide.md
    ```
  - What the linter will do:
    - Build TOC automatically
    - Adjust the indentation to improve the Markdown's format (but the
      precondition is that you have properly adjusted the indentation levels).
    - Remove extra empty lines under headings
    - Adjust text layout
  - Do not mix manual edits and linter runs
  - If the linter messes up the text
    - File bugs in `amp` with examples what the linter does incorrectly
- Last steps
  - Compare the generated markdown file with the original Gdoc from top to
    bottom to ensure accurate rendering.
  - Review the markdown file on GitHub to make sure it looks good, as it may
    slightly differ from the preview in your local markdown editor
- When a gdoc becomes obsolete or it's deleted
  - Add a note at the top of a gdoc explaining what happened
  - Example: "Moved to /new_markdown_file.md"
  - Strike out the entire document
  - Move the gdoc to the
    [\_OLD directory](https://drive.google.com/drive/u/0/folders/1J4B1vq8EwT-q_z7qSLCZ9Tug2CA9f8i7)

### Other approaches

- Best for a large document
- Approach 1 - Chrome Docs to Markdown extension:
  - Use the [Docs to Markdown](https://github.com/evbacher/gd2md-html/wiki)
    extension
    - Install
      [the extension](https://gsuite.google.com/marketplace/app/docs_to_markdown/700168918607)
      from the G Suite marketplace
    - [User guide](https://github.com/evbacher/gd2md-html/wiki#using-docs-to-markdown)
      for the extension
  - One needs to accept/reject all suggestions in a gdoc as the extension works
    poorly when a document is edited in the suggestion mode
- Approach 2 - Online converter:
  - [Google-docs-to-markdown/](https://mr0grog.github.io/google-docs-to-markdown/)

- Also need to go through
  [Cleaning up converted markdown](#cleaning-up-converted-markdown)
- You might need to remove artifacts manually

## Markdown -> Gdocs

- Approach 1:
  - Run
    ```bash
    > pandoc MyFile.md -f markdown -t odt -s -o MyFile.odt
    ```
  - Download the
    [template](https://docs.google.com/document/d/1Z_OdO6f7VYjimgjfGPofsYHyWvyxXrtOVVcvCauJIpI/edit)
    in odt format
  - Run
    ```bash
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
