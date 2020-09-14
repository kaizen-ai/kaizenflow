<!--ts-->
   * [How to convert a google doc to Markdown](#how-to-convert-a-google-doc-to-markdown)



<!--te-->
# How to convert a google doc to Markdown

- Use the
  [Docs to Markdown](https://github.com/evbacher/gd2md-html/wiki#using-docs-to-markdown)
  extension
  - Install
    [the extension](https://gsuite.google.com/marketplace/app/docs_to_markdown/700168918607)
    from the G Suite marketplace
  - [User guide](https://gsuite.google.com/marketplace/app/docs_to_markdown/700168918607)
    for the extension
- One needs to accept/reject all suggestions in a gdoc as the extension works
  poorly when a document is edited in the suggestion mode
- Replace all bullet points as `*` with `-`
- Run the `linter.py`
  - Do not mix manual edits and linter runs
  - If the linter messes up the text
    - File bugs in `amp` with examples what the linter does incorrectly
- When a gdoc becomes obsolete
  - Add a note at the top of a gdoc explaining what happened
    - Example: "Moved to /new_markdown_file.md"
  - Strike out the entire document
  - Move the gdoc to the
    [\_OLD directory](https://drive.google.com/drive/u/0/folders/1J4B1vq8EwT-q_z7qSLCZ9Tug2CA9f8i7)
