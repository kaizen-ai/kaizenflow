# Writing Docs

<!-- toc -->
<!-- tocstop -->

## Conventions

### Make no assumptions on the user's knowledge

- Nothing is obvious to somebody who doesn't know

### Verify that things worked

- Add ways to verify if a described process worked
  - E.g., "do this and that, if this and that is correct should see this"
- Have a trouble-shooting procedure
  - One approach is to always start from scratch

### Always use the linter

- Most cosmetic lints described further can be taken care automatically by our
  markdown linter, so make sure to run it after implementing the changes
- Use `i lint`, since `dev_scripts/lint_md.sh` is discontinued
- Do not mix manual edits and linter runs. Best practice is to run the linter
  and commit the changes it made as separate commit
- If the linter messes up the text, file an issue with examples of what the
  linter does incorrectly

### Add a table of content

- Unfortunately both markdown and GitHub don't support automatically generating
  a TOC for a document
- To generate a table of content add the following tag at the top of the
  markdown file:
  ```markdown
  <!-- toc -->
  ```
  - Run `i lint` to build the TOC automatically

##

- Make sure the headings structure contains exactly one level 1 heading
  (`# This one`)
  - This is important for displaying MkDocs documentation correctly via browser

## Use 80 columns formatting for md files

- Our markdown linter takes care of reflowing the text
- Vim has a `:gq` command to reflow the comments
- There are plugins for PyCharm and VisualStudio

### Use good vs bad

- Make examples of "good" ways of doing something and contrast them with "bad"
  ways

  **_Good_**

  ```markdown
  ...
  ```

  **_Bad_**

  ```markdown
  ...
  ```

### Use an empty line after heading

- Leave an empty line after a heading to make it more visible, e.g.,

  **Good**

  ```markdown
  # Very important title
  - Less important text
  ```

  **Bad**

  ```markdown
  # Coming through! I've big important things to do!
  - ... and his big important wheels got STUCK!
  ```

- Our linter automatically takes care of this

### Bullet lists

- We like using bullet list since they represent the thought process, force
  people to focus on short sentences (instead of rambling wall-of-text), and
  relation between sentences
- E.g.,
  ```markdown
  - This is thought #1
    - This is related to thought #1
  - This is thought #2
    - Well, that was cool!
    - But this is even better
  ```
- We use `-` instead of `*` or circles
- The linter automatically enforces this

### Use the right syntax highlighting

- When using a block of code use the write syntax highlighting
  - Code (```python)
  - Dirs (e.g.,` /home/users`)
  - Command lines (e.g., `> git push` or `docker> pytest`)
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
  - Markdown
    ```markdown
    ...
    ```
  - Nothing
    ```verbatim
    ....
    ```

### Indent `code` style

- GitHub / Pandoc seems to render incorrectly a code block unless it's indented
  over the previous line

### Embed screenshots only when strictly necessary

- Avoid to use screenshots whenever possible and use copy-paste of text with the
  right highlighting
- However, sometimes we need to use screenshots (e.g., plots, website interface)

### Improve your written English

- Use English spell-checker, but unfortunately this is not enough
- Type somewhere where you can use several choices:
  - [Grammarly](https://www.grammarly.com/)
  - ChatGPT
  - [LanguageTool](https://www.languagetool.org/)
- This is super-useful to improve your English since you see the error and the
  correction
  - Otherwise you will keep making the same mistakes forever

### Make sure your markdown looks good

- Compare your markdown with other already published

- You can:
  - Check in the code a branch and use GitHub to render it
  - Use Pycharm to edit, which also renders it side-by-side

### Do not overcapitalize headings

- Paragraph titles should be like `Data schema` not `Data Schema`

### Update the `Last review` tag

- When you read/refresh a file update the last line of the text
  ```verbatim
  Last review: GP on 2024-04-20, Paul on 2024-03-10
  ```

### Comment the code structure

- When you want to describe and comment the code structure do something like
  this
  ```
  > tree.sh -p data_schema
  data_schema/
  |-- dataset_schema_versions/
  |   `-- dataset_schema_v3.json
    Description of the current schema
  |-- test/
  |   |-- __init__.py
  |   `-- test_dataset_schema_utils.py
  |-- __init__.py
  |-- changelog.txt
    Changelog for dataset schema updates
  |-- dataset_schema_utils.py
    Utilities to parse schema
  `-- validate_dataset_signature.py*
    Script to test a schema
  ```

### Convention for file names

- Each file name should have a format like
  `docs/{component}/{audience}.{topic}.{diataxis_tag}.md`
  - E.g., `docs/documentation_meta/all.diataxis.explanation.md`
- Where
  - `component` is one of the software components (e.g., `datapull`, `dataflow`)
  - `audience` is the target audience (e.g., `all`, `ck`)
  - `topic` is the topic of the file
  - An how to guide should have a verb-object format
    - E.g., `docs/oms/broker/ck.generate_broker_test_data.how_to_guide.md`
  - A reference often has just a name
    - E.g., `docs/oms/broker/ck.binance_terms.reference.md`

// From https://opensource.com/article/20/3/documentation

### Use active voice

- Use the active voice most of th time and use the passive voice sparingly
- Active voice is shorter than passive voice
- Readers convert passive voice to active voice

**Good**

- You can change these configuration by ...

**Bad**

- There configurations can be changed by ...

### Use simple short sentences

- Use Grammarly/ChatGPT

### Format for easy reading

- Use headings, bullet points, and links to break up information into chunks
  instead of long explanatory paragraphs

### Keep it visual

- Use tables and diagrams, together with text, whenever possible

### Mind your spelling

- Always, always, always spell check for typos and grammar check
- Use Grammarly/ChatGPT

### Be efficient

- Nobody wants to read meandering paragraphs in documentation
- Engineers want to get technical information as efficiently as possible
- Do not add "fluff"
- Do not explain things in a repetitive way

### Do not add fluff

- Always point to documentation on the web instead of summarizing it
- If you want to summarize some doc (e.g., so that people don't have to read too
  much) add it to a different document instead of mixing with our documentation
- Focus on how we do, why we do, rather than writing AI-generated essays

## Resources

- [https://opensource.com/article/20/3/documentation]
- [Markdown cheatsheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
- [Google guide to Markdown](https://github.com/google/styleguide/blob/gh-pages/docguide/style.md)
  - TODO(gp): Make sure it's compatible with our linter
