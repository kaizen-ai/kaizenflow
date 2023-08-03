# Codebase Clean up Scripts

<!-- toc -->

- [Using the script approach](#using-the-script-approach)
- [Notes on `replace_text.py`](#notes-on-replace_textpy)
- [Example usage of `replace_text.py`](#example-usage-of-replace_textpy)

<!-- tocstop -->

## Using the script approach

- We want to apply changes to the code base with a script

  - Ideally we would like to apply all the changes automatically through the
    script
  - E.g.: in [SorrTask258](https://github.com/sorrentum/sorrentum/issues/258) a
    script that replaces `pytest.raises` with `self.assertRaises` everywhere in
    the code

- We are ok to make the vast majority (like 95%) of the changes automatically,
  and the rest manually

- We want to keep together in a single PR

  - the script performing automatically the changes and
  - the manual changes from the outcome of the script

- We don't want to check in the PR, the outcome of running the script

  - We can create a separate PR to communicate with the reviewers the output of
    running the script

- The reviewer should run the script on all the repos, run the unit tests, and
  merge (through a PR as usual)

- Typically we use
  [replace_text.py](https://github.com/sorrentum/sorrentum/blob/master/dev_scripts/replace_text.py)
  and wrap it up in a `sh` script

- For more complex problems we write a custom regex-based script

## Notes on `replace_text.py`

- Replace an instance of text in all:

  - `.py` file contents

  - `.ipynb` file contents

  - `.txt` file contents

  - filenames

- `--old` and `--new` arguments are passed as regular expressions or strings
  that should replace each other

- `--preview` in order to see script result without making actual changes

- `--only_dirs` - Provide space-separated list of directories to process only

- `--only_files` - Provide space-separated list of files to process only

- `--exclude_files` - Provide space-separated list of files to exclude from
  replacements

- `--exclude_dirs` - Provide space-separated list of dir to exclude from
  replacements

- `--ext` to process files with specific extentions. Defaults are
  `py,ipynb,txt,sh`. Use `_all_` for all files.

## Example usage of `replace_text.py`

- See [SorrIssue259](https://github.com/sorrentum/sorrentum/issues/259) and the
  related [PR](https://github.com/cryptokaizen/cmamp/pull/4721/) for reference
- We wanted to make `_to_multiline_cmd()` from `helpers/lib_tasks_utils.py` a
  public function

- This would require to rename `_to_multiline_cmd()` to `to_multiline_cmd()`
  with the script

- This
  [script](https://github.com/cryptokaizen/cmamp/blob/master/dev_scripts/cleanup_scripts/SorrTask259_Make_to_multi_line_cmd_public.sh)
  will make the replacement smoothly everywhere in the code except for the dirs
  specified `--exclude_dirs` flag.

- Another example
  [script](https://github.com/sorrentum/sorrentum/blob/master/dev_scripts/cleanup_scripts/SorrTask258_Replace_pytest_raises_with_self_assertraises.sh)
  to replace `pytest.raises` with `self.assertRaises`

  - Note the use of `--ext` flag to specify the file extentions the script
    should work on

- Of course the changes need to be applied in one repo and then propagated to
  all the other repos if the tests are successful
