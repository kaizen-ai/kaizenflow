<!--toc-->
   * [OpenAI Assistant Runner &amp; Manager](#openai-assistant-runner--manager)
      * [What is OpenAI Assistant](#what-is-openai-assistant)
      * [Why using Assistant](#why-using-assistant)
      * [General pattern](#general-pattern)
      * [How to use](#how-to-use)
         * [Assistant Manager](#assistant-manager)
         * [Assistant Runner](#assistant-runner)
      * [API Wrapper Overview](#api-wrapper-overview)
      * [Usage](#usage)
         * [File Structure](#file-structure)
         * [Uploading and Retrieving Files](#uploading-and-retrieving-files)
         * [Managing Assistants](#managing-assistants)
         * [ChatGPT Communication](#chatgpt-communication)
         * [E2E Assistant Runner](#e2e-assistant-runner)


<!-- tocstop -->

# OpenAI Assistant Runner & Manager

## What is OpenAI Assistant

* Always refer to OpenAI official document if anything is unclear:
  https://platform.openai.com/docs/assistants/overview
* An assistant is pretty much similar to a "GPTs", which is a modified GPT
  that has mastered some knowledge and will always be able to use them

## Why using Assistant

* Normally we "chat" with ChatGPT, that means anything we send to it is
  treated as input
* But GPT forget things really fast -- It will forgot what you previously
  said (or files uploaded), as the total length of the conversation grows
* By creating an Assistant, we build a new "instance" of ChatGPT and can
  give it some knowledge to learn
* These knowledge can be with many formats, and up to 20 files (512MB each)
  at a time
* With an instruction string, we define its behavior about how it should
  make use of those knowledge
* When talking to an assistant, we can still add files in the message
* These files does not count towards its 20 files' knowledge limit, as they
  are considered as input, and will be forgotten eventually

## General pattern

* Creation:
* Send some `guideline or example files` for one type of task to the
  assistant itself as knowledge
* Send an instruction about how it should deal with tasks using those
  knowledge
* Running:
* Whenever a specific task comes, send `the task and its data files` in the
  message as input
* Let the assistant solve the task based on the knowledge it learned
* It will forgot everything in this task, and be ready for the next task
  like this one never happens
* Chatting:
* You can continue the conversation if you are not satisified with its reply
* Chatting is not yet implemented in our code, since command line scripts
  cannot save conversations.

## How to use

* Each API key can be bound to an OpenAI Organization. A key belongs to our
  Org is needed to start
* Assistants are organization-wide, an assistant created under our Org can
  be accessed by any API key that belongs to our Org
* Play safely: do not call update/delete methods if it's not necessary

### Assistant Manager

* `dev_scripts/chatgpt_assistant_manager.py -h`
* Use this script to create, modify, or delete an assistant in our Org
* A set of instructions are in `helpers/hchatgpt_instructions.py`
* Feel free to add more instructions for different tasks here
* For better understanding, the name of an assistant created should be
  related to its instruction name

  ```bash
  # Creating a doc writer assistant, give it some docs as example.
  python dev_scripts/chatgpt/chatgpt_assistant_manager.py \
  -c DocWriter-1 \ # Create with name=DocWriter-1
  -m "gpt-4-1106-preview" \
  -i DocWriter \ # Use instruction=DocWriter. See `helpers/hchatgpt_instructions.py`
  -f docs/work_tools/all.bfg_repo_cleaner.how_to_guide.md docs/marketing/dropcontact.how_to_guide.md docs/coding/all.hplayback.how_to_guide.md \
  --r --c
  ```

### Assistant Runner

* `dev_scripts/chatgpt_assistant_runner.py -h`
* Use this script to run an assistant
* `dev_scripts/chatgpt_assistant_runner.py -l` will show all the available
  assistants in our Org.
* Refer to `helpers/hchatgpt_instructions.py` to see how they are instructed

  ```bash
  python dev_scripts/chatgpt/chatgpt_assistant_runner.py \
  -n MarkdownLinter \ # Use the assistant "MarkdownLinter"
  -f dev_scripts/chatgpt/example_data/corrupted_dropcontact.how_to_guide.md \ # Give this corrupted markdown file
  -o dev_scripts/chatgpt/example_data/gpt_linted_dropcontact.how_to_guide.md # Redirect its output to this file
  ```

## API Wrapper Overview

* `helpers/hchatgpt.py` provides methods that warp and interact with OpenAI
  API.
* By using these methods, we can easily build an assistant and chat to it
  with our files.

Key functionalities include:

- Uploading and removing files from OpenAI
- Adding and removing files for an assistant
- Creating threads and messages for user input
- Running threads with certain assistants
- End-to-end (E2E) communication method between users and the assistant

## Usage

The following snippets provide a basic overview of the code usage.

### File Structure

* Since OpenAI File manager does not hold folder structure, we use a cache
  dictionary to save the relation between our file (with folder) and OpenAI File
  IDs
* This dictionary will be constantly accessed and saved back to
  `project_root/gpt_id.json`
* If you find anything buggy, try deleting this cache file and rerun the
  code -- It will be auto generated to a uncorrupted version

### Uploading and Retrieving Files

To upload a file to OpenAI, which you can later attach to messages/assistants:

```python
file_id = upload_to_gpt('path_to_your_file')
```

If you want to retrieve a file that has been uploaded to OpenAI by its path or
ID:

```python
file_object = get_gpt_file_from_path('path_to_your_file')
# or
file_object = get_gpt_file_from_id(file_id)
```

### Managing Assistants

You can specify files an assistant should constantly use (like guidelines):

```python
set_assistant_files_by_name('assistant_name', ['file_path_1', 'file_path_2'])
```

Add or remove specific files to/from an existing assistant by file paths or IDs:

```python
add_files_to_assistant_by_name('assistant_name', ['new_file_path'])
delete_file_from_assistant_by_name('assistant_name', 'file_path_to_remove')
```

### ChatGPT Communication

Create a thread and send a message, with or without attaching files:

```python
thread_id = create_thread()
message_id = create_message_on_thread_with_file_names(thread_id, 'Your message content', ['file_name_1'])
```

Run a thread on an assistant to get the assistant's response:

```python
run_id = run_thread_on_assistant_by_name('assistant_name', thread_id)
response_messages = wait_for_run_result(thread_id, run_id)
```

### E2E Assistant Runner

Interact with an assistant conveniently with the `e2e_assistant_runner`
function. This function can take user input, send it to the assistant, and
manage file attachments in one call:

```python
response = e2e_assistant_runner('assistant_name', 'Your question or statement here', input_file_names=['file_name_1'])
print(response) # Outputs the assistant's response
```
