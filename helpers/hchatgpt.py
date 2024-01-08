"""
Import as:

import helpers.hchatgpt as hgpt
"""


import sys
import time
import math
import os
import json
import logging
from typing import Dict, List, Optional

import openai

_LOG = logging.getLogger(__name__)

# Do not upload your personal API key to github -- openai will revoke it.
# Set env var in your terminal will be a better approach.
os.environ['OPENAI_API_KEY'] = ""
client = openai.OpenAI()
# The dict of instructions for different scenarios.
# Only files under the given root directory may be uploaded to openai.
prefix_to_root = os.path.join(os.path.dirname(__file__), '..')

# =============================================================================
# Assistant Creation
# =============================================================================

# This method should only be used when a new assistant is needed.
# Otherwise, use the assistant name to retrieve an existing assistant.
def create_assistant(
    assistant_name: str,
    instructions: str,
    *,
    model: str = 'gpt-3.5-turbo-1106',
    use_retrieval: bool = True,
    use_code_interpreter: bool = True,
    use_function: Dict = None
) -> str:
    # Create the assistant
    tools = []
    if use_retrieval:
        tools.append({"type": "retrieval"})
    if use_code_interpreter:
        tools.append({"type": "code_interpreter"})
    if use_function:
        tools.append(use_function)
    assistant = client.beta.assistants.create(
        instructions=instructions,
        name=assistant_name,
        model=model,
        tools=tools,
    )
    return assistant.id

def update_assistant_by_id(
    assistant_id: str,
    *,
    instructions: str = '',
    name: str = '',
    tools: List[Dict[str, str]] = [],
    model: str = '',
    file_ids: List[str] = [],
) ->  str:
    update_config = {
        'instructions': instructions,
        'name': name,
        'tools': tools,
        'model': model,
        'file_ids': file_ids,
    }
    not_empty_params = {k:v for k, v in update_config.items() if v}
    updated_assistant = client.beta.assistants.update(assistant_id, **not_empty_params)
    return updated_assistant.id

def delete_assistant_by_id(assistant_id: str) -> None:
    client.beta.assistants.delete(assistant_id)

def get_all_assistants() -> List[openai.types.beta.assistant.Assistant]:
    list_assistants_response = client.beta.assistants.list(
        order="desc",
        limit="100",
    )
    assistants = list_assistants_response.data
    return assistants

def get_all_assistant_names() -> List[str]:
    assistants = get_all_assistants()
    return [assistant.name for assistant in assistants]

def get_assistant_id_by_name(assistant_name) -> str:
    assistant = None
    assistants = get_all_assistants()
    for cur_assistant in assistants:
        if cur_assistant.name == assistant_name:
            assistant = cur_assistant
    return assistant.id

# =============================================================================
# Create directory structure storing gpt file ids
# =============================================================================

def path_to_dict(path):
    for root, dirs, files in os.walk(path):
        tree = {d: path_to_dict(os.path.join(root, d)) for d in dirs}
        tree.update({f: {"name": f} for f in files})
        return tree

# TODO(Henry): We use fileIO here to store the directory structure, which may not be thread-safe.
#              Should change to use DAO if we have any.
def dump_gpt_ids(dictionary) -> None:
    with open(os.path.join(prefix_to_root, 'gpt_id.json'), 'w') as fp:
        json.dump(dictionary, fp)
        
def load_gpt_ids() -> Optional[Dict]:
    try:
        with open(os.path.join(prefix_to_root, 'gpt_id.json'), 'r') as f_in:
            return json.load(f_in)
    except:
        return None
        
# Will automatically upload file to gpt if not already uploaded.
def _get_gpt_id_file(dictionary, path_from_root) -> Dict[str, str]:
    cur = dictionary
    path_list = path_from_root.split('/')
    for level in path_list:
        cur = cur[level]
    if "gpt_id" not in cur:
        cur["gpt_id"] = _upload_to_gpt_no_set_id(path_from_root)
    dump_gpt_ids(dictionary)
    return cur

def set_gpt_id(path_from_root, gpt_id) -> None:
    gpt_id_dict = load_gpt_ids()
    item = _get_gpt_id_file(gpt_id_dict, path_from_root)
    item["gpt_id"] = gpt_id
    dump_gpt_ids(gpt_id_dict)

# Use `remove_from_gpt` to fully remove a file
def _remove_gpt_id(path_from_root):
    gpt_id_dict = load_gpt_ids()
    item = _get_gpt_id_file(gpt_id_dict, path_from_root)
    if "gpt_id" in item:
        del item["gpt_id"]
    dump_gpt_ids(gpt_id_dict)

def get_gpt_id(path_from_root) -> str:
    gpt_id_dict = load_gpt_ids()
    return _get_gpt_id_file(gpt_id_dict, path_from_root)["gpt_id"]

# =============================================================================
# Upload file to openai account
# =============================================================================

def _upload_to_gpt_no_set_id(path_from_root: str) -> str:
    _LOG.info(f"Uploading file {path_from_root} to chatgpt.")
    upload_file_response = client.files.create(
        # Must use 'rb' regardless of file type
        file=open(os.path.join(prefix_to_root, path_from_root), 'rb'),
        purpose='assistants',
    )
    gpt_id = upload_file_response.id
    return gpt_id

def upload_to_gpt(path_from_root: str) -> str:
    gpt_id = _upload_to_gpt_no_set_id(path_from_root)
    set_gpt_id(path_from_root, gpt_id)
    return gpt_id

def remove_from_gpt(path_from_root: str) -> None:
    gpt_id = get_gpt_id(path_from_root)
    client.files.delete(gpt_id)
    _remove_gpt_id(path_from_root)
    
def get_gpt_file_from_id(gpt_id: str) -> openai.types.file_object.FileObject:
    return client.files.retrieve(gpt_id)

def get_gpt_file_from_path(path_from_root: str) -> openai.types.file_object.FileObject:
    gpt_id = get_gpt_id(path_from_root)
    return get_gpt_file_from_id(gpt_id)

# =============================================================================
# Add/Remove files for an assistant
# =============================================================================
# Note that files for assistant are those that should be constantly used by this assistant (like docs/guidelines).
# For one-time used files, add them to message instead.
# One assistant can have up to 20 files linked to it.

def set_assistant_files_by_name(assistant_name: str, file_path_list: List[str]) -> str:
    assistant_id = get_assistant_id_by_name(assistant_name)
    file_ids = [get_gpt_id(path) for path in file_path_list]
    return update_assistant_by_id(assistant_id, file_ids=file_ids)


def add_files_to_assistant_by_name(assistant_name: str, file_path_list: List[str]) -> str:
    assistant_id = get_assistant_id_by_name(assistant_name)
    assistant_files = client.beta.assistants.files.list(
      assistant_id="asst_abc123"
    ).data
    existing_file_ids = [file.id for file in assistant_files]
    new_file_ids = [get_gpt_id(path) for path in file_path_list]
    file_ids = list(set(existing_file_ids + new_file_ids))
    return update_assistant_by_id(assistant_id, file_ids=file_ids)

# Unlink a file from an assistant.
# This will not delete the file from openai account.
def delete_file_from_assistant_by_id(assistant_id: str, file_id: str) -> None:
    client.beta.assistants.files.delete(
        assistant_id=assistant_id,
        file_id=file_id
    )
    
def delete_file_from_assistant_by_name(assistant_name: str, file_path: str) -> None:
    gpt_id = get_gpt_id(file_path)
    assistant_id = get_assistant_id_by_name(assistant_name)
    delete_file_from_assistant_by_id(assistant_id, gpt_id)

# =============================================================================
# Create Thread and Message from user input
# =============================================================================

def create_thread() -> str:
    message_thread = client.beta.threads.create()
    return message_thread.id

def create_message_on_thread(thread_id: str, content: str, file_ids: List[str]) -> str:
    if not content:
        _LOG.error("Message content must not be empty. This will cause an openAI error.")
    if file_ids:
        message = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=content,
            file_ids=file_ids,
        )
    else:
        message = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=content,
        )
    return message.id
    
def create_message_on_thread_with_file_names(thread_id: str, content: str, file_names: List[str]) -> str:
    if file_names:
        file_ids = [get_gpt_id(file) for file in file_names]
    else:
        file_ids = []
    return create_message_on_thread(thread_id, content, file_ids)

# =============================================================================
# Run thread on certain assistant
# =============================================================================

def run_thread_on_assistant(assistant_id, thread_id, model: str = '') -> str:
    if model:
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id,
            model=model
        )
    else:
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )
    return run.id

def run_thread_on_assistant_by_name(assistant_name, thread_id, model: str = '') -> str:
    assistant_id = get_assistant_id_by_name(assistant_name)
    if model:
        return run_thread_on_assistant(assistant_id, thread_id, model)
    else:
        return run_thread_on_assistant(assistant_id, thread_id)

def wait_for_run_result(thread_id: str, run_id: str, timeout: int = 180):
    finished = False
    _LOG.info("Waiting for chatgpt response...")
    for i in range(math.ceil(timeout/5)):
        _LOG.info(f"{i*5}/{timeout} seconds before timeout.")
        time.sleep(5)
        run = client.beta.threads.runs.retrieve(
            thread_id=thread_id,
            run_id=run_id
        )
        finished = (run.status == "completed")
        if finished:
            break
    if not finished:
        raise TimeoutError("Failed to retrieve response from openai.")
    messages = client.beta.threads.messages.list(
        thread_id=thread_id
    )
    return messages

# =============================================================================
# E2E chatgpt runner
# =============================================================================

def e2e_assistant_runner(
    assistant_name: str,
    user_input: str = '',
    vim_mode: bool = False,
    model: str = '',
    input_file_names: List[str] = [],
    output_file_path: str = ''
) -> str:
    if not assistant_name:
        _LOG.error("No assistant name provided.")
        return None
    directory_dict = load_gpt_ids()
    if not directory_dict:
        directory_dict = path_to_dict(prefix_to_root)
        dump_gpt_ids(directory_dict)
    if vim_mode:
        user_input = ''.join(sys.stdin.readlines())
    thread_id = create_thread()
    create_message_on_thread_with_file_names(
        thread_id, 
        user_input,
        input_file_names
    )
    if model:
        run_id = run_thread_on_assistant_by_name(assistant_name, thread_id, model)
    else:
        run_id = run_thread_on_assistant_by_name(assistant_name, thread_id)
    message = wait_for_run_result(thread_id, run_id)
    output = message.data[0].content[0].text.value
    if vim_mode or not output_file_path:
        sys.stdout.write(output)
    if output_file_path:
        with open(output_file_path, 'w') as fp:
            fp.write(output)
    return output

