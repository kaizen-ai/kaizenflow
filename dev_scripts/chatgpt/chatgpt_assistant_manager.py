import argparse
import logging

import helpers.hchatgpt as hchatgp
import helpers.hchatgpt_instructions as hchainst
import helpers.hdbg as hdbg
import helpers.hparser as hparser

instructions = hchainst.instructions

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Use chatGPT to process a file or certain text.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c",
        "--create",
        dest="create_name",
        help="name of the assistant to be created",
    )
    group.add_argument(
        "-e",
        "--edit",
        dest="edit_name",
        help="name of the assistant to be edited",
    )
    group.add_argument(
        "-d",
        "--delete",
        dest="delete_name",
        help="name of the assistant to be deleted, will ignore all other arguments",
    )
    parser.add_argument(
        "-n",
        "--new_name",
        dest="new_name",
        help="new name for the assistant, only used in -e",
    )
    parser.add_argument(
        "-m", "--model", dest="model", help="model used by the assistant"
    )
    parser.add_argument(
        "-i",
        "--instruction_name",
        dest="instruction_name",
        help="name of the instruction for the assistant, as shown in helpers.hchatgpt_instructions",
    )
    parser.add_argument(
        "-f",
        "--input_files",
        dest="input_file_paths",
        action="extend",
        nargs="*",
        help="files needed for the assistant, use relative path from project root",
    )
    parser.add_argument(
        "--r",
        "--retrieval_tool",
        dest="retrieval_tool",
        action=argparse.BooleanOptionalAction,
        help="enable the retrieval tool. Use --no-r to disable",
    )
    parser.add_argument(
        "--c",
        "--code_tool",
        dest="code_tool",
        action=argparse.BooleanOptionalAction,
        help="enable the code_interpreter tool. Use --no-c to disable",
    )
    parser.add_argument(
        "--function",
        dest="function",
        action="store",
        help="apply certain function tool to the assistant, not implemented yet",
    )

    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    if args.create_name:
        # TODO(Henry): Implement the function calling tool in openai API.
        hchatgp.create_assistant(
            assistant_name=args.create_name,
            instructions=instructions[args.instruction_name],
            model=args.model,
            use_retrieval=args.retrieval_tool,
            use_code_interpreter=args.code_tool,
        )
        hchatgp.add_files_to_assistant_by_name(
            args.create_name, args.input_file_paths
        )
    elif args.edit_name:
        assistant_id = hchatgp.get_assistant_id_by_name(args.edit_name)
        tools = []
        if args.retrieval_tool:
            tools.append({"type": "retrieval"})
        if args.code_tool:
            tools.append({"type": "code_interpreter"})
        hchatgp.update_assistant_by_id(
            assistant_id=assistant_id,
            instructions=instructions[args.instruction_name],
            name=args.new_name,
            tools=tools,
            model=args.model,
            file_ids=[hchatgp.get_gpt_id(path) for path in args.input_file_paths],
        )
    elif args.delete_name:
        assistant_id = hchatgp.get_assistant_id_by_name(args.delete_name)
        hchatgp.delete_assistant_by_id(assistant_id)


if __name__ == "__main__":
    _main(_parse())
