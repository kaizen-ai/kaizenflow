
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hchatgpt as hgpt
import helpers.hparser as hparser
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Use chatGPT to process a file or certain text.', formatter_class=argparse.RawDescriptionHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--list', dest='list', action='store_true',
                    help='show all currently available assistants and exit')
    group.add_argument('-n', '--assistant_name', dest='assistant_name',
                    help='name of the assistant to be used')
    parser.add_argument('-f', '--input_files', dest='input_file_paths', action='extend', nargs='*',
                    help='files needed in this run, use relative path from project root')
    parser.add_argument('-m', '--model', dest='model',
                    help='use specific model for this run, overriding existing assistant config')
    parser.add_argument('-o', '--output_file', dest='output_file',
                    help='redirect the output to the given file')
    parser.add_argument('-i', '--input_text', dest='input_text',
                    help='take a text input from command line as detailed instruction')
    parser.add_argument('--vim', dest='vim_mode', action='store_true',
                    help='disable -i (but not -o), take input from stdin and output to stdout forcely')
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    if args.list:
        print(hprint.format_list(hgpt.get_all_assistant_names()))
        return
    model = args.model
    assistant_name = args.assistant_name
    user_input = args.input_text
    input_file_paths = args.input_file_paths
    output_file = args.output_file
    vim_mode = args.vim_mode
    hgpt.e2e_assistant_runner(
        assistant_name,
        user_input=user_input,
        model=model,
        vim_mode=vim_mode,
        input_file_names=input_file_paths,
        output_file_path=output_file
    )
    


if __name__ == "__main__":
    _main(_parse())