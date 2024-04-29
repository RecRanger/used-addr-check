from used_addr_check.download_list import download_list, BITCOIN_LIST_URL

from used_addr_check.index_create import load_or_generate_index
from used_addr_check.index_search import search_multiple_in_file

import argparse
from pathlib import Path


def main_cli():
    # USAGE:
    # program.py search -f all_addr.txt -s SEARCH_QUERY_HERE -s QUERY_2

    parser = argparse.ArgumentParser(
        description="CLI for file processing and searching"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for the 'download' command
    download_parser = subparsers.add_parser(
        "download", help="Download the file"
    )
    download_parser.add_argument(
        "-o",
        "--output",
        dest="output_path",
        required=True,
        help="Output file path (should end in .gz)",
    )
    download_parser.add_argument(
        "-u",
        "--url",
        dest="url",
        default=BITCOIN_LIST_URL,
        help="URL to download the file from",
    )

    # Subparser for the 'index' command
    index_parser = subparsers.add_parser(
        "index", help="index a file, save it to orig_name.txt.index.json"
    )
    index_parser.add_argument(
        "-f",
        "--file",
        dest="file_path",
        required=True,
        help="File to be searched (.txt)",
    )

    # Subparser for the 'search' command
    search_parser = subparsers.add_parser("search", help="Search a file")
    search_parser.add_argument(
        "-f",
        "--file",
        dest="file_path",
        required=True,
        help="File to be searched (.txt)",
    )
    search_parser.add_argument(
        "-s",
        "--search",
        required=True,
        action="append",
        help="Search query(s) to find in the file",
    )

    args = parser.parse_args()

    if args.command == "index":
        load_or_generate_index(
            haystack_file_path=Path(args.file_path), force_recreate=True
        )
    elif args.command == "search":
        search_multiple_in_file(Path(args.file_path), args.search)
    elif args.command == "download":
        download_list(Path(args.output_path))
    else:
        parser.print_help()


if __name__ == "__main__":
    main_cli()
