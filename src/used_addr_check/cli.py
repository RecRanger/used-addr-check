# from used_addr_check.download_list import download_list, BITCOIN_LIST_URL
from used_addr_check import __VERSION__
from used_addr_check.index_create import load_or_generate_index
from used_addr_check.index_search import search_multiple_in_file
from used_addr_check.scan_file import scan_file_for_used_addresses

import argparse
import sys
from pathlib import Path


def main_cli():
    parser = argparse.ArgumentParser(
        description="CLI for file processing and searching"
    )
    parser.add_argument(
        "-V",
        "--version",
        dest="version",
        action="store_true",
        help="Print version to stdout and exit",
    )
    subparsers = parser.add_subparsers(dest="command")

    # # Subparser for the 'download' command
    # download_parser = subparsers.add_parser(
    #     "download", help="Download the file"
    # )
    # download_parser.add_argument(
    #     "-o",
    #     "--output",
    #     dest="output_path",
    #     required=True,
    #     help="Output file path (should end in .gz)",
    # )
    # download_parser.add_argument(
    #     "-u",
    #     "--url",
    #     dest="url",
    #     default=BITCOIN_LIST_URL,
    #     help="URL to download the file from",
    # )

    # Subparser for the 'version' command (subparser not really used)
    subparsers.add_parser(
        "version",
        help="Print version to stdout and exit",
    )

    # Subparser for the 'index' command
    index_parser = subparsers.add_parser(
        "index",
        help="Index a haystack 'used addresses' file, save it to orig_name.txt.index.json",  # noqa
    )
    index_parser.add_argument(
        "-f",
        "--haystack",
        dest="haystack_file_path",
        required=True,
        help="Haystack address list file path (.txt)",
    )

    # Subparser for the 'search' command
    search_parser = subparsers.add_parser("search", help="Search a file")
    search_parser.add_argument(
        "-f",
        "--haystack",
        dest="haystack_file_path",
        required=True,
        help="Haystack address list file path (.txt)",
    )
    search_parser.add_argument(
        "-n",
        "--needle",
        dest="needles",
        required=True,
        action="append",
        help="Search query(s) to find in the file",
    )

    # Subparser for the 'scan_file' command
    scan_file_parser = subparsers.add_parser(
        "scan_file",
        help="Scan a file for bitcoin addresses, and see which ones have been used.",  # noqa
    )
    scan_file_parser.add_argument(
        "-f",
        "--haystack",
        dest="haystack_file_path",
        required=True,
        help="Haystack address list file path (.txt)",
    )
    scan_file_parser.add_argument(
        "-n",
        "--needle",
        dest="needle_haystack_file_path",
        required=True,
        help="Needle file path, with list of addresses. Addresses will be "
        "extracted from this file",
    )

    args = parser.parse_args()

    if args.command == "version" or args.version:
        print(f"used_addr_scan version v{__VERSION__}")
        sys.exit(0)
    elif args.command == "index":
        load_or_generate_index(
            haystack_file_path=Path(args.haystack_file_path),
            force_recreate=True,
        )
    elif args.command == "search":
        search_multiple_in_file(Path(args.haystack_file_path), args.needles)
    # elif args.command == "download":
    #     download_list(Path(args.output_path))
    elif args.command == "scan_file":
        scan_file_for_used_addresses(
            Path(args.haystack_file_path),
            Path(args.needle_haystack_file_path),
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main_cli()
