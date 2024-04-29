from used_addr_check.download_list import download_list, BITCOIN_LIST_URL
from used_addr_check.make_optimized_file import ingest_raw_list_file
from used_addr_check.search_optimized_file import search_file
from used_addr_check.optimized_file import hash_algo_literal_t

import argparse
from pathlib import Path
from typing import get_args


def main_cli():
    # USAGE:
    # program.py ingest -i source_file.gz -o optimized_file.dat
    # program.py search -f optimized_file.dat -s SEARCH_QUERY_HERE

    parser = argparse.ArgumentParser(
        description="CLI for file processing and searching"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for the download command
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

    # Subparser for the ingest command
    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest and optimize a file"
    )
    ingest_parser.add_argument(
        "-i",
        "--input",
        dest="input_path",
        required=True,
        help="Input file path",
    )
    ingest_parser.add_argument(
        "-o",
        "--output",
        dest="output_path",
        required=True,
        help="Output file path",
    )
    ingest_parser.add_argument(
        "-a",
        "--hash-algo",
        dest="hash_algo",
        choices=get_args(hash_algo_literal_t),
        default="xxhash32",
        help="Hash algorithm to use. xxhash is faster, but results in more false-positives. md5 is slower, but more accurate.",  # noqa
    )

    # Subparser for the search command
    search_parser = subparsers.add_parser("search", help="Search a file")
    search_parser.add_argument(
        "-f",
        "--file",
        dest="file_path",
        required=True,
        help="File to be searched",
    )
    search_parser.add_argument(
        "-s",
        "--search",
        required=True,
        action="append",
        help="Search query(s) to find in the file",
    )

    args = parser.parse_args()

    if args.command == "ingest":
        ingest_raw_list_file(
            gzip_file_path=Path(args.input_path),
            output_path=Path(args.output_path),
            hash_algo=args.hash_algo,
        )
    elif args.command == "search":
        search_file(Path(args.file_path), args.search)
    elif args.command == "download":
        download_list(Path(args.output_path))
    else:
        parser.print_help()


if __name__ == "__main__":
    main_cli()
