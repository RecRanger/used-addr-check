from used_addr_check.make_optimized_file import ingest_raw_list_file
from used_addr_check.search_optimized_file import search_file

from pathlib import Path
import argparse


def main_cli():
    # USAGE:
    # program.py ingest -i source_file.gz -o optimized_file.dat
    # program.py search -f optimized_file.dat -s SEARCH_QUERY_HERE

    parser = argparse.ArgumentParser(
        description="CLI for file processing and searching"
    )
    subparsers = parser.add_subparsers(dest="command")

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
        ingest_raw_list_file(Path(args.input_path), Path(args.output_path))
    elif args.command == "search":
        search_file(Path(args.file_path), args.search)
    else:
        parser.print_help()


if __name__ == "__main__":
    main_cli()
