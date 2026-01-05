# used_addr_check (Python)
A tool to efficiently check if a Bitcoin Address has ever been used before

## Description

Based on [loyce.club's "all bitcoin addresses ever used" list](http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz), this library and CLI tool can search the list very, very fast and efficiently.

## Features

* Lookup either a single address, or a long list of potential addresses.
* CLI and library options
* Generates a binary search tree index from the text file, and then uses that to search for the address.

## Getting Started

This project depends on the large address list being downloaded and extracted.

```bash
# Download the zipped text file of used addresses, where each line is a used Bitcoin address.
wget http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz

# Extract the file
gunzip -d ./all_Bitcoin_addresses_ever_used_sorted.txt.gz --stdout | pv > all_Bitcoin_addresses_ever_used_sorted.txt
```

Optionally, if you intend to use the `scan_file` subcommand, install `ripgrep`
from optimal performance:

```bash
# first, install cargo/rust
# then, run:
cargo install ripgrep
```


## Usage - CLI

```bash
uv tool install used_addr_check

# download and extract the required file:
wget http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz
gunzip -d ./all_Bitcoin_addresses_ever_used_sorted.txt.gz --stdout | pv > addr_list.txt

# generate the index file (optional):
used_addr_check index -f ./addr_list.txt
# the index file is now at: ./addr_list.index.parquet

# search a couple of addresses:
used_addr_check search -f ./addr_list.txt -s moW9o415jNfgyuzytEMZD84Kovri5DJ64e -s mncqTEYTidNdbqGZnXTd1JFYRrruuh5StV

# search for a long list of addresses (extracted by regex):
used_addr_check scan_file -f ./addr_list.txt -n file_with_addresses_to_lookup.txt
```

## Usage - Library

This example will generate the index file, if required.

```python
from used_addr_check import search_multiple_in_file

needles = [
    'moW9o415jNfgyuzytEMZD84Kovri5DJ64e',
    'mncqTEYTidNdbqGZnXTd1JFYRrruuh5StV'
]
haystack_file_path = './addr_list.txt'

addresses_found_list: List[str] = search_multiple_in_file(
    haystack_file_path=haystack_file_path,
    needles=needled,
)
print(f"{addresses_found_list=}")
```

## Performance Notes

* With the default indexing size of one index entry per 1000 addresses in the "haystack" file, the index is a 140MB Parquet file.
* On a 2023 mid-range laptop with an SSD:
    * Indexing takes 4 minutes.
    * Addresses can be searched at 20 query addresses ("needles") per sec.

There are certainly opportunities for further improvement, but this performance is adequate.

## Contributing

Please Star this repo if it's helpful.

Please open GitHub Issues and Pull Requests with features/bugs/fixes.

## Future Features

* Optionally disable `loguru` logging in subfunctions
* Convert "found address" result to an iterator.
* Test cases.
