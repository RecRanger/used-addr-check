# used_addr_check
A tool to efficiently check if a Bitcoin Address has ever been used before

## Description

Based on [loyce.club's "all bitcoin addresses ever used" list](http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz), this library and CLI tool can search the list very, very fast and efficiently.


### Process

1. Run `wget http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz`
    * Downloads a zipped text file, where is line is a Bitcoin address.
2. `ingest_raw_list_file(...)` converts the downloaded `.gz` file to a binary file of just the 64-bit file hashes.
3. In `search_file`, using the `bisect` library, the file is searched for the search query.


## CLI Usage

```bash
pip install used_addr_check

wget http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz

used_addr_check ingest -i ./all_Bitcoin_*.txt.gz -o ./btc_optimized.dat

used_addr_check search -f ./btc_optimized.dat -s moW9o415jNfgyuzytEMZD84Kovri5DJ64e -s mncqTEYTidNdbqGZnXTd1JFYRrruuh5StV
```
