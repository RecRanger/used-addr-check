from pathlib import Path

import backoff
from loguru import logger
import requests

BITCOIN_LIST_URL = "http://alladdresses.loyce.club/all_Bitcoin_addresses_ever_used_sorted.txt.gz"  # noqa


def _get_remote_file_size(url: str) -> int:
    with requests.head(url) as r:
        r.raise_for_status()
        return int(r.headers.get("content-length", 0))


@backoff.on_exception(
    backoff.constant,
    requests.exceptions.RequestException,
    max_tries=30,
    interval=5,
    on_backoff=lambda details: logger.debug(
        f"Transfer failed partway; resuming... (try {details['tries']}/30)"
    ),
)
def _download_file(url: str, dest: Path) -> Path:
    """Download a file from a URL to a local destination, resuming if possible.
    If the file already exists and is fully downloaded, do nothing.
    If the file is partially downloaded, resume the download.
    If the file does not exist, download it from scratch.
    If `dest` is a directory, the file will be saved in that directory.

    Returns the path to the downloaded file.
    """
    total_size = _get_remote_file_size(url)

    if dest.is_dir():
        destination = dest / Path(url).name
    else:
        destination = dest

    # Check how much of the file has already been downloaded
    if destination.exists():
        current_size = destination.stat().st_size
    else:
        current_size = 0

    # If file is already fully downloaded
    if current_size == total_size:
        logger.info("File already downloaded.")
        return destination
    elif current_size > total_size:
        logger.warning(
            f"File size mismatch. Current size: {current_size:,} bytes, "
            f"Total size: {total_size:,} bytes"
        )
        logger.warning("Deleting the file and re-downloading...")
        destination.unlink()
        current_size = 0
    else:
        # File is partially downloaded
        logger.info(
            f"Resuming download at offset={current_size:,}/{total_size:,}"
        )

    # Stream the file from the last byte we have already downloaded
    headers = {"Range": f"bytes={current_size}-"}
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        # Open the file in append mode, create if does not exist
        with open(destination, "ab") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
    return destination


def download_list(destination: Path) -> None:
    logger.info(f"Downloading list from: {BITCOIN_LIST_URL}")
    _download_file(BITCOIN_LIST_URL, destination)
    logger.info(
        f"Downloaded list to: {destination}. "
        f"Size: {destination.stat().st_size:,} bytes."
    )
