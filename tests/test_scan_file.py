from pathlib import Path
import pytest

from used_addr_check.scan_file import (
    _extract_addresses_from_file_python_re,
    _extract_addresses_from_file_ripgrep,
    extract_addresses_from_file,
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"

SAMPLE_INPUT_FILE_1 = TEST_DATA_DIR / "scan_file_sample_1.txt"
SAMPLE_INPUT_FILE_1_EXPECTED = [
    "bc1qdf97u20sav0uxanvgkttmewljvjqh9ljpcmehm",
    "3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd",
    "12PCbUDS4ho7vgSccmixKTHmq9qL2mdSns",
    "1PC9aZC4hNX2rmmrt7uHTfYAS3hRbph4UN",
    "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
]

# test file 2 has random bytes
SAMPLE_INPUT_FILE_2 = TEST_DATA_DIR / "scan_file_sample_2.txt"
SAMPLE_INPUT_FILE_2_EXPECTED = [
    "15ruLg4LeREntByp7Xyzhf5hu2qGn8ta2o",
    "bc1qkd5az2ml7dk5j5h672yhxmhmxe9tuf97j39fm6",
    "1PC9aZC4hNX2rmmrt7uHTfYAS3hRbph4UN",
]


# Combine tests for multiple functions and files
@pytest.mark.parametrize(
    "func, input_file, expected",
    [
        (
            _extract_addresses_from_file_python_re,
            SAMPLE_INPUT_FILE_1,
            SAMPLE_INPUT_FILE_1_EXPECTED,
        ),
        (
            _extract_addresses_from_file_python_re,
            SAMPLE_INPUT_FILE_2,
            SAMPLE_INPUT_FILE_2_EXPECTED,
        ),
        (
            _extract_addresses_from_file_ripgrep,
            SAMPLE_INPUT_FILE_1,
            SAMPLE_INPUT_FILE_1_EXPECTED,
        ),
        (
            _extract_addresses_from_file_ripgrep,
            SAMPLE_INPUT_FILE_2,
            SAMPLE_INPUT_FILE_2_EXPECTED,
        ),
        (
            extract_addresses_from_file,
            SAMPLE_INPUT_FILE_1,
            SAMPLE_INPUT_FILE_1_EXPECTED,
        ),
        (
            extract_addresses_from_file,
            SAMPLE_INPUT_FILE_2,
            SAMPLE_INPUT_FILE_2_EXPECTED,
        ),
    ],
)
def test_extract_addresses_from_file(func, input_file, expected):
    actual = func(input_file)
    assert actual == expected
