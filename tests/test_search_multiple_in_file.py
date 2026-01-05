import random
import uuid
from pathlib import Path

import pytest

from used_addr_check.index_search import search_multiple_in_file


def test_search_multiple_in_file_small(tmp_path: Path) -> None:
    """Full integration test with a small, readable haystack."""

    haystack = tmp_path / "haystack.txt"
    haystack.write_text(
        "\n".join(["alpha123", "beta456", "gamma789", "delta000"]) + "\n",  # noqa: FLY002
        encoding="utf-8",
    )

    needles = ["alpha123", "gamma789", "missing999"]

    found = search_multiple_in_file(haystack, needles)

    assert found == ["alpha123", "gamma789"]


@pytest.mark.parametrize(
    "haystack_file_size",
    [
        int(1e6),  # 1 MB
        int(1e9),  # 1 GB
    ],
)
def test_search_multiple_in_file_generated(
    tmp_path: Path, haystack_file_size: int
) -> None:
    """Full integration test using a generated newline-delimited text file.

    This test validates indexing, disk I/O, and searching at scale.
    """
    target_needle_count = 10

    haystack_path = tmp_path / "huge_haystack.txt"

    needles_to_search = [
        "DOESNOTEXIST999",
    ] + [uuid.uuid4().hex for _ in range(12)]  # Add a few random needles.

    # Construct the haystack.
    haystack_list: list[str] = [
        uuid.uuid4().hex for _ in range(haystack_file_size // len(uuid.uuid4().hex))
    ]

    expected_found: list[str] = random.sample(haystack_list, k=target_needle_count)
    needles_to_search.extend(expected_found)

    # Sort then write the haystack to disk.
    haystack_list.sort()

    with haystack_path.open("w", encoding="utf-8") as haystack_file:
        for haystack_line in haystack_list:
            haystack_file.write(haystack_line + "\n")

    assert len(expected_found) == target_needle_count

    found = search_multiple_in_file(haystack_path, needles_to_search)
    assert found == expected_found

    # Validate all lists are non-empty.
    assert len(haystack_list) > 0
    assert len(needles_to_search) > 0
    assert len(found) > 0
    assert len(expected_found) > 0
