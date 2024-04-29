from dataclasses import dataclass


@dataclass
class IndexEntry:
    line_value: str
    byte_offset: int
    line_number: int
