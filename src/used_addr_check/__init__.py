__VERSION__ = "0.0.1"
__AUTHOR__ = "RecRanger"

from .index_create import (  # noqa F401
    create_or_load_index,
    generate_index,
    store_index_json,
    load_index_json,
)
from .index_search import (  # noqa F401
    binary_search_index,
    search_in_file_with_index,
)
from .index_types import IndexEntry  # noqa F401
from .cli import main_cli  # noqa F401
