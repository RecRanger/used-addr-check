__VERSION__ = "0.1.6"
__AUTHOR__ = "RecRanger"

from .index_create import (  # noqa F401
    load_or_generate_index,
    generate_index,
    store_index_json,
    load_index_json,
    store_index_parquet,
    load_index_parquet,
)
from .index_search import (  # noqa F401
    search_in_file_with_index,
    search_multiple_in_file,  # <- main library function
)
from .index_types import IndexEntry  # noqa F401
from .cli import main_cli  # noqa F401
