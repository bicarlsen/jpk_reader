"""Handle real time scope data (`.out`)."""

import polars

def load_data(path: str) -> polars.LazyFrame:
    """Load real time scope data.

    Args:
        path (str): Path to the data file.

    Returns:
        polars.LazyFrame: Loaded data.
    """
