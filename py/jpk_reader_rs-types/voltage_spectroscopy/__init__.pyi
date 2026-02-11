"""Load voltage spectroscopy data (`.jpk-voltage-ramp`)."""

import polars

def load_data(path: str) -> polars.DataFrame:
    """Load a single voltage spectroscopy data file (`.jpk-voltage-ramp`)

    Returns:
        polars.DataFrame: Data with position (`x`, `y`), segment (`segment`), and all channel data.
    """

def load_dir(path: str) -> polars.DataFrame:
    """Load all voltage spectroscopy data files (`.jpk-voltage-ramp`) within the directory.

    Args:
        path (str): Path to the data directory.

    Returns:
        polars.DataFrame: Dataframe containing data from all the voltage spectroscopy files (`.jpk-voltage-ramp`)
        in the directory. Has columns of position (`x`, `y`), segment (`segment`), and all data channels.
    """
