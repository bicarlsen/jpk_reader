import pyarrow

class QIMapReader:
    """A JPK QI Map data (`.jpk-qi-data`) reader."""

    def __init__(self, path: str) -> None: ...
    def all_data(self) -> pyarrow.RecordBatch:
        """Gets all data from the file.

        Returns:
            pyarrow.RecordBatch: All file data.

        Raises:
            RuntimeError: If the file can not be opened.
        """
