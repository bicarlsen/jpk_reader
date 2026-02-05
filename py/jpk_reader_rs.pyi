from typing import Optional
import pyarrow

class QIMapReader:
    """A JPK QI Map data (`.jpk-qi-data`) reader."""

    def __init__(self, path: str) -> None: ...
    def len(self) -> int:
        """
        Returns:
            int: Number of files in the archive.
        """

    def files(self) -> list[str]:
        """Get all files names in the archive.

        Returns:
            list[str]: Sorted list of file names.
        """

    def all_data(self) -> pyarrow.RecordBatch:
        """Gets all data from the file.

        Returns:
            pyarrow.RecordBatch: All file data.

        Raises:
            RuntimeError: If the file can not be read.
        """

    def all_metadata(
        self,
    ) -> dict[tuple[str, Optional[int], Optional[int]], dict[str, str]]:
        """Get all metadata from the file.

        Returns:
            dict[tuple[str, Optional[int], Optional[int]], dict[str, str]]: Dictionary keyed by the property type.
            Keys are of the form `(type, index, segment)`:
            + ("dataset", None, None)
            + ("shared_data", None, None)
            + ("index", int, None)
            + ("segment", int, int)

        Raises:
            RuntimeError: If the file can not be read.
        """
