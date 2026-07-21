class InvalidReportType(Exception):
    """Raised when report type is not story or discussion."""
    pass


class InvalidFileType(Exception):
    """Raised when file extension is not csv."""
    pass


class FileTooLarge(Exception):
    """Raised when upload size exceeds configuration settings."""
    pass


class EmptyFile(Exception):
    """Raised when the uploaded file contains zero bytes."""
    pass


class DuplicateFile(Exception):
    """Raised when a CSV file with the same details is already uploaded."""
    pass


class RecordNotFound(Exception):
    """Raised when the CSV record cannot be found in database."""
    pass


class RecordAlreadyProcessing(Exception):
    """Raised when the record is already in_progress."""
    pass


class RecordNotPending(Exception):
    """Raised when trying to process a record that is not in pending status."""
    pass
