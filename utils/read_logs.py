import os
import logging
from typing import List, Optional
from app.settings import OFFSET_FILES


def get_offset(file: str) -> int:
    """
    Retrieves the last byte offset stored in the specified file.

    Parameters:
        file (str): Path to the offset file.

    Returns:
        int: The stored offset value, or 0 if not found or file is empty.

    Logs:
        - Error if the file cannot be read or parsed.
    """
    try:
        if os.path.exists(file):
            with open(file, 'r') as f:
                value = f.read().strip()
                return int(value) if value else 0
        return 0
    except Exception as e:
        logging.error(f"Error reading offset from {file}: {e}", exc_info=True)
        return 0


def set_offset(offset: int, file: str) -> None:
    """
    Writes the given offset value to the specified file.

    Parameters:
        offset (int): The new offset value to store.
        file (str): Path to the offset file.

    Logs:
        - Info when the offset is successfully written.
        - Error if the file cannot be written.
    """
    try:
        with open(file, 'w') as f:
            f.write(str(offset))
        logging.info(f"Offset set to {offset} in {file}")
    except Exception as e:
        logging.error(f"Error setting offset in {file}: {e}", exc_info=True)


def clear_file_content(file_path: str) -> None:
    """
    Clears all content from the specified file.

    Parameters:
        file_path (str): Path to the file whose contents should be cleared.

    Logs:
        - Info when the file is cleared successfully.
        - Error if the file cannot be cleared.
    """
    try:
        with open(file_path, 'w'):
            pass
        logging.info(f"File content cleared successfully: {file_path}")
    except Exception as e:
        logging.error(f"Error clearing file content in {file_path}: {e}", exc_info=True)


def read_logs(file: str, source_type: str) -> Optional[List[str]]:
    """
    Reads log lines from a file starting at the last known offset for the source type.

    Parameters:
        file (str): Path to the log file.
        source_type (str): Key to fetch the corresponding offset file from OFFSET_FILES.

    Returns:
        List[str] | None: A list of log lines read from the file, or None if an error occurs.

    Logs:
        - Error if reading the file or determining offset fails.
    """
    try:
        offset_file = OFFSET_FILES.get(source_type)
        if not offset_file:
            logging.warning(f"Offset file not defined for source type: {source_type}")
            return None

        offset = get_offset(offset_file)

        with open(file, 'r') as f:
            f.seek(offset)
            data = f.readlines()
            set_offset(f.tell(), offset_file)

        return data
    except Exception as e:
        logging.error(f"Error reading logs from {file}: {e}", exc_info=True)
        return None
