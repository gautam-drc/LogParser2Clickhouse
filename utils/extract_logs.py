import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from app.settings import CSV_FILES
import logging


def get_file(source_type: str) -> str:
    """
    Retrieves the file path for the CSV corresponding to the given source type.

    Parameters:
        source_type (str): One of ['enrollment', 'instructor', 'course', 'login']

    Returns:
        str: Path to the CSV file as defined in the CSV_FILES dictionary.
             Returns an empty string if the source_type is not defined.

    Logs:
        Warning if the source_type is invalid or not configured.
    """
    csv_file = CSV_FILES.get(source_type)
    if not csv_file:
        logging.warning(f"CSV file not defined for source type: {source_type}")
        return ''
    return csv_file


def extract_string(s: str) -> str:
    """
    Extracts the JSON portion of a log line starting from the first '{' character.

    Parameters:
        s (str): Raw log string

    Returns:
        str: A JSON-formatted string, or '{}' if no valid JSON found.
    """
    start = s.find('{')
    return s[start:] if start != -1 else '{}'


def str_to_json(s: str) -> Dict[str, Any]:
    """
    Converts a log string to a Python dictionary.

    Parameters:
        s (str): Raw log string that may contain a JSON object

    Returns:
        dict: Parsed JSON object with an additional `update_time` field.
              Returns an empty dictionary if parsing fails.

    Logs:
        Error if JSON decoding fails.
    """
    try:
        s = extract_string(s)
        log_entry = json.loads(s)
        log_entry['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return log_entry
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        return {}


def log_to_csv(logs: str, source_type: str) -> pd.Series:
    """
    Parses multi-line log data and saves it as a flattened CSV file.

    Parameters:
        logs (str): Multi-line string containing raw log entries.
        source_type (str): Key to identify which CSV file to write (e.g., "enrollment")

    Returns:
        pd.Series: Data types of the resulting DataFrame columns.

    Logs:
        - Info when logs are successfully saved.
        - Warning if CSV path is not found.
        - Error if saving fails.
    """
    data = [str_to_json(line) for line in logs]
    df = pd.json_normalize(data)

    datetime_cols = [col for col in df.columns if "date" in col.lower() or "time" in col.lower()]
    df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce')

    try:
        file_path = get_file(source_type)
        if file_path:
            df.to_csv(file_path, index=False)
            logging.info(f"Log data saved to {file_path}")
        else:
            logging.warning(f"CSV file path not found for source type: {source_type}")
    except Exception as e:
        logging.error(f"Error saving log data to CSV: {e}")

    return df.dtypes


def instructor_log_csv(logs: str, source_type: str) -> pd.Series:
    """
    Extracts and flattens instructor data from log entries and writes it to a CSV.

    Parameters:
        logs (str): Multi-line string containing raw log entries.
        source_type (str): Key to identify which CSV file to write (e.g., "instructor")

    Returns:
        pd.Series: Data types of the resulting DataFrame columns.

    Logs:
        - Info when instructor logs are successfully saved.
        - Warning if CSV path is not found.
        - Error if saving fails.
    """
    data = [str_to_json(line) for line in logs]
    rows = []

    for item in data:
        if isinstance(item, dict) and "instructors" in item:
            for instructor in item.get("instructors", []):
                instructor["course_id"] = item.get("course_id")
                instructor["update_time"] = item.get("update_time")
                rows.append(instructor)

    df = pd.DataFrame(rows)

    try:
        file_path = get_file(source_type)
        if file_path:
            df.to_csv(file_path, index=False)
            logging.info(f"Instructor log data saved to {file_path}")
        else:
            logging.warning(f"CSV file path not found for source type: {source_type}")
    except Exception as e:
        logging.error(f"Error saving instructor log data to CSV: {e}")

    return df.dtypes
