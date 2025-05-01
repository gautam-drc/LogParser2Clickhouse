import logging
from typing import Dict, Union, List

import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file
from app.settings import DATABASE as DB_SETTINGS

# Extract DB config
HOST: str = DB_SETTINGS.get('host')
PORT: int = int(DB_SETTINGS.get('port', 9000))
USER: str = DB_SETTINGS.get('user')
PASSWORD: str = DB_SETTINGS.get('password')
DATABASE: str = DB_SETTINGS.get('database')

# Create ClickHouse client connection
client = clickhouse_connect.get_client(
    host=HOST,
    port=PORT,
    username=USER,
    password=PASSWORD
)

# Gautam Savasaviya

# Pandas dtype to ClickHouse type mapping
DATATYPE_MAPPING: Dict[str, str] = {
    "int64": "Int64",
    "int32": "Int32",
    "int16": "Int16",
    "int8": "Int8",
    "uint64": "UInt64",
    "uint32": "UInt32",
    "uint16": "UInt16",
    "uint8": "UInt8",
    "float64": "Float64",
    "float32": "Float32",
    "bool": "Bool",  
    "datetime64[ns]": "Nullable(DateTime)",
    "datetime64[ns, UTC]": "Nullable(DateTime)",
    "timedelta64[ns]": "Int64",  # Representing duration
    "object": "String",          # Generic object in pandas
    "string": "String",
    "category": "String",
}


def create_database() -> None:
    """
    Creates the specified ClickHouse database if it does not exist.
    """
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`")
        logging.info(f"Database `{DATABASE}` created or already exists.")
    except Exception as e:
        logging.error(f"Error creating database `{DATABASE}`: {e}", exc_info=True)


def generate_table_query(columns: Dict[str, str], tbl: str, ord_by: Union[str, List[str], tuple]) -> str:
    """
    Generates a CREATE TABLE SQL query for ClickHouse.

    Parameters:
        columns (Dict[str, str]): Dictionary of column names and pandas dtypes.
        tbl (str): Table name.
        ord_by (Union[str, List[str], tuple]): Column(s) to order by.

    Returns:
        str: SQL query string to create the table.
    """
    try:
        fields = [
            f"{col} {'DateTime' if col in ['update_time', 'timestamp'] else DATATYPE_MAPPING.get(str(dtype), 'String')}"
            for col, dtype in columns.items()
        ]
        order_by_clause = ", ".join(ord_by) if isinstance(ord_by, (list, tuple)) else ord_by

        query = f"""
        CREATE TABLE IF NOT EXISTS `{DATABASE}`.`{tbl}` (
            {', '.join(fields)}
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY ({order_by_clause})
        """
        logging.debug(f"Generated CREATE TABLE query:\n{query}")
        return query
    except Exception as e:
        logging.error(f"Error generating table query: {e}", exc_info=True)
        return ""


def create_table(columns: Dict[str, str], tbl: str, ord_by: Union[str, List[str], tuple]) -> None:
    """
    Creates a ClickHouse table if it does not exist.

    Parameters:
        columns (Dict[str, str]): Column definitions with pandas dtypes.
        tbl (str): Table name.
        ord_by (Union[str, List[str], tuple]): Column(s) to order by.
    """
    try:
        create_database()
        query = generate_table_query(columns, tbl, ord_by)
        client.command(query)
        logging.info(f"Table `{tbl}` created successfully.")
    except Exception as e:
        logging.error(f"Error creating table `{tbl}`: {e}", exc_info=True)


def insert_csv_file(file: str, tbl: str, columns: Dict[str, str], ord_by: Union[str, List[str], tuple]) -> None:
    """
    Inserts data from a CSV file into a ClickHouse table.

    Parameters:
        file (str): Path to the CSV file.
        tbl (str): Target ClickHouse table.
        columns (Dict[str, str]): Table column definitions.
        ord_by (Union[str, List[str], tuple]): Columns to order the table by.
    """
    try:
        create_table(columns=columns, tbl=tbl, ord_by=ord_by)
        insert_file(client=client, database=DATABASE, table=tbl, file_path=str(file), fmt='CSVWithNames') # Convert Path object to string, as insert_file expects a string path, not a pathlib.Path object
        client.command(f'OPTIMIZE TABLE `{DATABASE}`.`{tbl}` FINAL')
        logging.info(f"Data inserted and optimized for table `{tbl}`.")
    except Exception as e:
        logging.error(f"Error inserting data into `{tbl}`: {e}", exc_info=True)


def remove_table_data(tbl: str) -> None:
    """
    Truncates all data from the specified ClickHouse table.

    Parameters:
        tbl (str): Name of the table to clear.
    """
    try:
        client.command(f'TRUNCATE TABLE `{DATABASE}`.`{tbl}`')
        logging.info(f"Data removed from `{tbl}`.")
    except Exception as e:
        logging.error(f"Error truncating table `{tbl}`: {e}", exc_info=True)


def create_table_for_view(view: str) -> None:
    """
    Creates a table to store data for a materialized view if it doesn't exist.

    Parameters:
        view (str): Name of the target view (table will use the same name).
    """
    try:
        exists = client.command(f"EXISTS TABLE `{DATABASE}`.`{view}`")
        if exists == 0:
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS `{DATABASE}`.`{view}` (
                user_id Int64,
                join_date Nullable(DateTime),
                username String,
                course_id String,
                course_name String,
                organization String,
                course_created_date Nullable(DateTime),
                course_status String,
                enroll_date Nullable(DateTime),
                is_enrolled UInt8,
                is_course_complete Int64,
                instructor_id Int64,
                instructor_username String
            ) ENGINE = MergeTree()
            ORDER BY (user_id)
            '''
            client.command(create_table_query)
            logging.info(f"Table `{view}` created successfully for materialized view.")
        else:
            logging.info(f"Table `{view}` already exists.")
    except Exception as e:
        logging.error(f"Error creating view table `{view}`: {e}", exc_info=True)


def create_materialized_view(view: str, refresh_rate: int = 30) -> None:
    """
    Creates a materialized view in ClickHouse with periodic refresh.

    Parameters:
        view (str): Base name for the view; materialized view will be `{view}_mv`.
        refresh_rate (int): Refresh interval in minutes (default is 30).
    """
    try:
        create_table_for_view(view)

        query = f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{DATABASE}`.`{view}_mv`
        REFRESH EVERY {refresh_rate} MINUTE TO `{DATABASE}`.`{view}` AS
        SELECT 
            e.user_id, 
            e.join_date, 
            e.username AS username, 
            c.course_id AS course_id, 
            c.course_name, 
            c.organization, 
            c.course_created_date, 
            c.course_status, 
            e.enroll_date, 
            e.is_enrolled, 
            e.is_course_complete, 
            i.id AS instructor_id, 
            i.username AS instructor_username 
        FROM `{DATABASE}`.`enrollment` AS e
        LEFT JOIN `{DATABASE}`.`course` AS c ON e.course_id = c.course_id 
        LEFT JOIN `{DATABASE}`.`instructor` AS i ON c.course_id = i.course_id;
        '''
        client.command(query)
        logging.info(f"Materialized view `{view}_mv` created with refresh rate of {refresh_rate} minutes.")
    except Exception as e:
        logging.error(f"Error creating materialized view `{view}_mv`: {e}", exc_info=True)
