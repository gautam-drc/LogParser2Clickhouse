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
        logging.error(
            f"Error creating database `{DATABASE}`: {e}", exc_info=True)


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
        order_by_clause = ", ".join(ord_by) if isinstance(
            ord_by, (list, tuple)) else ord_by

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
        # Convert Path object to string, as insert_file expects a string path, not a pathlib.Path object
        insert_file(client=client, database=DATABASE, table=tbl,
                    file_path=str(file), fmt='CSVWithNames')
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


def generate_join_query(
    tables_columns: Dict[str, List[str]],
    from_table: str,
    join_column: Dict[tuple[str, str], str]
) -> str:
    """
    Generates a SQL JOIN query based on the provided tables, columns, and join conditions.
    Parameters:
        tables_columns (Dict[str, List[str]]): Dictionary of table names and their columns.
        from_table (str): The main table to select from.
        join_column (Dict[tuple[str, str], str]): Dictionary of join conditions.
            Each key is a tuple of (table1, table2) and the value is the join column.
            Make sure table1 is base table and table2 is the one to join. and all pairs are in proper sequence.

            Example:
            {
                ('enrollment', 'course'): 'course_id', # enrollment and course joins on course_id
                ('course', 'instructor'): 'course_id' # course and instructor joins on course_id
            }
    Returns:
        str: SQL JOIN query string.
    """

    if tables_columns is None or len(tables_columns) <= 1:
        raise ValueError("At least two tables are required for a JOIN query.")

    if from_table not in tables_columns:
        raise ValueError("From table is not in tables list")

    try:

        table_alias = {tbl: ''.join(word[0] for word in tbl.split(
            '_')) for tbl in tables_columns.keys()}

        query = f"SELECT "

        for tbl, cols in tables_columns.items():
            query += ", ".join(
                [f"{table_alias[tbl]}.{col} AS {table_alias[tbl]}_{col}" for col in cols]) + ", "
        query = query.rstrip(", ")

        query += f" FROM {DATABASE}.{from_table} AS {table_alias[from_table]}"

        for (tbl1, tbl2), col in join_column.items():
            tbl1_alias = table_alias[tbl1]
            tbl2_alias = table_alias[tbl2]

            query += f" LEFT JOIN {DATABASE}.{tbl2} AS {tbl2_alias} ON {tbl1_alias}.{col} = {tbl2_alias}.{col}"

        logging.info("Generated JOIN query successfully.")
        logging.debug(f"Generated JOIN query:\n{query}")
        return query.rstrip()

    except Exception as e:
        logging.error(f"Error generating JOIN query: {e}", exc_info=True)
        return ""


def create_view_table(
    view: str,
    columns: Dict[str, Dict[str, str]],
    ord_by: Union[str, List[str], tuple]
) -> None:
    """
    Creates a view table in ClickHouse if it doesn't exist.
    Parameters:
        view (str): Name of the view.
        columns (Dict[str, Dict[str, str]]): Dictionary of column names and their types.
        ord_by (Union[str, List[str], tuple]): Column(s) to order by.

    Returns:
        None
    """
    if not columns:
        raise ValueError("Columns dictionary cannot be empty.")
    if not ord_by or ord_by is None:
        raise ValueError("Order by clause can not be none or empty.")

    try:
        is_exists = client.command(f"EXISTS TABLE `{DATABASE}`.`{view}`")

        if not is_exists:
            query = f"CREATE TABLE IF NOT EXISTS `{DATABASE}`.`{view}`"
            table_alias = {tbl: ''.join(
                word[0] for word in tbl.split('_')) for tbl in columns.keys()}
            fields = [
                f"{table_alias[tbl]}_{col} {'DateTime' if col in ['update_time', 'timestamp'] else DATATYPE_MAPPING.get(str(dtype), 'String')}"
                for tbl, cols in columns.items() for col, dtype in cols.items()
            ]

            # TODO: Add prefix of table name to the column name
            order_by_clause = ", ".join(ord_by) if isinstance(
                ord_by, (list, tuple)) else ord_by
            query += f"({', '.join(fields)}) ENGINE = MergeTree() ORDER BY ({order_by_clause})"
            client.command(query)

        logging.info(f"View table `{view}` created.")
    except Exception as e:
        logging.error(
            f"Error creating view table `{view}`: {e}", exc_info=True)


def create_materialized_view(
        view: str, 
        tables_columns: Dict[str, Dict[str, str]],
        from_table: str,
        join_column: Dict[tuple[str, str], str], 
        ord_by: Union[str, List[str], tuple],
        refresh_rate: int = 30
    ) -> None:
    """
    Create a materialized view in ClickHouse with periodic refresh.
    Parameters:
        view (str): Base name for the view; materialized view will be `{view}_mv`.
        tables_columns (Dict[str, Dict[str, str]]): Dictionary of table names and their columns with pandas data type.
        from_table (str): The main table to select from.
        join_column (Dict[tuple[str, str], str]): Dictionary of join conditions.
            Each key is a tuple of (table1, table2) and the value is the join column.
            Make sure table1 is base table and table2 is the one to join. and all pairs are in proper sequence.

            Example:
            {
                ('enrollment', 'course'): 'course_id', # enrollment and course joins on course_id
                ('course', 'instructor'): 'course_id' # course and instructor joins on course_id
            }
        ord_by (Union[str, List[str], tuple]): Column(s) to order by.
        refresh_rate (int): Refresh interval in minutes (default is 30).
    Returns:
        None
    
    """
    try:
        create_view_table(view=view, columns=tables_columns, ord_by=ord_by)

        tables_columns = {table: [col for col in columns.keys()] for table, columns in tables_columns.items()}
        join_query = generate_join_query(tables_columns=tables_columns, from_table=from_table,join_column=join_column)

        query = f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{DATABASE}`.`{view}_mv`
        REFRESH EVERY {refresh_rate} MINUTE TO `{DATABASE}`.`{view}` AS
        {join_query}
        '''
        client.command(query)
        logging.info(
            f"Materialized view `{view}_mv` created with refresh rate of {refresh_rate} minutes.")
    except Exception as e:
        logging.error(
            f"Error creating materialized view `{view}_mv`: {e}", exc_info=True)
