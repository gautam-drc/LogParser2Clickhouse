import logging
from utils.read_logs import read_logs, clear_file_content
from utils.extract_logs import log_to_csv, instructor_log_csv
from utils.database import create_table, insert_csv_file, remove_table_data, create_materialized_view
from app.settings import LOG_FILES, OFFSET_FILES, CSV_FILES


def main() -> None:
    """
    Main function to:
    1. Read application logs.
    2. Convert logs to CSV format.
    3. Create ClickHouse tables and insert data.
    4. Create views and clear offset files after processing.

    Logs handled:
    - Enrollment
    - Instructor
    - Course
    - Login
    """

    tables = ['enrollment', 'instructor', 'course', 'login']
    tbl_columns = {tbl: None for tbl in tables}

    for table in tables:
        remove_table_data(table)

    try:
        # Step 1: Read logs from each category
        enroll_logs = read_logs(LOG_FILES.get('enrollment'), 'enrollment')
        instructor_logs = read_logs(LOG_FILES.get('instructor'), 'instructor')
        course_logs = read_logs(LOG_FILES.get('course'), 'course')
        login_logs = read_logs(LOG_FILES.get('login'), 'login')

        # Step 2: Process and insert data into ClickHouse
        if enroll_logs:
            columns = log_to_csv(enroll_logs, 'enrollment')
            insert_csv_file(
                file=CSV_FILES.get('enrollment'),
                tbl='enrollment',
                columns=columns,
                ord_by=('user_id', 'course_id')
            )
            tbl_columns['enrollment'] = columns

        if instructor_logs:
            columns = instructor_log_csv(instructor_logs, 'instructor')
            insert_csv_file(
                file=CSV_FILES.get('instructor'),
                tbl='instructor',
                columns=columns,
                ord_by=('id', 'course_id')
            )
            tbl_columns['instructor'] = columns

        if course_logs:
            columns = log_to_csv(course_logs, 'course')
            insert_csv_file(
                file=CSV_FILES.get('course'),
                tbl='course',
                columns=columns,
                ord_by='course_id'
            )
            tbl_columns['course'] = columns

        if login_logs:
            columns = log_to_csv(login_logs, 'login')
            insert_csv_file(
                file=CSV_FILES.get('login'),
                tbl='login',
                columns=columns,
                ord_by=('user_id', 'timestamp')
            )
            tbl_columns['login'] = columns

        # Step 3: Create materialized view for course enrollment summary

        join_conditions = {
            ('enrollment', 'course'):'course_id',
            ('course', 'instructor'):'course_id',
            ('enrollment', 'login'):'user_id'
        }

        

        create_materialized_view(view='course_enrollment', tables_columns=tbl_columns, from_table='enrollment', join_column=join_conditions, ord_by=('e_user_id', 'e_course_id'))

        # Step 4: Clear offset files to avoid reprocessing
        for file in OFFSET_FILES.values():
            clear_file_content(file)

        logging.info("Log processing and data insertion completed successfully.")

    except Exception as e:
        logging.error(f"Unexpected error in main processing: {e}", exc_info=True)


if __name__ == '__main__':
    main()
