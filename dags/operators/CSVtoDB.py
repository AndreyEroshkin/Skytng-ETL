from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# other packages
from datetime import datetime, timedelta
from os import environ
import csv
import logging
import os
import gzip


class DataSourceToCsv(BaseOperator):
    """
    Extract data from the data source to CSV file
    """
    ui_color = '#A6A526'
    
    @apply_defaults
    def __init__(
            self,
            table_name,
            extract_query,
            connection,
            *args, **kwargs):
        super(DataSourceToCsv, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.extract_query = extract_query
        self.connection = connection
        self.file_path = './'  # filepath_to_save_CSV
        self.cursor = self.create_connection()

    
    def create_connection(self):
        try:
            pg_con = PostgresHook.get_connection(self.connection).
            return pg_con.cursor()
        except Exception as e:
            logging.error(e)
        return None
    
    def execute_query(self, query):
    try:
        self.cursor.execute(query)
        self.cursor.close()
        self.connection.commit()
        return True
    except Error as e:
        print(f"The error '{e}' occurred")

    def read_query(self, query):
        r = None
        try:
            self.cursor.execute(query)
            r = self.cursor.fetchall()
            self.cursor.close()
            return r
        except Error as e:
            print(f"The error '{e}' occurred")

    def read_query_df(self, select_query, column_names):
        try:
            self.cursor.execute(select_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.cursor.close()
            return 1
        df = pd.DataFrame(self.cursor.fetchall(), columns=column_names)
        self.cursor.close()
        return df














    def __csv_to_db(self, execution_date):
        csv_file_path = self.file_path + \
            self.table_name + \
            '_' + execution_date + '.csv.gz'
        with gzip.open(csv_file_path, 'rt') as f:
            csvobj = csv.reader(f,delimiter = '|',quotechar='"')
        
        create_query = """
        CREATE TABLE etl.order (
            id bigint primary key,
            student_id bigint,
            teacher_id bigint,
            stage varchar(10),
            status varchar(512),
            created_at timestamp,
            updated_at timestamp
        );
        """
        
        
        
        cursor = PostgresHook(self.connection).get_conn().cursor()
        cursor.execute(final_query)
        result = cursor.fetchall()
        # Write to CSV file
        temp_path = self.file_path + \
            self.table_name + \
            '_' + execution_date + '.csv'
        with open(temp_path, 'w') as fp:
            a = csv.writer(fp, quoting=csv.QUOTE_MINIMAL, delimiter='|')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        # Read CSV file
        full_path = temp_path + '.gz'
        with open(temp_path, 'rb') as f:
            data = f.read()
        # Compress CSV file
        with gzip.open(full_path, 'wb') as output:
            try:
                output.write(data)
            finally:
                output.close()
        # Close file after reading
        f.close()
        # Delete csv file
        os.remove(temp_path)
        # Change access mode
        os.chmod(full_path, 0o777)

    def execute(self, context):
        execution_date = str(context.get('execution_date'))
        self.__datasource_to_csv(execution_date)
