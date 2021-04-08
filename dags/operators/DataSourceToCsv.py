# airflow related
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
    ui_color = '#E6A5E6'
    
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

    def __datasource_to_csv(self, execution_date):
        final_query = self.extract_query.\
            replace("$EXECUTION_DATE", """'%s'""" % execution_date)
        logging.info("QUERY : %s" % final_query)
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
