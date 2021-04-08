
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

class MoveCSVToSQLOperator(BaseOperator):

    template_fields = ('csv_file', 'sql_table')
    ui_color = '#A6E6A6'

    @apply_defaults
    def __init__(
            self,
            schema,
            csv_file,
            mssql_table,
            hive_conn_id='hive_cli_default',
            mssql_conn_id='mssql_default',
            num_mappers='10',
            column_list='*',
            delimiter="','",
            quote_char="'\"'",
            escape_char="'\\\\'",
            *args, **kwargs):
        """
        :param hive_table: hive table name
        :param mssql_table: mssql table name
        :param hives_conn_id: conn id
        :param mssql_conn_id: conn id
        :param num_mappers: number of mappers to use
        :param column_list: user column list
        :param delimiter: the delimeter to use while creating temporary hive textfile table
        :param quote_char: the quote char to use while creating temporary hive textfile table
        :escape_char:  the escape char to use while creating temporary hive textfile table
        """

        super(MoveHiveToSQLOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.hive_table = hive_table
        self.mssql_table = mssql_table
        self.hive_conn_id = hive_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.num_mappers = num_mappers
        self.column_list = column_list
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.escape_char = escape_char

    def execute(self, context):

        hook = HiveCliHook()

        # -------Convert Hive Table from PARQUET to TEXT --------------------------------------------

        logging.info("Creating a temporary Hive TEXTFILE table...")

        tmp_table = self.hive_table + '__tmp_textfile'

        query = """
            DROP TABLE {tmp_table};

            CREATE TABLE {tmp_table}
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                WITH SERDEPROPERTIES (
                   "separatorChar" = {delimiter},
                   "quoteChar"     = {quote_char},
                   "escapeChar"    = {escape_char}
                )
            STORED AS TEXTFILE
            AS
            SELECT {column_list}
            FROM {table}

        """.format(tmp_table=tmp_table,
                   table=self.hive_table,
                   delimiter=self.delimiter,
                   quote_char=self.quote_char,
                   escape_char=self.escape_char,
                   column_list=self.column_list)

        hook.run_cli(schema=self.schema, hql=query)

        # --------------Run sqoop--------------------------------------------------------------------

        # get default mssql connection
        sql_conn = BaseHook.get_connection('mssql_default')
        conn_str = "'jdbc:sqlserver://{host};databaseName={database}'".format(
            host=sql_conn.host,
            database=sql_conn.extra_dejson.get('database')
        )

        # get default hive cli connection
        hive_conn = BaseHook.get_connection('hive_cli_default')
        hdfs_export_dir = 'hdfs://{host}:{port}/user/hive/warehouse/my_hive_db.db/{table}'.format(
            host=hive_conn.host,
            port=8020,
            table=tmp_table
        )

        cmd = ['/usr/bin/sqoop', 'export',
               '--connect', conn_str,
               '--username', sql_conn.login,
               '--password', sql_conn.password,
               '--table', self.mssql_table,
               '--export-dir', hdfs_export_dir,
               '-m', self.num_mappers,
               '--input-fields-terminated-by', self.delimiter,
               '--input-enclosed-by', self.quote_char,
               '--input-escaped-by', self.escape_char,
               '--input-null-string', "'\\\N'"]
        cmd = ' '.join(cmd)

        logging.info("Executing sqoop")
        sp = Popen(cmd, shell=True)

        sp.wait()
        logging.info("Command exited with return code {0}".format(sp.returncode))

        # finally remove temp text hive table
        hook.run_cli(schema=self.schema, hql="DROP TABLE {}".format(tmp_table))