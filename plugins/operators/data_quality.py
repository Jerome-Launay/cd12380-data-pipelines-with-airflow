import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_list = ['users', 'songs', 'artists', 'times', 'songplays']

    def execute(self, context):
        self.log.info('Starting quality checks...')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for table in self.table_list:
            records = redshift.get_records('SELECT COUNT(*) FROM {}'.format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError('Data quality check failed. \'{}\' table returned no results...'.format(table))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError('Data quality check failed. \'{}\' table contained {} rows.'.format(table, num_records))
            logging.info('Data quality check on \'{}\' table passed with {} records.'.format(table, num_records))