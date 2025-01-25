from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table = '',
                 redshift_conn_id = '',
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.queries_dict = {
            'users': SqlQueries.user_table_insert,
            'times': SqlQueries.time_table_insert,
            'artists': SqlQueries.artist_table_insert,
            'songs': SqlQueries.song_table_insert
        }

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info('Deleting rows from \'{}\' table...'.format(self.table))
            redshift.run('TRUNCATE TABLE {}'.format(self.table))
        self.log.info('Loading \'{}\' dimension table...'.format(self.table))
        redshift.run(self.queries_dict[self.table])
        self.log.info('\'{}\' table has been loaded!'.format(self.table))
