from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 extra_parameters = '',
                 region = 'us-east-1',
                 json_path = 'auto',
                 additional_parameters = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.extra_parameters = extra_parameters
        self.region = region
        self.json_path = json_path
        self.additional_parameters = additional_parameters

    def execute(self, context):
        self.log.info(context["dag"].dag_id)
        self.log.info(context["ds"])
        self.log.info(context["execution_date"])
        # Fetch connection parameters
        self.log.info('Commencing staging operation...')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Clear destination table
        self.log.info('Clearing data from destination table...')
        redshift.run('DELETE FROM {}'.format(self.table))

        # Copy data to redshift
        self.log.info('Copying data to Redshift...')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region,
            self.json_path,
            self.additional_parameters
        )
        self.log.info('Staging operation completed!')
        redshift.run(formatted_sql)





