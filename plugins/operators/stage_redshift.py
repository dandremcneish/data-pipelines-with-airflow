from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON-formatted files from S3 into a Redshift staging table using a
    SQL COPY statement. The s3_key is templated so backfills can pull
    timestamped partitions based on the DAG's execution date.
    """
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 json_path='auto',
                 truncate_table=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
        self.truncate_table = truncate_table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Clearing data from destination Redshift table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        # s3_key is a templated field, so Airflow has already rendered any
        # {{ execution_date }}-style Jinja macros in it by the time we get here.
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path,
        )
        redshift.run(formatted_sql)
        self.log.info(f'Staging of {self.table} complete')
