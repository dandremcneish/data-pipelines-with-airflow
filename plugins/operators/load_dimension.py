from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads a dimension table in Redshift from a provided SQL SELECT
    (from the SqlQueries helper class). Defaults to the truncate-insert
    pattern (clear the table, then load fresh), but truncate_table can be
    set to False to switch to append-only mode when needed.
    """
    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_query='',
                 truncate_table=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Truncating dimension table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Loading dimension table {self.table}')
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_sql)
        self.log.info(f'Load of dimension table {self.table} complete')
