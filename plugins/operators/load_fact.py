from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Loads a fact table in Redshift by running a provided SQL SELECT
    (from the SqlQueries helper class) and inserting the results into the
    target table. Fact tables are large, so this operator only ever
    appends -- it never truncates the destination table.
    """
    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Loading fact table {self.table}')
        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_sql)
        self.log.info(f'Load of fact table {self.table} complete')
