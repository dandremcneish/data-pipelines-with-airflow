from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs a list of SQL-based data quality checks against Redshift.

    Each check is a dict: {'check_sql': '<query returning one value>',
                            'expected_result': <value the query should return>}

    If any check's actual result doesn't match its expected result, this
    raises a ValueError, which Airflow treats as a task failure -- the task
    will retry per the DAG's default_args and eventually fail if the data
    problem isn't resolved.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.dq_checks:
            raise ValueError('DataQualityOperator was given no checks to run')

        failing_checks = []

        for check in self.dq_checks:
            check_sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            self.log.info(f'Running data quality check: {check_sql}')
            records = redshift.get_records(check_sql)

            if not records or not records[0]:
                failing_checks.append(
                    f'Check returned no results. Query: {check_sql}'
                )
                continue

            actual_result = records[0][0]
            if actual_result != expected_result:
                failing_checks.append(
                    f'Check failed. Query: {check_sql} -- '
                    f'expected {expected_result}, got {actual_result}'
                )

        if failing_checks:
            raise ValueError(
                'Data quality check(s) failed:\n' + '\n'.join(failing_checks)
            )

        self.log.info(f'All {len(self.dq_checks)} data quality checks passed')
