from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,       # no dependencies on past runs
    'retries': 3,                   # retry a failed task 3 times
    'retry_delay': timedelta(minutes=5),  # wait 5 minutes between retries
    'email_on_retry': False,        # no email on retry
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,                  # catchup turned off
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        json_path='s3://udacity-dend/log_json_path.json',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        region='us-west-2',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        json_path='auto',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        region='us-west-2',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        truncate_table=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        truncate_table=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        truncate_table=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        truncate_table=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=[
            {
                'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
                'expected_result': 0,
            },
            {
                'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL",
                'expected_result': 0,
            },
            {
                'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
                'expected_result': 0,
            },
            {
                'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
                'expected_result': 0,
            },
            {
                'check_sql': "SELECT COUNT(*) FROM \"time\" WHERE start_time IS NULL",
                'expected_result': 0,
            },
        ],
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # ---- Task dependencies ----
    # Begin_execution -> Stage_events, Stage_songs
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    # Stage_events, Stage_songs -> Load_songplays_fact_table
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    # Load_songplays_fact_table -> the four dimension loads (parallel)
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]

    # All four dimension loads -> Run_data_quality_checks
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks

    # Run_data_quality_checks -> Stop_execution
    run_quality_checks >> end_operator


final_project_dag = final_project()
