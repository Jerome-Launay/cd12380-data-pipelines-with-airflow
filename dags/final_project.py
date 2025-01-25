from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import get_current_context
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_staging_events_task = PostgresOperator(
    task_id = "create_staging_events_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.staging_events_table_create
    )

    create_staging_songs_task = PostgresOperator(
    task_id = "create_staging_songs_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.staging_songs_table_create
    )

    create_songs_task = PostgresOperator(
    task_id = "create_songs_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.song_table_create
    )

    create_artists_task = PostgresOperator(
    task_id = "create_artists_songs_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.artist_table_create
    )

    create_users_task = PostgresOperator(
    task_id = "create_users_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.user_table_create
    )

    create_songplay_task = PostgresOperator(
    task_id = "create_songplay_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.songplay_table_create
    )

    create_time_task = PostgresOperator(
    task_id = "create_time_task",
    postgres_conn_id = "redshift",
    sql = SqlQueries.time_table_create
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id = 'stage_events',
        table = 'staging_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = 'udacity-dend',
        s3_key = 'log-data',
        json_path = 's3://udacity-dend/log_json_path.json',
        region = 'us-west-2',
        additional_parameters = "TIMEFORMAT 'epochmillisecs'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id = 'stage_songs',
        table = 'staging_songs',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = 'udacity-dend',
        s3_key = 'song-data',
        region = 'us-west-2',
        additional_parameters = 'TRUNCATECOLUMNS'
    )

    load_songplays_table = LoadFactOperator(
        task_id = 'load_songplays_fact_table',
        redshift_conn_id = 'redshift'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id = 'load_user_dim_table',
        table = 'users',
        redshift_conn_id = 'redshift'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id = 'load_song_dim_table',
        table = 'songs',
        redshift_conn_id = 'redshift'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id = 'load_artist_dim_table',
        table = 'artists',
        redshift_conn_id = 'redshift'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id = 'load_time_dim_table',
        table = 'times',
        redshift_conn_id = 'redshift'
    )

    run_quality_checks = DataQualityOperator(
        task_id = 'run_data_quality_checks',
        redshift_conn_id = 'redshift'
    )

    start_operator >> create_staging_events_task >> stage_events_to_redshift >> load_songplays_table
    start_operator >> create_staging_songs_task >> stage_songs_to_redshift >> load_songplays_table
    start_operator >> create_artists_task
    start_operator >> create_songs_task
    start_operator >> create_time_task
    start_operator >> create_users_task
    start_operator >> create_songplay_task
    create_songplay_task >> load_songplays_table
    load_songplays_table >> load_artist_dimension_table
    create_artists_task >> load_artist_dimension_table
    load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table
    create_songs_task >> load_song_dimension_table
    load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table
    create_time_task >> load_time_dimension_table
    load_time_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table
    create_users_task >> load_user_dimension_table
    load_user_dimension_table >> run_quality_checks

final_project_dag = final_project()