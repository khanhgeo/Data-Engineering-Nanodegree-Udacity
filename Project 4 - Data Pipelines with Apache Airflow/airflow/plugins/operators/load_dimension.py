from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 mode,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.mode = mode

    def execute(self, context):
        self.log.info('Connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        if self.mode == 'delete-load':
            self.log.info('Deleting data in dimension tables')
            redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info('Loading data into dimension tables')
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info('Completed data loading into dimension tables')
