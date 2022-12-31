from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info('Connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Inserting data into fact table')
        redshift.run(f'INSERT INTO {self.table} {self.query}')
        self.log.info('Completed inserting data into fact table')
        
