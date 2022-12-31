from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dq_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Connect to Redshift')
        redshift_hook = PostgresHook(self.redshift_conn_id)
           
        error_count = 0
        failing_tests = []
        self.log.info('Starting to check data quality')    
        for check in self.dq_checks:
            records = redshift_hook.get_records(check['query'])
            if records[0][0] != check['expected_result']:
                error_count += 1
                failing_tests.append(check['query'])
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        self.log.info('Data quality checks passed!')
            
