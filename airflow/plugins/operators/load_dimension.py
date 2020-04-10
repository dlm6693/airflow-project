from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template = """
        TRUNCATE TABLE {table};
        INSERT INTO {table}
        {query};
        COMMIT;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id,
                 table,
                 query,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        sql_query = LoadDimensionOperator.sql_template.format(
            table=self.table,
            query=self.query
        )
        self.log.info('Loading data into table {self.table} now')
        redshift.run(sql_query)
        self.log.info('Data load complete')