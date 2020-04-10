from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        COMPUPDATE OFF
        JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        iam = aws_hook.expand_role('dwhRole')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data from destination table {self.table}')
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to table {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        if self.json_path != 'auto':
            json_path = f"s3://{self.s3_bucket}/{self.json_path}"
        else:
            json_path = self.json_path
        sql_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            iam,
            json_path
        )
        redshift.run(sql_query)




