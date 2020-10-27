from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class HasRowsOperator(BaseOperator):

    """
    Data check to ensure the Postgres table is not empty.
    """
    
    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 *args, **kwargs):
        
        super(kHasRowsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = conn_id
        self.table = table
    
    def execute(self, context):
        
        postgres_hook = PostgresHook(self.conn_id)
        rows = postgres_hook.get_records(
            f"""SELECT COUNT(*) FROM {self.table};"""
        )[0][0]
        
        # Raise ValueError if table is empty
        if rows == 0:
            raise ValueError(
                f"""
                Check failed:
                No rows in {self.table} table
                """
            )
        else:
            self.log.info(
                f"""
                Check passed:
                {self.table} table contains {rows} rows
                """
            )