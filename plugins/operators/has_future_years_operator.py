from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

from warnings import warn

class CheckFutureYearsOperator(BaseOperator):
    """
    Checks number of observations in Postgres table with a year in the future.
    """
    
    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 *args, **kwargs):
        
        super(CheckFutureYearsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = conn_id
        self.postgres_table_name = table
    
    def execute(self, context):
        
        postgres_hook = PostgresHook(self.conn_id)
        future_years = postgres_hook.get_records(
            f"""
            SELECT COUNT(*)
            FROM {self.table}
            WHERE date > date_part('date', current_date);
            """
        )[0][0]
        
        # Output warning if there are observations with future years
        if n_future_years > 0:
            warn(
                f"""
                There are {future_years} observations in {self.table} table with year in the future
                """
            )