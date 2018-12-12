import os
import psycopg2
import psycopg2.extras

class redshift_api():

    def __init__(self):
        self.redshift_host = os.environ['REDSHIFT_HOST']
        self.redshift_db_name = os.environ['REDSHIFT_DB_NAME']
        self.redshift_port = os.environ['REDSHIFT_PORT']
        self.redshift_user = os.environ['REDSHIFT_USER']
        self.redshift_password = os.environ['REDSHIFT_PASSWORD']
        self.redshift_conn = None
        self.redshift_cursor = None

    def connect(self):
        """
        Set object attributes redshift_conn and redshift_cursor with a live Redshift connection and cursor respectively.
        :return: None
        """
        self.redshift_conn = psycopg2.connect(host=self.redshift_host, dbname=self.redshift_db_name, port=self.redshift_port, user=self.redshift_user, password=self.redshift_password)
        self.redshift_cursor = self.redshift_conn.cursor()

    def disconnect(self):
        """
        Disconnect from the live Redshift connection.
        :return: None
        """
        if self.redshift_conn is not None:
            self.redshift_conn.close()

    def execute_sql(self, sql):
        """
        Execute a SQL statement in Redshift
        :param sql: The SQL statement to run
        :return: None
        """
        if self.redshift_cursor is not None:
            psycopg2.extras.execute_values(self.redshift_cursor, sql)
