import psycopg
from contextlib import contextmanager
from psycopg.rows import dict_row
from dagster import ConfigurableResource, EnvVar


class PostgresResource(ConfigurableResource):
    host: str = EnvVar("DB_HOST")
    dbname: str = EnvVar("DB_NAME")
    user: str = EnvVar("DB_USER")
    password: str = EnvVar("DB_PASSWORD")
    port: str = EnvVar("DB_PORT")
    sslmode: str = EnvVar("DB_SSL")

    def connect(self):
        return psycopg.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port,
            sslmode=self.sslmode,
            row_factory=dict_row,
        )

    def connect_tuple(self):
        return psycopg.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port,
            sslmode=self.sslmode,
        )

    @contextmanager
    def get_connection(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def get_tuple_connection(self):
        conn = self.connect_tuple()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
