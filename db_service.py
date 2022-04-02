# -*- coding: utf-8 -*-
"""
Created on Sat Apr 2 2022

@author: Michael Lin
"""
import urllib
import sqlalchemy
import psycopg2
from sqlalchemy import event


class SqlalchemyConnection(object):
    """ SqlAlchemy connection """

    def __init__(self, dsn, app_name, uid=None, password=None):
        self._dsn = dsn
        self._app_name = app_name
        self._uid = uid
        self._password = password

    def setup(self):
        conn_str = f'DSN={self._dsn};UID={self._uid};PWD={self._password};APP={self._app_name}'
        db_params = urllib.parse.quote_plus(conn_str)
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params),
                                          isolation_level='AUTOCOMMIT')

        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        return engine


class RedshiftConnection(object):
    """ Redshift connection """

    def __init__(self, host=None, user=None, port=None, password=None, dbname=None):
        self._host = host
        self._user = user
        self._port = port
        self._password = password
        self._dbname = dbname

    @property
    def connection(self):
        class Connection(object):
            def __init__(self, host, user, port, password, dbname):
                self._host = host
                self._user = user
                self._port = port
                self._password = password
                self._dbname = dbname

            def __enter__(self):
                self._connection = psycopg2.connect(host=self._host, user=self._user, port=self._port,
                                                    password=self._password, dbname=self._dbname)
                return self._connection

            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    self._connection.close()
                except Exception as e:
                    raise e
                if exc_type is not None:
                    raise RuntimeError

        return Connection(self._host, self._user, self._port, self._password, self._dbname)
