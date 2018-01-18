from airflow.hooks.http_hook import HttpHook
import json
import logging


class FreshdeskHook(HttpHook):
    def __init__(
            self,
            method='GET',
            http_conn_id='http_default',
            *args,
            **kwargs):
        self._args = args
        self._kwargs = kwargs

        self.connection = self.get_connection(http_conn_id)
        self.extras = self.connection.extra_dejson

        super().__init__(method, http_conn_id)

    def run(self, endpoint, data=None, headers=None, extra_options=None, extra_args={}):

        return super().run(endpoint, data, headers, extra_options)
    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass
