from tempfile import NamedTemporaryFile
from dateutil.parser import parse
import logging
import json

from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook


class FreshdeskToS3Operator(BaseOperator, SkipMixin):
    """
    Trello to S3 Operator
    :param freshdesk_conn_id:       The Airflow id used to store the Freshdesk
                                    credentials.
    :type freshdesk_conn_id:        string
    :param freshdesk_endpoint:      The endpoint to retrive data from.
                                    Implemented for:
                                        - agents
                                        - companies
                                        - contacts
                                        - conversations
                                        - groups
                                        - roles
                                        - satisfaction_ratings
                                        - tickets
                                        - time_entries
    :type freshdesk_endpoint:       string
    :param s3_conn_id:              The Airflow connection id used to store
                                    the S3 credentials.
    :type s3_conn_id:               string
    :param s3_bucket:               The S3 bucket to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    :param s3_key:                  The S3 key to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    :param updated_at:              replication key
    :type updated_at:               string
    """

    template_fields = ('s3_key',
                       'updated_at')

    @apply_defaults
    def __init__(self,
                 freshdesk_conn_id,
                 freshdesk_endpoint,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 updated_at=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.freshdesk_conn_id = freshdesk_conn_id
        self.freshdesk_endpoint = freshdesk_endpoint
        self.updated_at = updated_at

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.hook = None

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Freshdesk model
        and write it to a file.
        """
        self.updated_at = parse(self.updated_at) if self.updated_at else None

        self.hook = HttpHook(method='GET', http_conn_id=self.freshdesk_conn_id)

        mappings = {
            'satisfaction_ratings': {
                'endpoint': 'surveys/satisfaction_ratings'
            },
            'conversations': {
                'parent': 'tickets',
                'endpoint': 'tickets/{}/conversations'
            },
            'time_entries': {
                'parent': 'tickets',
                'endpoint': 'tickets/{}/time_entries'
            }
        }

        if self.freshdesk_endpoint in mappings:
            if 'parent' in mappings[self.freshdesk_endpoint]:
                parent_endpoint = mappings[self.freshdesk_endpoint]['parent']
                parents = self.hook.run(parent_endpoint).json()
                ids = [parent['id'] for parent in parents]
                results = []

                for parent_id in ids:
                    response = self.get_all(
                        mappings[self.freshdesk_endpoint]['endpoint'].format(parent_id))
                    results.extend(response)
            else:
                results = self.get_all(
                    mappings[self.freshdesk_endpoint]['endpoint'])
        else:
            results = self.get_all(self.freshdesk_endpoint)

        if len(results) == 0 or results is None:
            logging.info("No records pulled from Freshdesk.")
            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)
            return True

        else:
            # Write the results to a temporary file and save that file to s3.
            with NamedTemporaryFile("w") as tmp:
                for result in results:
                    tmp.write(json.dumps(result) + '\n')

                tmp.flush()

                dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )
                dest_s3.connection.close()
                tmp.close()

    def get_all(self, endpoint):
        """
        Get all pages.
        """
        response = self.hook.run(endpoint)
        results = response.json()

        while 'link' in response.headers:
            response = self.hook.run(response.headers['link'])
            results.extend(response.json())

        # filter results
        if self.updated_at:
            results = [x for x in results
                       if ('updated_at' not in x) or
                       (parse(x['updated_at']).replace(tzinfo=None) > self.updated_at)]

        return results
