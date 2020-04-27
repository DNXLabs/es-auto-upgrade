#!bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
from datetime import datetime
import time
from requests_aws4auth import AWS4Aut
import boto3
import os

host = os.environ['AWS_HOST']
region = os.environ['AWS_REGION']
credentials = boto3.Session().get_credentials()
service = 'es'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

indices_state = es.cluster.state()['metadata']['indices']

for source_index in sorted(indices_state.keys(), reverse=True):

    # Skip closed indices
    if indices_state[source_index]['state'] != 'open':
        print("Opening closed index {0}".format(source_index))
        es.indices.open(source_index)
        time.sleep(5)

    destination_index = source_index + '-reindex'

    print("Reindexing data in index {0} into {1}".format(source_index, destination_index))

    result = es.reindex({
        "source": {"index": source_index},
        "dest": {"index": destination_index}
    }, wait_for_completion=True, request_timeout=300)

    print(result)

    if result['total'] and result['took'] and not result['timed_out']:
        print("Seems reindex was successfull, going to delete the old index!")
    es.indices.delete(source_index, timeout='300s')