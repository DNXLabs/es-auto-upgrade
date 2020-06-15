#!bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
from datetime import datetime
import time
from requests_aws4auth import AWS4Auth
import boto3
import os


session = boto3.session.Session()
credentials = boto3.Session().get_credentials()

es = session.client('es')

def get_domain_host_endpoint(domain_name):

    response = es.describe_elasticsearch_domain(
        DomainName=domain_name
    )
    return response['DomainStatus']['Endpoint']

host = get_domain_host_endpoint(os.environ['NEW_DOMAIN_NAME'])
region = os.environ['AWS_REGION']
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
        es.indices.delete(source_index, timeout='300s')
        print("Old indexes deleted!")

indices_state = es.cluster.state()['metadata']['indices']

for source_index in sorted(indices_state.keys(), reverse=True):

    # Skip closed indices
    if indices_state[source_index]['state'] != 'open':
        print("Opening closed index {0}".format(source_index))
        es.indices.open(source_index)
        time.sleep(5)

    destination_index = source_index.replace('-reindex','')

    print("Reindexing data in index {0} into {1}".format(source_index, destination_index))

    result = es.reindex({
        "source": {"index": source_index},
        "dest": {"index": destination_index}
    }, wait_for_completion=True, request_timeout=300)

    print(result)

    if result['total'] and result['took'] and not result['timed_out']:
        es.indices.delete(source_index, timeout='300s')
        print("Old indexes deleted!")