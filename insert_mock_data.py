#!bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
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

host = get_domain_host_endpoint(os.environ['OLD_DOMAIN_NAME'])
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
print(es)

movie = {
    "title": "Moneyball",
    "director": "Bennett Miller",
    "year": "2011"
}

employees = [
    {
        "first_name":"nitin",
        "last_name":"panwar",
        "age": 27,
        "about": "Love to play cricket",
        "interests": ['sports','music'],
    },
    {
        "first_name" :  "Jane",
        "last_name" :   "Smith",
        "age" :         32,
        "about" :       "I like to collect rock albums",
        "interests":  [ "music" ]
    },
    {
        "first_name" :  "Douglas",
        "last_name" :   "Fir",
        "age" :         35,
        "about":        "I like to build cabinets",
        "interests":  [ "forestry" ]
    }
]


es.index(index="movies", doc_type="doc", id=10, body=movie)
print(es.get(index="movies", doc_type="doc", id=10))

for i in range(0, 3):
    es.index(index="corp",doc_type="employee",id=i+1,body=employees[i])
    print(es.get(index="corp",doc_type="employee",id=i+1))