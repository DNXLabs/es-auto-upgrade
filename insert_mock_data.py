#!bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import os

host = os.environ['AWS_HOST']
region = os.environ['AWS_REGION']
service = 'es'
credentials = boto3.Session().get_credentials()
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
e1={
    "first_name":"nitin",
    "last_name":"panwar",
    "age": 27,
    "about": "Love to play cricket",
    "interests": ['sports','music'],
}
e2={
    "first_name" :  "Jane",
    "last_name" :   "Smith",
    "age" :         32,
    "about" :       "I like to collect rock albums",
    "interests":  [ "music" ]
}
e3={
    "first_name" :  "Douglas",
    "last_name" :   "Fir",
    "age" :         35,
    "about":        "I like to build cabinets",
    "interests":  [ "forestry" ]
}

for i in range(0, 10):
    es.index(index="movies", doc_type="doc", id=i, body=movie)
    print(es.get(index="movies", doc_type="doc", id=i))

es.index(index="corp",doc_type="employee",id=1,body=e1)
print(es.get(index="corp",doc_type="employee",id=1))
es.index(index="corp",doc_type="employee",id=2,body=e2)
print(es.get(index="corp",doc_type="employee",id=2))
es.index(index="corp",doc_type="employee",id=3,body=e3)
print(es.get(index="corp",doc_type="employee",id=3))

# res=es.delete(index='movies',doc_type='doc',id=5)
# print(res)
# res=es.delete(index='corp',doc_type='employee',id=1)
# print(res)
# res=es.delete(index='corp',doc_type='employee',id=2)
# print(res)
# res=es.delete(index='corp',doc_type='employee',id=3)
# print(res)