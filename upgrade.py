import boto3
import os
import time
import requests
import json
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

session = boto3.session.Session()
credentials = boto3.Session().get_credentials()

es = session.client('es')
s3 = session.client('s3')
s3_res = boto3.resource('s3')
iam = session.client('iam')
sts = session.client('sts')

# Auth
auth_region = os.getenv('AUTH_REGION')

# Elasticsearch
old_domain_name = os.getenv('OLD_DOMAIN_NAME')
new_domain_name = os.getenv('NEW_DOMAIN_NAME')
elasticsearch_version = '5.1'
instance_type = os.getenv('INSTANCE_TYPE', 'm5.xlarge.elasticsearch')

# S3
bucket_name = os.getenv('BUCKET_NAME', 'es-automated-update')
bucket_region = os.getenv('BUCKET_REGION', 'ap-southeast-2')

account_id = sts.get_caller_identity()["Account"]
service = 'es'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, auth_region, service, session_token=credentials.token)


def check_bucket_exists():
    try:
        s3.head_bucket(
            Bucket=bucket_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # We can't find the resource that you asked for.
            return False

    return True


def create_s3_bucket():
    if not check_bucket_exists():
        response = s3.create_bucket(
            ACL='private',
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': bucket_region
            }
        )
        print('Bucket created')
        return response['Location']
    else:
        print('Bucket already exists, skipping this step')

def delete_s3_bucket():
    if check_bucket_exists():
        bucket = s3_res.Bucket(bucket_name)
        bucket.objects.all().delete()
        s3.delete_bucket(
            Bucket=bucket_name,
        )
        print('Bucket deleted')
    else:
        print('Bucket do not exists, skipping this step')


def check_es_domain_exists(domain_name):
    try:
        es.describe_elasticsearch_domain(
            DomainName=domain_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            return False
    return True


def wait_es_process(domain_name):
    print('Starting ES waiting process')
    print('Processing domain...')
    while True:
        response = es.describe_elasticsearch_domain(
            DomainName=domain_name
        )
        processing_status = response['DomainStatus']['Processing']
        if processing_status == False:
            while True:
                response = es.describe_elasticsearch_domain(
                    DomainName=domain_name
                )
                if 'Endpoint' in response['DomainStatus']:
                    break
            break
        time.sleep(2)


def create_es_domain(domain_name):
    if not check_es_domain_exists(domain_name):
        response = es.create_elasticsearch_domain(
            DomainName=domain_name,
            ElasticsearchVersion=elasticsearch_version,
            ElasticsearchClusterConfig={
                'InstanceType': instance_type,
                'InstanceCount': 1,
                'DedicatedMasterEnabled': False,
            },
            EBSOptions={
                'EBSEnabled': True,
                'VolumeType': 'standard',
                'VolumeSize': 10
            },
            AccessPolicies='{"Version": "2012-10-17", "Statement": [{"Action": "es:*", "Principal":"*","Effect": "Allow", "Condition": {"IpAddress":{"aws:SourceIp":["*"]}}}]}'
        )
        print('ES domain created')

        wait_es_process(domain_name)

        return response['DomainStatus']['ARN']
    else:
        print('ES domain already exists, skipping this step')


def get_domain_host_endpoint(domain_name):

    response = es.describe_elasticsearch_domain(
        DomainName=domain_name
    )
    return response['DomainStatus']['Endpoint']


def create_policy():
    policy_document = """{
    "Version":"2012-10-17",
    "Statement":[
        {
            "Action":[
                "s3:ListBucket"
            ],
            "Effect":"Allow",
            "Resource":[
                "arn:aws:s3:::%s"
            ]
        },
        {
            "Action":[
                "s3:GetObject",
                "s3:PutObject",processing_status = response['DomainStatus']['Processing']
        if processing_status == False:
                "s3:DeleteObject",
                "iam:PassRole"
            ],
            "Effect":"Allow",
            "Resource":[
                "arn:aws:s3:::%s/*"
            ]
        }
    ]
}""" % (bucket_name, bucket_name)
    try:
        response = iam.create_policy(
            PolicyName='es-snapshot-policy',
            PolicyDocument=policy_document,
            Description='Policy to allow ES access to S3 bucket.'
        )
        print('Policy es-snapshot-policy created')
        return response['Policy']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # We can't find the resource that you asked for.
            print('Policy already exists, skipping this step')


def delete_policy():
    policy_arn = "arn:aws:iam::%s:policy/es-snapshot-policy" % (account_id)
    try:
        iam.delete_policy(
            PolicyArn=policy_arn,
        )
        print('Policy es-snapshot-policy deleted')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            print('Policy do not exist, skipping this step')


def create_role():
    assume_role_policy_document= """{
        "Version": "2012-10-17",
        "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
            "Service": "es.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
        ]
    }"""
    try:
        iam.create_role(
            RoleName='es-snapshots-role',
            AssumeRolePolicyDocument=assume_role_policy_document,
            Description='string',
        )
        print('Role es-snapshots-role created')
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # We can't find the resource that you asked for.
            print('Role already exists, skipping this step')


def delete_role():
    try:
        iam.delete_role(
            RoleName='es-snapshots-role'
        )
        print('Role es-snapshots-role deleted')
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            print('Role do not exist, skipping this step')


def attach_role_policy():
    policy_arn = "arn:aws:iam::%s:policy/es-snapshot-policy" % (account_id)
    try:
        iam.attach_role_policy(
            RoleName='es-snapshots-role',
            PolicyArn=policy_arn
        )
        print('Policy es-snapshot-policy attached to es-snapshots-role')
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # We can't find the resource that you asked for.
            print('Attachment already exists, skipping this step')


def detach_role_policy():
    policy_arn = "arn:aws:iam::%s:policy/es-snapshot-policy" % (account_id)
    try:
        iam.detach_role_policy(
            RoleName='es-snapshots-role',
            PolicyArn=policy_arn
        )
        print('Policy es-snapshot-policy dettached to es-snapshots-role')
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # We can't find the resource that you asked for.
            print('Attachment do not exist, skipping this step')


def register_snapshot(domain_name):
    PATH = '/_snapshot/es-index-backups'
    elasticsearch_host = get_domain_host_endpoint(domain_name)
    URL = 'https://' + elasticsearch_host + PATH

    headers = {"Content-Type": "application/json"}
    payload = {
        "type": "s3",
        "settings": {
            "bucket": bucket_name,
            "region": bucket_region,
            "role_arn": "arn:aws:iam::%s:role/es-snapshots-role" % (account_id)
        }
    }

    print('Registering snapshot repository ' + domain_name)
    while True:
        response = requests.put(URL, auth=awsauth, json=payload, headers=headers)
        if response.status_code == 200:
            print('Successfully registered repository ' + domain_name)
            break
        time.sleep(2)


def take_snapshot(domain_name):
    PATH = '/_snapshot/es-index-backups/snapshot'
    elasticsearch_host = get_domain_host_endpoint(domain_name)
    URL = 'https://' + elasticsearch_host + PATH

    headers = {"Content-Type": "application/json"}

    print('Taking snapshot')
    response = requests.put(URL, auth=awsauth, headers=headers)
    if response.status_code == 200:
        print('Successfully started')
        wait_snapshot_status_complete(domain_name)


def restore_snapshot(domain_name):
    PATH = '/_snapshot/es-index-backups/snapshot/_restore'
    elasticsearch_host = get_domain_host_endpoint(domain_name)
    URL = 'https://' + elasticsearch_host + PATH

    headers = {"Content-Type": "application/json"}

    print('Restoring snapshot')
    response = requests.post(URL, auth=awsauth, headers=headers)
    if response.status_code == 200:
        print('Snapshot restoring...')
    wait_snapshot_status_complete(domain_name)


def wait_snapshot_status_complete(domain_name):
    PATH = '/_snapshot/es-index-backups/snapshot/_status'
    elasticsearch_host = get_domain_host_endpoint(domain_name)
    URL = 'https://' + elasticsearch_host + PATH

    headers = {"Content-Type": "application/json"}

    print('Checking snapshot')
    while True:
        response = requests.get(URL, auth=awsauth, headers=headers)
        data = json.loads(response.text)
        if response.status_code == 500:
            continue
        if data['snapshots'][0]['state'] == 'SUCCESS':
            print(data['snapshots'][0]['state'])
            print(data['snapshots'][0]['stats'])
            break
        print(data['snapshots'][0]['state'])
        time.sleep(2)

def upgrade_es_check(domain_name, target_version):
    print('Performing upgrade check for domain ' + domain_name)
    try:
        response = es.upgrade_elasticsearch_domain(
            DomainName=domain_name,
            TargetVersion=target_version,
            PerformCheckOnly=True
        )
    except ClientError as e:
         print(e)

    time.sleep(10)

    while True:
        try:
            response = es.get_upgrade_status(
                DomainName=domain_name
            )
        except ClientError as e:
            print(e)
        step_status = response['StepStatus']
        if step_status == 'SUCCEEDED' or step_status == 'FAILED' or step_status == 'SUCCEEDED_WITH_ISSUES':
            print(step_status)
            break

def upgrade_es(domain_name, target_version):
    try:
        es.upgrade_elasticsearch_domain(
            DomainName=domain_name,
            TargetVersion=target_version,
            PerformCheckOnly=False
        )
    except ClientError as e:
         print(e)

    print('Domain upgrade started')
    wait_upgrade_finish(domain_name)

def wait_upgrade_finish(domain_name):
    time.sleep(5)
    print('Upgrade status in progress...')
    while True:
        try:
            response = es.get_upgrade_status(
                DomainName=domain_name
            )
            step_status = response['StepStatus']
            if step_status == 'SUCCEEDED' or step_status == 'SUCCEEDED_WITH_ISSUES':
                break
        except ClientError as e:
            print(e)
        time.sleep(10)

def reindex(domain_name):
    host = get_domain_host_endpoint(domain_name)
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

    indices_state = es.cluster.state()['metadata']['indices']

    for source_index in sorted(indices_state.keys(), reverse=True):

        # Skip closed indices
        if indices_state[source_index]['state'] != 'open':
            print("Opening closed index {0}".format(source_index))
            es.indices.open(source_index)
            time.sleep(5)

        destination_index = source_index.replace('-reindex', '')

        print("Reindexing data in index {0} into {1}".format(source_index, destination_index))

        result = es.reindex({
            "source": {"index": source_index},
            "dest": {"index": destination_index}
        }, wait_for_completion=True, request_timeout=300)

        print(result)

        if result['total'] and result['took'] and not result['timed_out']:
            print("Seems reindex was successfull, going to delete the old index!")
        es.indices.delete(source_index, timeout='300s')


if __name__ == '__main__':
    # Setup
    create_s3_bucket()
    create_es_domain(new_domain_name)
    create_policy()
    create_role()
    attach_role_policy()
    register_snapshot(old_domain_name)
    register_snapshot(new_domain_name)
    take_snapshot(old_domain_name)
    restore_snapshot(new_domain_name)

    # # Teardown
    delete_s3_bucket()
    detach_role_policy()
    delete_policy()
    delete_role()

    # Upgrade from 5.1 to 5.6
    upgrade_es_check(new_domain_name, '5.6')
    upgrade_es(new_domain_name, '5.6')

    # Upgrade from 5.6 to 6.8
    reindex(new_domain_name)
    upgrade_es_check(new_domain_name, '6.8')
    upgrade_es(new_domain_name, '6.8')

    # Upgrade from 6.8 to 7.4
    reindex(new_domain_name)
    upgrade_es_check(new_domain_name, '7.4')
    upgrade_es(new_domain_name, '7.4')