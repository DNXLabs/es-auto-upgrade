# es-auto-upgrade

If you are looking for something to upgrade automatically your old Elastic Search domain running on AWS you are in the right place.

The python script in this repository is responsable to upgrade your domain direct from version 2.3 to 7.4(latest), with safety checks and data integrity.


## Setup

#### Dependencies
- Python 3

#### Python Virtual Environment
```
# Create environment
python3 -m venv env

# To activate the environment
source env/bin/activate

# When you finish you can exit typing
deactivate
```

#### Install dependencies
```
pip3 install -r requirements.txt
```

## Usage

To avoid problems with your root domain this script will create one brand new domain and execute all the steps in there.

### Steps
1. Create S3 bucket to store snapshot from 2.3 domain
2. Create new Elasticsearch domain
3. Create AMI permissions
    - Create policy
    - Create Role
    - Attach policy to role
4. Register snapshot in both domains
5. Take snapshot from 2.3 and save it to S3 bucket
6. Restore snapshot from S3 into new domain
7. Delete S3 bucket
8. Delete AMI permissions
    - Detach policy from role
    - Delete policy
    - Delete Role
9. Roll upgrade from 5.1 to 5.6
10. Reindex action
11. Check domain upgrade status
12. Roll upgrade from 5.6 to 6.8
13. Reindex action
14. Check domain upgrade status
15. Roll upgrade from 6.8 to 7.4
16. Reindex action
17. Check domain data integrity


To start using it all you need is to define the `old and new domains names`, the `size of the new instance` you want, and the `region` of your `auth` and `bucket` you want.

The user which is running the script need to have permission to `CRUD` bucket and ElasticSearch domains.

> If you desire to create the new cluster manually just create it and set the `NEW_DOMAIN_NAME` variable with the previous domain created. Remember to set the version to 5.1.

### Variables
```bash
export OLD_DOMAIN_NAME=test
export NEW_DOMAIN_NAME=test-new
export AWS_REGION=ap-southeast-2
```

### Optional Variables
```bash
# Elasticsearch
export CREATE_NEW_DOMAIN=True
export NEW_INSTANCE_TYPE=m5.xlarge.elasticsearch # Optional

# S3
export BUCKET_NAME=es-automated-update # Optional
```

## Author
Managed by [DNX Solutions](contact@dnx.solutions).

## License
Apache 2 Licensed. See [LICENSE](https://github.com/DNXLabs/es-auto-upgrade/blob/master/LICENSE) for full details.