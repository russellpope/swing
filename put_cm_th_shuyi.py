import boto3
import sys
import time
from pprint import pprint
import random
import string


### c05
#access_key_id = 'PSFBSAZRLFKKILIFLCBDIDNBPBBPBCCIBANOOIBBBJH'
#secret_access_key = 'E1F118EED018221f11+ebd7+9FFD025880d94da1FIDB'
#endpoint_url = 'http://10.60.75.140'

### thunderball
access_key_id = 'PSFBSAZRPABPKDBICLEPDCNNOAGNLBHGNAMMKBGFAO'
secret_access_key = 'D6EC1ACC0d671bd60+2b7028EDA4E9a89e5f8fOJLL'
#endpoint_url = 'http://10.64.6.186'
endpoint_url = 'http://10.72.48.100'

### kodama
#access_key_id = 'PSFBIAZFEAAAMDDK'
#secret_access_key = '22EC00004+01ba/53FCB69460cee2ef/DEKFKHJLIIANLCHB'
#endpoint_url = 'http://10.64.160.100'

### spidey
#access_key_id = 'PSFBSAZRCJPJKFBABCKEDNJMOOKPGBKLMDKGNBLIGM'
#secret_access_key = '208C1D6A3cba16fae+5442/30EB4A993bf05aedDKPB'
#endpoint_url = 'http://10.58.225.20'

### robin
#access_key_id = 'PSFBSAZRIGADDEBODOLGNLKLNHHIAEHIOAIGLNHOK'
#secret_access_key = 'B152B680e8740877/2c029281F8972fcf6a8eHNME'
#endpoint_url = 'http://10.64.160.110'

#bucket_name = 'shuyi'
bucket_name = 'th1'
#object_name = 'obj-0922-1'

### updated on may12 
object_name = {'a'*900}
#object_name = {'a'*512}
#object_name = {'a'*30}

# bucket_name = sys.argv[1]
# object_name = sys.argv[2]
session = boto3.session.Session()
res = session.resource(service_name='s3', use_ssl=False, aws_access_key_id=access_key_id,
                       aws_secret_access_key=secret_access_key, endpoint_url=endpoint_url)
client = res.meta.client
bucket = res.Bucket(bucket_name)
body = 'hello world'

#bucket_name_prefix = 'shuyi'
bucket_name_prefix = 'th'
bucket_number = 1000
object_number = 1000000000

def apply_lifecycle_policy_multiple_rules(client, bucket_name, prefix=None, expires=1, total_count=2):
    count = 0
    rules = []
    expires = 0
    if not prefix:
        prefix = ''.join(random.choices(string.ascii_lowercase, k=10))
    while count < total_count:
        #pre = prefix + str(count)
        pre = ''.join(random.choices(string.ascii_lowercase + string.ascii_uppercase, k=100))
        #expires += 1
        expires = random.randint(1,1000)
        rule = {
            'Filter': {
                'Prefix': pre,
            },
            'Status': 'Enabled',
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': expires
            },
        }
        rules.append(rule)
        count += 1

    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': rules
        }
    )

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

while True:

    body = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=6000))

    customer = "customer-abcdefghijklmnopqrstuvwxyz"
    metadata = "metadata-abcdefghijklmnopqrstuvwxyz"
    k = 1
    metadatas = {}

    while k <= 25:
        metadatas['{}{}'.format(customer, k)] = '{}{}'.format(metadata, k)
        k += 1

    #pprint(metadatas)

    for l in range(object_number):
        object_name = randomString(stringLength=100)
        response = client.put_object(Bucket=bucket_name, Key=object_name, Body=body, Metadata=metadatas)
        #pprint(metadata)
        #pprint(response)
        #pprint(object_name)


    time.sleep (10)
