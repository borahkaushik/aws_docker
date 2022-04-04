import urllib.request
import boto3

def ec2_shutdown():
    id = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().decode()
    print('instance ID:', id)
    client = boto3.client('ec2')
    response = client.stop_instances(
        InstanceIds=[
            id,
        ]
    )
    print(response)