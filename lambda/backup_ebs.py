import boto3


def handler(event, context):
    ec2 = boto3.client('ec2')

    # Get all volumes
    result = ec2.describe_volumes(Filters=[{'Name': 'status', 'Values': ['in-use']}])
    print("available volumes")
    print(result)

    for volume in result['Volumes']:
        print(f"Backing up {volume['VolumeId']} in {volume['AvailabilityZone']}")

        # Create snapshot
        res = ec2.create_snapshot(VolumeId=volume['VolumeId'], Description='Created by backup_ebs lambda function')

        # Get snapshot resource
        ec2resource = boto3.resource('ec2')
        snapshot = ec2resource.Snapshot(res['SnapshotId'])

        volume_name = ''

        # Find name tag for volume
        if 'Tags' in volume:
            for tags in volume['Tags']:
                if tags["Key"] == 'Name':
                    volume_name = tags["Value"]
        else:
            volume_name = f"Instance: {volume['Attachments'][0]['InstanceId']}"

        # Add volume name to snapshot for easier identification
        snapshot.create_tags(Tags=[{'Key': 'Name', 'Value': volume_name}])

    return {'statusCode': 200, 'headers': {'Content-Type': 'text/plain'}, 'body': 'Snapshot created successfully'}
