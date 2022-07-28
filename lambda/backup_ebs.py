import boto3


def handler(event, context):
    print('[event]', event)
    print('[context]', context)

    ec2 = boto3.client('ec2')

    params = event["queryStringParameters"]

    filters = [{'Name': 'status', 'Values': ['in-use']}]

    if params:
        if "instance_id" in params.keys() and params["instance_id"]:
            filters.append({'Name': 'attachment.instance-id', 'Values': [params["instance_id"]]})
        if "volume_id" in params.keys() and params["volume_id"]:
            filters.append({'Name': 'volume-id', 'Values': [params["volume_id"]]})

    # Get all volumes
    print('[filters]', filters)
    result = ec2.describe_volumes(Filters=filters)
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
