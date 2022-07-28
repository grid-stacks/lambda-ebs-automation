import time

import boto3


def get_ebs_volume_size(client, volume_id):
    response = client.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]['Size']
    print('[volume_id] [size]', response)
    return response


def extend_volume(client, volume_id, new_size):
    print(f'[extending] Going to extend volume {volume_id} to {new_size}G')
    try:
        client.modify_volume(
            VolumeId=volume_id,
            Size=new_size
        )
        return {"success": True, "message": f"Volume {volume_id} extended successfully to {new_size}G"}
    except Exception as e:
        print(f'[error] Unable to extend volume. Exception: {e}')
        return {"success": False, "message": f"Unable to extend volume. Exception: {e}"}


def wait_volume_modified(client, volume_id):
    available_states = ["optimizing", "completed"]
    state = ""
    while state not in available_states:
        state = client.describe_volumes_modifications(VolumeIds=[volume_id])['VolumesModifications'][0].get(
            'ModificationState')
        print(f'[state] Volume {volume_id} still not available, waiting...')
        time.sleep(5)
    return True


def handler(event, context):
    print('[event]', event)
    print('[context]', context)

    ec2 = boto3.client('ec2')

    params: dict = event["queryStringParameters"]

    inc = params["inc"] if "inc" in params.keys() and params["inc"] else 10

    filters = [{'Name': 'status', 'Values': ['in-use']}]

    if params:
        if "instance_id" in params.keys() and params["instance_id"]:
            filters.append({'Name': 'attachment.instance-id', 'Values': [params["instance_id"]]})
        if "volume_id" in params.keys() and params["volume_id"]:
            filters.append({'Name': 'volume-id', 'Values': [params["volume_id"]]})

    print('[filters]', filters)

    # Get all volumes
    try:
        result = ec2.describe_volumes(Filters=filters)
        print("available volumes")
        print(result)
    except Exception as e:
        print(f'[error] Unable to filter volumes. Exception: {e}')
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'text/plain'},
            'body': f"Unable to filter volumes. Exception: {e}"
        }

    for volume in result['Volumes']:
        volume_id = volume['VolumeId']
        instance_id = volume['Attachments'][0]['InstanceId']

        print(f"Backing up {volume_id} in {volume['AvailabilityZone']}")

        # Create snapshot
        try:
            res = ec2.create_snapshot(VolumeId=volume_id, Description='Created by backup_ebs lambda function')
        except Exception as e:
            print(f'[error] Unable to create snapshot. Exception: {e}')
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'text/plain'},
                'body': f"Unable to create snapshot. Exception: {e}"
            }

        # Get snapshot resource
        try:
            ec2resource = boto3.resource('ec2')
            snapshot = ec2resource.Snapshot(res['SnapshotId'])

            volume_name = ''

            # Find name tag for volume
            if 'Tags' in volume:
                for tags in volume['Tags']:
                    if tags["Key"] == 'Name':
                        volume_name = tags["Value"]
            else:
                volume_name = f"Instance: {instance_id}"

            # Add volume name to snapshot for easier identification
            snapshot.create_tags(Tags=[{'Key': 'Name', 'Value': volume_name}])

        except Exception as e:
            print(f'[error] Unable to process snapshot. Exception: {e}')
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'text/plain'},
                'body': f"Unable to process snapshot. Exception: {e}"
            }

        try:
            volume_size = get_ebs_volume_size(ec2, volume_id)
        except Exception as e:
            print(f'[error] Unable to get volume size. Exception: {e}')
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'text/plain'},
                'body': f"Unable to get volume size. Exception: {e}"
            }

        new_volume_size = int(volume_size * (1 + inc / 100))
        print('[new volume size]', new_volume_size)

        extended = extend_volume(ec2, volume_id, new_volume_size)

        if extended["success"]:
            wait_volume_modified(ec2, volume_id)
        else:
            return {'statusCode': 400, 'headers': {'Content-Type': 'text/plain'}, 'body': extended["message"]}

    return {'statusCode': 200, 'headers': {'Content-Type': 'text/plain'}, 'body': 'Snapshot created successfully'}
