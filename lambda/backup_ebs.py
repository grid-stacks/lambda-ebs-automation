import time

import boto3
import botocore


def get_ebs_volume_size(client, volume_id):
    """
    Returns disk size of given ebs volume

    :param client: boto3 ec2 client
    :param volume_id: id of volume of which size is getting
    """
    response = client.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]['Size']
    print('[volume_id] [size]', response)
    return response


def extend_volume(client, volume_id, new_size):
    """
    Extend ebs volume to specified size

    :param client: boto3 ec2 client
    :param volume_id: id of volume which is extending
    :param new_size: size to be extending
    """

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
    """
    Check whether the volume is back in optimizing or completed state after extending

    :param client: boto3 ec2 client
    :param volume_id: id of volume which is checking for completion
    """

    available_states = ["optimizing", "completed"]
    state = ""
    while state not in available_states:
        state = client.describe_volumes_modifications(VolumeIds=[volume_id])['VolumesModifications'][0].get(
            'ModificationState')
        print(f'[state] Volume {volume_id} still not available, waiting...')
        time.sleep(5)
    return True


def get_main_disk(ssm, instance_id, fs):
    """
    Run command in ec2 linux instance and return attached main ebs disk

    :param ssm: boto3 ssm client
    :param instance_id: id of instance of which main disk is returning
    :param fs: root path
    """

    print('[instance]', instance_id)

    response = ssm.send_command(
        InstanceIds=(instance_id,),
        DocumentName="AWS-RunShellScript",
        DocumentVersion='$LATEST',
        Comment='Getting main driver from lambda',
        Parameters={
            'commands': ["df " + fs + " | awk '{print $1}'"]
        },
    )

    command_id = response["Command"]["CommandId"]
    print('[command id]', command_id)

    keep_waiting = None
    while keep_waiting is None:
        command_resp = ssm.list_commands(CommandId=command_id)
        if command_resp['Commands'][0]['Status'] == "InProgress" or command_resp['Commands'][0]['Status'] == "Pending":
            time.sleep(1)
        else:
            keep_waiting = 1

    output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    while output["Status"] == "InProgress":
        print('[progress status]', output['Status'])
        output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    print('[complete status]', output['Status'], output["StandardOutputContent"])

    chunks = output["StandardOutputContent"].split('\n')
    print('[chunks]', chunks)

    disk_name = chunks[1]
    print('[disk]', disk_name)

    return disk_name


def extend_disk(ssm, instance_id, disk_name):
    """
    Run command in ec2 linux instance and extend disk

    :param ssm: boto3 ssm client
    :param instance_id: id of instance of which main disk is extending
    :param disk_name: name of disk to be extending
    """

    print('[grow instance]', instance_id)
    print('[extending]', disk_name)

    response = ssm.send_command(
        InstanceIds=(instance_id,),
        DocumentName="AWS-RunShellScript",
        DocumentVersion='$LATEST',
        Comment='Extending disk from lambda',
        Parameters={
            'commands': ["sudo resize2fs " + disk_name]
        },
    )

    command_id = response["Command"]["CommandId"]
    print('[grow command id]', command_id)

    keep_waiting = None
    while keep_waiting is None:
        command_resp = ssm.list_commands(CommandId=command_id)
        if command_resp['Commands'][0]['Status'] == "InProgress" or command_resp['Commands'][0]['Status'] == "Pending":
            time.sleep(1)
        else:
            keep_waiting = 1

    output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    while output["Status"] == "InProgress":
        print('[progress status]', output['Status'])
        output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    print('[grow complete status]', output['Status'], output["StandardOutputContent"])

    if output['Status'] == "Success":
        return True
    else:
        return False


def extend_partition(ssm, instance_id, fs):
    """
    Run command in ec2 linux instance and extend partition

    :param ssm: boto3 ssm client
    :param instance_id: id of instance of which disk partition is extending
    :param fs: partition path to be extending
    """

    print('[partition instance]', instance_id)
    print('[partition]', fs)

    response = ssm.send_command(
        InstanceIds=(instance_id,),
        DocumentName="AWS-RunShellScript",
        DocumentVersion='$LATEST',
        Comment='Extending partition from lambda',
        Parameters={
            'commands': ["sudo xfs_growfs -d " + fs]
        },
    )

    command_id = response["Command"]["CommandId"]
    print('[partition command id]', command_id)

    keep_waiting = None
    while keep_waiting is None:
        command_resp = ssm.list_commands(CommandId=command_id)
        if command_resp['Commands'][0]['Status'] == "InProgress" or command_resp['Commands'][0]['Status'] == "Pending":
            time.sleep(1)
        else:
            keep_waiting = 1

    output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    while output["Status"] == "InProgress":
        print('[progress status]', output['Status'])
        output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    print('[partition complete status]', output['Status'], output["StandardOutputContent"])

    if output['Status'] == "Success":
        return True
    else:
        return False


def handler(event, context):
    """
    Main lambda handler
    """

    print('[event]', event)
    print('[context]', context)

    ec2 = boto3.client('ec2')
    ssm = boto3.client('ssm')

    # If we invoke lambda from aws api gateway and pass query parameters,
    # then the params will be in 'queryStringParameters' of event dictionary
    # We will pass three query params: inc, instance_id and volume_id
    params: dict = event["queryStringParameters"]

    # We can pass percentage of disk size increment
    # If not passed, then default increment will be 10%
    inc = params["inc"] if "inc" in params.keys() and params["inc"] else 10

    # Starting ebs disk filtering
    # We will filter disks which are in 'in-use' status only
    filters = [{'Name': 'status', 'Values': ['in-use']}]

    # Additional filtering will be added if instance_id and volume_id is passed
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

    # Looping all available volumes, taking snapshots and extending them
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

        try:
            snapshot_id = res['SnapshotId']
            snapshot_complete_waiter = ec2.get_waiter('snapshot_completed')
            snapshot_complete_waiter.wait(SnapshotIds=[snapshot_id])
        except botocore.exceptions.WaiterError as e:
            if "max attempts exceeded" in e.message:
                print(f'[error] Snapshot not completed. Exception: {e}')
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'text/plain'},
                    'body': f"Snapshot not completed. Exception: {e}"
                }
            else:
                print(f'[error] Unable to wait. Exception: {e}')
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'text/plain'},
                    'body': f"Unable to wait. Exception: {e}"
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

        # Getting ebs volume size
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

        # Extending volume
        extended = extend_volume(ec2, volume_id, new_volume_size)

        if extended["success"]:
            # Wait for volume extending confirmation
            wait_volume_modified(ec2, volume_id)
        else:
            return {'statusCode': 400, 'headers': {'Content-Type': 'text/plain'}, 'body': extended["message"]}

        try:
            # Get root disk in linux to extend
            disk_to_extend = get_main_disk(ssm, instance_id, "/")

            if disk_to_extend:
                # Extending the disk in linux
                disk_extended = extend_disk(ssm, instance_id, disk_to_extend)

                if disk_extended:
                    # Extending partition
                    extend_partition(ssm, instance_id, "/")

        except Exception as e:
            print(f'[error] Unable to get main disk. Exception: {e}')
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'text/plain'},
                'body': f"Unable to get main disk. Exception: {e}"
            }

    return {'statusCode': 200, 'headers': {'Content-Type': 'text/plain'}, 'body': 'Snapshot created successfully'}
