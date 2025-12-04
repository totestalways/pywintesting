import json
import time
import boto3


def get_running_instance_by_name(ec2_client, name_tag_key: str, name_value: str):
    """
    Find a *running* EC2 instance by its Name (or other tag key).
    Returns the instance dict or None if not found.
    """
    filters = [
        {"Name": f"tag:{name_tag_key}", "Values": [name_value]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]

    resp = ec2_client.describe_instances(Filters=filters)
    for reservation in resp.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            return instance  # first match

    return None


def run_commands_via_ssm(ssm_client, instance_id: str, commands, max_wait_seconds: int):
    """
    Run shell commands on a single instance via SSM.
    Returns dict with Status, Stdout, Stderr.
    """
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
    )

    command_id = response["Command"]["CommandId"]

    start_time = time.time()
    while True:
        try:
            inv = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id
            )

            status = inv["Status"]
            if status in ("Success", "Failed", "TimedOut", "Cancelled"):
                return {
                    "Status": status,
                    "Stdout": inv.get("StandardOutputContent", ""),
                    "Stderr": inv.get("StandardErrorContent", "")
                }

        except ssm_client.exceptions.InvocationDoesNotExist:
            # SSM still propagating
            pass

        if time.time() - start_time > max_wait_seconds:
            return {
                "Status": "TimedOut",
                "Stdout": "",
                "Stderr": "Timed out waiting for SSM command result"
            }

        time.sleep(1)


def lambda_handler(event, context):
    """
    Expected event fields (TOP-LEVEL ONLY):

      REQUIRED:
        - name_of_the_machine  (e.g. "MYAPP")

      OPTIONAL:
        - target_region        (e.g. "eu-west-1", default: "eu-west-1")
        - name_tag_key         (tag key, default: "Name")
        - max_wait_seconds     (int, default: 60)

    Resulting EC2 Name tags:
      MACHINE-{name_of_the_machine}-BLUE-EC2
      MACHINE-{name_of_the_machine}-GREEN-EC2
    """

    # REQUIRED
    name_of_the_machine = event.get("name_of_the_machine")
    if not name_of_the_machine:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "name_of_the_machine not found in event"})
        }

    # OPTIONAL – all from event.get with defaults
    target_region = event.get("target_region", "eu-west-1")
    name_tag_key = event.get("name_tag_key", "Name")
    max_wait_seconds = int(event.get("max_wait_seconds", 60))

    # --- Build EC2 Name tag values ---
    blue_name = f"MACHINE-{name_of_the_machine}-BLUE-EC2"
    green_name = f"MACHINE-{name_of_the_machine}-GREEN-EC2"

    # --- Create clients for the chosen region ---
    ec2 = boto3.client("ec2", region_name=target_region)
    ssm = boto3.client("ssm", region_name=target_region)

    # --- Look up BLUE and GREEN instances ---
    blue_instance = get_running_instance_by_name(ec2, name_tag_key, blue_name)
    green_instance = get_running_instance_by_name(ec2, name_tag_key, green_name)

    target_instance = None
    active_color = None
    reasons = []

    if blue_instance and not green_instance:
        target_instance = blue_instance
        active_color = "BLUE"
    elif green_instance and not blue_instance:
        target_instance = green_instance
        active_color = "GREEN"
    elif not blue_instance and not green_instance:
        reasons.append("No running BLUE or GREEN instance found.")
    else:
        reasons.append("Both BLUE and GREEN instances are running, cannot decide which is active.")

    if not target_instance:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No active instance selected",
                "blue_running": bool(blue_instance),
                "green_running": bool(green_instance),
                "reasons": reasons,
                "region": target_region,
                "blue_name": blue_name,
                "green_name": green_name,
            }, indent=2),
        }

    instance_id = target_instance["InstanceId"]

    # --- Define the commands to run on the active version ---
    commands = [
        "echo 'Hello from Lambda via SSM on the active instance'",
        "hostname",
        "uptime",
        # add more commands here
    ]

    ssm_result = run_commands_via_ssm(ssm, instance_id, commands, max_wait_seconds)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "region": target_region,
            "name_of_the_machine": name_of_the_machine,
            "active_color": active_color,
            "target_instance_id": instance_id,
            "target_instance_name": blue_name if active_color == "BLUE" else green_name,
            "ssm_result": ssm_result,
        }, indent=2),
    }
