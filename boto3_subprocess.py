import json
import os
import subprocess
import tempfile
import boto3


# ---------- IMPORTANT ----------
# Your PEM key must be placed in a Lambda layer at:
# /opt/keys/mykey.pem
# -------------------------------
PEM_PATH = "/opt/keys/mykey.pem"


def load_pem_from_layer() -> str:
    """Read the PEM key from the layer-mounted path."""
    with open(PEM_PATH, "r", encoding="utf-8") as f:
        return f.read()


def run_ssh_command(host, username, pem_key_str, commands):
    """Run SSH commands using the ssh binary via subprocess."""
    
    # Write PEM to /tmp as a temp file (ssh requires file path)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(pem_key_str.encode("utf-8"))
        key_path = f.name

    os.chmod(key_path, 0o600)

    # Join commands with "; " to run in one SSH session
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-i", key_path,
        f"{username}@{host}",
        " ; ".join(commands),
    ]

    proc = subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=30,
    )

    # Cleanup temporary key
    os.remove(key_path)

    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def get_running_instance_by_name(ec2_client, name_tag_key: str, name_value: str):
    """Find a running EC2 instance by Name tag."""
    filters = [
        {"Name": f"tag:{name_tag_key}", "Values": [name_value]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]

    resp = ec2_client.describe_instances(Filters=filters)
    for reservation in resp.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            return instance
    return None


def lambda_handler(event, context):
    """
    Expected event fields:

      REQUIRED:
        - name_of_the_machine  (e.g. "MYAPP")

      OPTIONAL:
        - target_region        (default: "eu-west-1")
        - name_tag_key         (default: "Name")
        - ssh_username         (default: "ec2-user")
        - use_private_ip       (default: True)
    """

    # --------- Inputs ----------
    name_of_the_machine = event.get("name_of_the_machine")
    if not name_of_the_machine:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "name_of_the_machine is required"})
        }

    target_region = event.get("target_region", "eu-west-1")
    name_tag_key = event.get("name_tag_key", "Name")
    ssh_username = event.get("ssh_username", "ec2-user")
    use_private_ip = event.get("use_private_ip", True)

    # --------- Build BLUE/GREEN names ----------
    blue_name = f"MACHINE-{name_of_the_machine}-BLUE-EC2"
    green_name = f"MACHINE-{name_of_the_machine}-GREEN-EC2"

    ec2 = boto3.client("ec2", region_name=target_region)

    blue_instance = get_running_instance_by_name(ec2, name_tag_key, blue_name)
    green_instance = get_running_instance_by_name(ec2, name_tag_key, green_name)

    target_instance = None
    active_color = None
    reasons = []

    # Decide which instance is active
    if blue_instance and not green_instance:
        target_instance = blue_instance
        active_color = "BLUE"
    elif green_instance and not blue_instance:
        target_instance = green_instance
        active_color = "GREEN"
    elif not blue_instance and not green_instance:
        reasons.append("No BLUE or GREEN instance is running.")
    else:
        reasons.append("Both BLUE and GREEN are running — cannot pick automatically.")

    if not target_instance:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No active EC2 selected",
                "blue_running": bool(blue_instance),
                "green_running": bool(green_instance),
                "reasons": reasons,
                "region": target_region
            }, indent=2)
        }

    instance_id = target_instance["InstanceId"]

    # Host (IP) to connect
    host = (
        target_instance.get("PrivateIpAddress")
        if use_private_ip else
        target_instance.get("PublicIpAddress")
    )

    if not host:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "No private or public IP available"})
        }

    # --------- Load PEM key from layer ----------
    pem_key = load_pem_from_layer()

    # --------- Commands to run over SSH ----------
    commands = [
        "echo 'Hello from Lambda via SSH'",
        "hostname",
        "uptime"
    ]

    ssh_result = run_ssh_command(host, ssh_username, pem_key, commands)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "region": target_region,
            "active_color": active_color,
            "target_instance_id": instance_id,
            "target_host": host,
            "ssh_result": ssh_result
        }, indent=2)
    }
