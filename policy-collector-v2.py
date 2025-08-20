import boto3
import json
import pandas as pd
import tempfile
import os

iam = boto3.client("iam")


def collect_user_policies(username):
    """Collect inline, attached, and group-inherited policies for a user."""
    policies = []

    # Inline user policies
    inline_policies = iam.list_user_policies(UserName=username)["PolicyNames"]
    for pol in inline_policies:
        doc = iam.get_user_policy(UserName=username, PolicyName=pol)["PolicyDocument"]
        policies.append({
            "PrincipalName": username,
            "PrincipalType": "User",
            "PolicyType": "Inline",
            "PolicyName": pol,
            "PolicyArn": None,
            "PolicyDocument": doc
        })

    # Attached user managed policies
    attached_policies = iam.list_attached_user_policies(UserName=username)["AttachedPolicies"]
    for ap in attached_policies:
        version = iam.get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
        doc = iam.get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=version)["PolicyVersion"]["Document"]
        policies.append({
            "PrincipalName": username,
            "PrincipalType": "User",
            "PolicyType": "UserAttached",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": doc
        })

    # Group inherited policies
    groups = iam.list_groups_for_user(UserName=username)["Groups"]
    for g in groups:
        gname = g["GroupName"]

        # Inline group policies
        gp_inline = iam.list_group_policies(GroupName=gname)["PolicyNames"]
        for gp in gp_inline:
            doc = iam.get_group_policy(GroupName=gname, PolicyName=gp)["PolicyDocument"]
            policies.append({
                "PrincipalName": username,
                "PrincipalType": "User",
                "PolicyType": "GroupInherited",
                "PolicyName": gp,
                "PolicyArn": None,
                "PolicyDocument": doc
            })

        # Attached group policies
        gp_attached = iam.list_attached_group_policies(GroupName=gname)["AttachedPolicies"]
        for ap in gp_attached:
            version = iam.get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
            doc = iam.get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=version)["PolicyVersion"]["Document"]
            policies.append({
                "PrincipalName": username,
                "PrincipalType": "User",
                "PolicyType": "GroupInherited",
                "PolicyName": ap["PolicyName"],
                "PolicyArn": ap["PolicyArn"],
                "PolicyDocument": doc
            })

    return policies


def collect_group_policies(groupname):
    """Collect inline + attached policies for a group."""
    policies = []
    gp_inline = iam.list_group_policies(GroupName=groupname)["PolicyNames"]
    for gp in gp_inline:
        doc = iam.get_group_policy(GroupName=groupname, PolicyName=gp)["PolicyDocument"]
        policies.append({
            "PrincipalName": groupname,
            "PrincipalType": "Group",
            "PolicyType": "Inline",
            "PolicyName": gp,
            "PolicyArn": None,
            "PolicyDocument": doc
        })

    gp_attached = iam.list_attached_group_policies(GroupName=groupname)["AttachedPolicies"]
    for ap in gp_attached:
        version = iam.get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
        doc = iam.get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=version)["PolicyVersion"]["Document"]
        policies.append({
            "PrincipalName": groupname,
            "PrincipalType": "Group",
            "PolicyType": "Attached",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": doc
        })

    return policies


def collect_role_policies(rolename):
    """Collect inline + attached policies for a role."""
    policies = []
    inline_policies = iam.list_role_policies(RoleName=rolename)["PolicyNames"]
    for pol in inline_policies:
        doc = iam.get_role_policy(RoleName=rolename, PolicyName=pol)["PolicyDocument"]
        policies.append({
            "PrincipalName": rolename,
            "PrincipalType": "Role",
            "PolicyType": "Inline",
            "PolicyName": pol,
            "PolicyArn": None,
            "PolicyDocument": doc
        })

    attached_policies = iam.list_attached_role_policies(RoleName=rolename)["AttachedPolicies"]
    for ap in attached_policies:
        version = iam.get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
        doc = iam.get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=version)["PolicyVersion"]["Document"]
        policies.append({
            "PrincipalName": rolename,
            "PrincipalType": "Role",
            "PolicyType": "Attached",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": doc
        })

    return policies


def run_collector(entity_type, input_csv, output_path):
    """Runs the collector for a given entity type and writes JSONL output."""
    df = pd.read_csv(input_csv)
    all_policies = []

    for _, row in df.iterrows():
        name = row[0]  # assumes first col = Name (UserName, GroupName, RoleName)
        if entity_type == "user":
            all_policies.extend(collect_user_policies(name))
        elif entity_type == "group":
            all_policies.extend(collect_group_policies(name))
        elif entity_type == "role":
            all_policies.extend(collect_role_policies(name))

    with open(output_path, "w") as f:
        for rec in all_policies:
            f.write(json.dumps(rec) + "\n")

    return output_path


def lambda_handler(event, context):
    """
    Lambda entrypoint.
    Expects event with:
      {
        "entity_type": "user" | "group" | "role",
        "input": "s3://bucket/input.csv",
        "output": "s3://bucket/output.jsonl"
      }
    """
    entity_type = event["entity_type"]
    input_path = event["input"]
    output_path = event["output"]

    # S3 setup
    s3 = boto3.client("s3")

    # Download input CSV from S3 to /tmp
    if input_path.startswith("s3://"):
        bucket, key = input_path.replace("s3://", "").split("/", 1)
        local_input = os.path.join(tempfile.gettempdir(), "input.csv")
        s3.download_file(bucket, key, local_input)
    else:
        raise ValueError("Input must be an S3 path inside Lambda")

    # Local output path
    local_output = os.path.join(tempfile.gettempdir(), "output.jsonl")

    # Run collector
    run_collector(entity_type, local_input, local_output)

    # Upload result back to S3
    if output_path.startswith("s3://"):
        bucket, key = output_path.replace("s3://", "").split("/", 1)
        s3.upload_file(local_output, bucket, key)
    else:
        raise ValueError("Output must be an S3 path inside Lambda")

    return {
        "statusCode": 200,
        "body": f"Policies collected for {entity_type}. Output written to {output_path}"
    }
