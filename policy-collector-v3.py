import boto3
import json
import pandas as pd
import tempfile
import os

iam = boto3.client("iam")


def get_entity_tag_value(entity_type, entity_name, tag_key):
    """
    Fetch the value of a given tag for an IAM entity (User, Group, Role).
    Returns "" if the tag is not present.
    """
    try:
        if entity_type == "user":
            resp = iam.list_user_tags(UserName=entity_name)
            tags = resp.get("Tags", [])
        elif entity_type == "group":
            resp = iam.list_group_tags(GroupName=entity_name)
            tags = resp.get("Tags", [])
        elif entity_type == "role":
            resp = iam.list_role_tags(RoleName=entity_name)
            tags = resp.get("Tags", [])
        else:
            return ""

        for t in tags:
            if t["Key"] == tag_key:
                return t["Value"]

        return ""
    except Exception as e:
        print(f"Error fetching tags for {entity_type} {entity_name}: {e}")
        return ""


def collect_user_policies(username, tag_key):
    """Collect inline, attached, and group-inherited policies for a user."""
    policies = []
    tag_value = get_entity_tag_value("user", username, tag_key)

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
            "PolicyDocument": doc,
            "TagValue": tag_value
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
            "PolicyDocument": doc,
            "TagValue": tag_value
        })

    # Group inherited policies
    groups = iam.list_groups_for_user(UserName=username)["Groups"]
    for g in groups:
        gname = g["GroupName"]
        g_tag_value = get_entity_tag_value("group", gname, tag_key)

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
                "PolicyDocument": doc,
                "TagValue": g_tag_value
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
                "PolicyDocument": doc,
                "TagValue": g_tag_value
            })

    return policies


def collect_group_policies(groupname, tag_key):
    """Collect inline + attached policies for a group."""
    policies = []
    tag_value = get_entity_tag_value("group", groupname, tag_key)

    gp_inline = iam.list_group_policies(GroupName=groupname)["PolicyNames"]
    for gp in gp_inline:
        doc = iam.get_group_policy(GroupName=groupname, PolicyName=gp)["PolicyDocument"]
        policies.append({
            "PrincipalName": groupname,
            "PrincipalType": "Group",
            "PolicyType": "Inline",
            "PolicyName": gp,
            "PolicyArn": None,
            "PolicyDocument": doc,
            "TagValue": tag_value
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
            "PolicyDocument": doc,
            "TagValue": tag_value
        })

    return policies


def collect_role_policies(rolename, tag_key):
    """Collect inline + attached policies for a role."""
    policies = []
    tag_value = get_entity_tag_value("role", rolename, tag_key)

    inline_policies = iam.list_role_policies(RoleName=rolename)["PolicyNames"]
    for pol in inline_policies:
        doc = iam.get_role_policy(RoleName=rolename, PolicyName=pol)["PolicyDocument"]
        policies.append({
            "PrincipalName": rolename,
            "PrincipalType": "Role",
            "PolicyType": "Inline",
            "PolicyName": pol,
            "PolicyArn": None,
            "PolicyDocument": doc,
            "TagValue": tag_value
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
            "PolicyDocument": doc,
            "TagValue": tag_value
        })

    return policies


def run_collector(entity_type, input_csv, output_path, tag_key):
    """Runs the collector for a given entity type and writes JSONL output."""
    df = pd.read_csv(input_csv)
    all_policies = []

    for _, row in df.iterrows():
        name = row[0]  # assumes first col = Name (UserName, GroupName, RoleName)
        if entity_type == "user":
            all_policies.extend(collect_user_policies(name, tag_key))
        elif entity_type == "group":
            all_policies.extend(collect_group_policies(name, tag_key))
        elif entity_type == "role":
            all_policies.extend(collect_role_policies(name, tag_key))

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
        "output": "s3://bucket/output.jsonl",
        "tag_key": "Environment"   # Example tag name
      }
    """
    entity_type = event["entity_type"]
    input_path = event["input"]
    output_path = event["output"]
    tag_key = event.get("tag_key", "")

    s3 = boto3.client("s3")

    if input_path.startswith("s3://"):
        bucket, key = input_path.replace("s3://", "").split("/", 1)
        local_input = os.path.join(tempfile.gettempdir(), "input.csv")
        s3.download_file(bucket, key, local_input)
    else:
        raise ValueError("Input must be an S3 path inside Lambda")

    local_output = os.path.join(tempfile.gettempdir(), "output.jsonl")

    run_collector(entity_type, local_input, local_output, tag_key)

    if output_path.startswith("s3://"):
        bucket, key = output_path.replace("s3://", "").split("/", 1)
        s3.upload_file(local_output, bucket, key)
    else:
        raise ValueError("Output must be an S3 path inside Lambda")

    return {
        "statusCode": 200,
        "body": f"Policies collected for {entity_type}. Output written to {output_path}"
    }
