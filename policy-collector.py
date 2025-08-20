import boto3
import pandas as pd
import json
from io import StringIO

s3 = boto3.client("s3")
iam = boto3.client("iam")

def read_csv_from_s3(s3_uri):
    """Download CSV from S3 into Pandas DataFrame"""
    if not s3_uri.startswith("s3://"):
        return pd.read_csv(s3_uri)

    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])

def write_jsonl_to_s3(records, s3_uri):
    """Write JSONL records to S3 or local file"""
    lines = [json.dumps(r) for r in records]
    content = "\n".join(lines)

    if not s3_uri.startswith("s3://"):
        with open(s3_uri, "w") as f:
            f.write(content)
        return

    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))

def get_policy_doc(policy_arn):
    """Fetch managed policy default version"""
    policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
    version_id = policy["DefaultVersionId"]
    version = iam.get_policy_version(PolicyArn=policy_arn, VersionId=version_id)
    return version["PolicyVersion"]["Document"]

def collect_user_policies(user_name):
    records = []

    # Attached managed policies
    attached = iam.list_attached_user_policies(UserName=user_name)["AttachedPolicies"]
    for ap in attached:
        records.append({
            "PrincipalName": user_name,
            "PrincipalType": "User",
            "PolicyType": "Managed",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": get_policy_doc(ap["PolicyArn"])
        })

    # Inline policies
    inline = iam.list_user_policies(UserName=user_name)["PolicyNames"]
    for pn in inline:
        pol = iam.get_user_policy(UserName=user_name, PolicyName=pn)
        records.append({
            "PrincipalName": user_name,
            "PrincipalType": "User",
            "PolicyType": "Inline",
            "PolicyName": pn,
            "PolicyArn": None,
            "PolicyDocument": pol["PolicyDocument"]
        })
    return records

def collect_group_policies(group_name):
    records = []

    # Attached managed policies
    attached = iam.list_attached_group_policies(GroupName=group_name)["AttachedPolicies"]
    for ap in attached:
        records.append({
            "PrincipalName": group_name,
            "PrincipalType": "Group",
            "PolicyType": "Managed",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": get_policy_doc(ap["PolicyArn"])
        })

    # Inline policies
    inline = iam.list_group_policies(GroupName=group_name)["PolicyNames"]
    for pn in inline:
        pol = iam.get_group_policy(GroupName=group_name, PolicyName=pn)
        records.append({
            "PrincipalName": group_name,
            "PrincipalType": "Group",
            "PolicyType": "Inline",
            "PolicyName": pn,
            "PolicyArn": None,
            "PolicyDocument": pol["PolicyDocument"]
        })
    return records

def collect_role_policies(role_name):
    records = []

    # Attached managed policies
    attached = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
    for ap in attached:
        records.append({
            "PrincipalName": role_name,
            "PrincipalType": "Role",
            "PolicyType": "Managed",
            "PolicyName": ap["PolicyName"],
            "PolicyArn": ap["PolicyArn"],
            "PolicyDocument": get_policy_doc(ap["PolicyArn"])
        })

    # Inline policies
    inline = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
    for pn in inline:
        pol = iam.get_role_policy(RoleName=role_name, PolicyName=pn)
        records.append({
            "PrincipalName": role_name,
            "PrincipalType": "Role",
            "PolicyType": "Inline",
            "PolicyName": pn,
            "PolicyArn": None,
            "PolicyDocument": pol["PolicyDocument"]
        })
    return records

def run_collector(entity_type, input_csv, output_file):
    df = read_csv_from_s3(input_csv)
    records = []

    if entity_type == "user":
        for _, row in df.iterrows():
            records.extend(collect_user_policies(row["UserName"]))

    elif entity_type == "group":
        for _, row in df.iterrows():
            records.extend(collect_group_policies(row["GroupName"]))

    elif entity_type == "role":
        for _, row in df.iterrows():
            records.extend(collect_role_policies(row["RoleName"]))

    else:
        raise ValueError("entity-type must be one of: user, group, role")

    write_jsonl_to_s3(records, output_file)
    return {"status": "success", "records_collected": len(records), "output": output_file}

def lambda_handler(event, context):
    """
    Expected event format:
    {
      "entity_type": "user",   # or group/role
      "input_csv": "s3://bucket/input.csv",
      "output_file": "s3://bucket/output.jsonl"
    }
    """
    try:
        entity_type = event["entity_type"]
        input_csv = event["input_csv"]
        output_file = event["output_file"]

        result = run_collector(entity_type, input_csv, output_file)
        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}
