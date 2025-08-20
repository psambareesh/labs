import json
import csv
import re
import os
import boto3
from io import StringIO
from collections import defaultdict

s3 = boto3.client("s3")

# ---------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------

def load_jsonl_from_s3(bucket, key):
    """
    Reads a JSONL file from S3 and returns a list of dicts.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8").splitlines()
    return [json.loads(line) for line in data if line.strip()]


def write_csv_to_s3(bucket, key, rows, fieldnames):
    """
    Writes list of dicts as CSV into S3.
    """
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode("utf-8"))


def expand_actions(action_field):
    if isinstance(action_field, str):
        return [action_field]
    elif isinstance(action_field, list):
        return action_field
    return []


def classify_action(action):
    if action == "*":
        return ("ALL_SERVICES", "Admin")

    m = re.match(r"([a-z0-9-]+):(.*)", action)
    if not m:
        return ("UNKNOWN", "Unknown")
    service, op = m.groups()

    if op == "*":
        return (service, "Admin")
    if op.startswith(("Get", "Head", "Read")):
        return (service, "Read")
    elif op.startswith(("List", "Describe")):
        return (service, "List")
    elif op.startswith(("Create", "Put", "Delete", "Update", "Start", "Stop", "Invoke")):
        return (service, "Write")
    elif op.startswith(("Tag", "Untag")):
        return (service, "Tagging")
    elif any(op.startswith(p) for p in [
        "PassRole", "Attach", "Detach", "CreatePolicy", "PutPolicy", "SetPolicy",
        "AddUserToGroup", "RemoveUserFromGroup"
    ]):
        return (service, "PermissionsManagement")
    return (service, "Read")


# ---------------------------------------------------------
# Core Processing
# ---------------------------------------------------------

def parse_policies(principal_records):
    action_details = []

    for rec in principal_records:
        principal = rec["Principal"]
        principal_type = rec["PrincipalType"]
        policies = rec.get("Policies", [])

        for pol in policies:
            pname = pol.get("PolicyName")
            ptype = pol.get("PolicyType")
            stmts = pol.get("PolicyDocument", {}).get("Statement", [])

            if isinstance(stmts, dict):
                stmts = [stmts]

            for stmt in stmts:
                effect = stmt.get("Effect", "Allow")
                actions = expand_actions(stmt.get("Action", []))
                resources = stmt.get("Resource", ["*"])
                if isinstance(resources, str):
                    resources = [resources]

                for act in actions:
                    svc, level = classify_action(act)
                    action_details.append({
                        "Principal": principal,
                        "PrincipalType": principal_type,
                        "PolicyName": pname,
                        "PolicyType": ptype,
                        "Effect": effect,
                        "Action": act,
                        "Service": svc,
                        "AccessLevel": level,
                        "Resources": resources,
                        "Condition": stmt.get("Condition", {})
                    })
    return action_details


def aggregate_to_matrix(action_details):
    matrix = {}
    for row in action_details:
        key = (row["Principal"], row["PrincipalType"], row["Service"])
        if key not in matrix:
            matrix[key] = {
                "Principal": row["Principal"],
                "PrincipalType": row["PrincipalType"],
                "Service": row["Service"],
                "AllowedLevels": set(),
                "DeniedLevels": set(),
                "Resources": [],
                "HasExplicitDeny": False,
                "Sources": set()
            }

        entry = matrix[key]
        if row["Effect"] == "Allow":
            entry["AllowedLevels"].add(row["AccessLevel"])
        elif row["Effect"] == "Deny":
            entry["DeniedLevels"].add(row["AccessLevel"])
            entry["HasExplicitDeny"] = True

        entry["Resources"].extend(row["Resources"])
        entry["Sources"].add(row["PolicyName"])

    results = []
    for _, entry in matrix.items():
        effective_levels = entry["AllowedLevels"] - entry["DeniedLevels"]
        unique_resources = set(entry["Resources"])
        if "*" in unique_resources and len(unique_resources) == 1:
            scope = "AllResources"
        elif "*" in unique_resources:
            scope = "Mixed"
        else:
            scope = "Scoped"

        results.append({
            "Principal": entry["Principal"],
            "PrincipalType": entry["PrincipalType"],
            "Service": entry["Service"],
            "AccessLevels": ",".join(sorted(effective_levels)) if effective_levels else "None",
            "ResourceScope": scope,
            "HasExplicitDeny": entry["HasExplicitDeny"],
            "Sources": ";".join(sorted(entry["Sources"]))
        })
    return results


# ---------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------

def lambda_handler(event, context):
    """
    Event format:
    {
      "UsersFile": {"Bucket": "my-bucket", "Key": "users.jsonl"},
      "GroupsFile": {"Bucket": "my-bucket", "Key": "groups.jsonl"},
      "RolesFile": {"Bucket": "my-bucket", "Key": "roles.jsonl"},
      "OutputPrefix": "reports/output/"
    }
    """
    users = load_jsonl_from_s3(event["UsersFile"]["Bucket"], event["UsersFile"]["Key"])
    groups = load_jsonl_from_s3(event["GroupsFile"]["Bucket"], event["GroupsFile"]["Key"])
    roles = load_jsonl_from_s3(event["RolesFile"]["Bucket"], event["RolesFile"]["Key"])

    details = []
    details.extend(parse_policies(users))
    details.extend(parse_policies(groups))
    details.extend(parse_policies(roles))

    if not details:
        return {"status": "No policies found"}

    bucket = event["UsersFile"]["Bucket"]
    output_prefix = event["OutputPrefix"]

    # Detailed actions CSV
    write_csv_to_s3(bucket, f"{output_prefix}principal_action_detail.csv", details, fieldnames=details[0].keys())

    # Service access matrix CSV
    matrix = aggregate_to_matrix(details)
    write_csv_to_s3(bucket, f"{output_prefix}service_access_matrix.csv", matrix, fieldnames=matrix[0].keys())

    return {
        "status": "Success",
        "detail_file": f"s3://{bucket}/{output_prefix}principal_action_detail.csv",
        "matrix_file": f"s3://{bucket}/{output_prefix}service_access_matrix.csv"
    }
