import boto3
import csv
import os

s3 = boto3.client("s3")

def load_matrix_from_s3(s3_uri):
    """Load a CSV from S3 into a dict keyed by (Principal, PrincipalType, Service)."""
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(lines)
    
    data = {}
    for row in reader:
        key_tuple = (row["Principal"], row["PrincipalType"], row["Service"])
        data[key_tuple] = row
    return data

def compare_matrices(day1, day2):
    """Compare two service access matrices and return drift records."""
    drift = []

    all_keys = set(day1.keys()) | set(day2.keys())
    for k in all_keys:
        row1 = day1.get(k)
        row2 = day2.get(k)

        if row1 and not row2:
            drift.append({
                "Principal": k[0],
                "PrincipalType": k[1],
                "Service": k[2],
                "ChangeType": "Removed",
                "Day1": row1.get("AccessLevels"),
                "Day2": "None"
            })
        elif not row1 and row2:
            drift.append({
                "Principal": k[0],
                "PrincipalType": k[1],
                "Service": k[2],
                "ChangeType": "Added",
                "Day1": "None",
                "Day2": row2.get("AccessLevels")
            })
        elif row1 and row2:
            # Compare important fields
            if (row1["AccessLevels"] != row2["AccessLevels"] or
                row1["ResourceScope"] != row2["ResourceScope"] or
                row1["HasExplicitDeny"] != row2["HasExplicitDeny"]):
                drift.append({
                    "Principal": k[0],
                    "PrincipalType": k[1],
                    "Service": k[2],
                    "ChangeType": "Modified",
                    "Day1": f"{row1['AccessLevels']} | {row1['ResourceScope']} | {row1['HasExplicitDeny']}",
                    "Day2": f"{row2['AccessLevels']} | {row2['ResourceScope']} | {row2['HasExplicitDeny']}"
                })
    return drift

def lambda_handler(event, context):
    """
    Expects event like:
    {
      "Day1Matrix": "s3://my-bucket/day1/service_access_matrix.csv",
      "Day2Matrix": "s3://my-bucket/day2/service_access_matrix.csv",
      "Output": "s3://my-bucket/drift/service_access_drift_report.csv"
    }
    """
    day1_matrix = load_matrix_from_s3(event["Day1Matrix"])
    day2_matrix = load_matrix_from_s3(event["Day2Matrix"])

    drift = compare_matrices(day1_matrix, day2_matrix)

    # Write drift to CSV
    output_bucket, output_key = event["Output"].replace("s3://", "").split("/", 1)
    if drift:
        fieldnames = ["Principal", "PrincipalType", "Service", "ChangeType", "Day1", "Day2"]
        output_csv = []
        output_csv.append(",".join(fieldnames))
        for row in drift:
            output_csv.append(",".join([row[f] for f in fieldnames]))
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body="\n".join(output_csv).encode("utf-8")
        )
    else:
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body="No drift detected\n".encode("utf-8")
        )

    return {"status": "success", "drift_records": len(drift)}
