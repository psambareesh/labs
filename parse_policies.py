def parse_policies(principal_records):
    """
    Parse IAM policy records into normalized action details.
    Expects each record to have:
      PrincipalName, PrincipalType, PolicyType, PolicyName, PolicyArn, PolicyDocument
    """
    action_details = []

    for rec in principal_records:
        principal = rec.get("PrincipalName", "UNKNOWN")
        principal_type = rec.get("PrincipalType", "UNKNOWN")
        pname = rec.get("PolicyName")
        ptype = rec.get("PolicyType")
        parn  = rec.get("PolicyArn")
        policy_doc = rec.get("PolicyDocument", {})

        stmts = policy_doc.get("Statement", [])
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
                    "PolicyArn": parn,
                    "Effect": effect,
                    "Action": act,
                    "Service": svc,
                    "AccessLevel": level,
                    "Resources": resources,
                    "Condition": stmt.get("Condition", {})
                })

    return action_details
