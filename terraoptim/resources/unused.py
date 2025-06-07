import datetime
import boto3

DEFAULT_DAYS = 30

def get_time_range(days):
    now = datetime.datetime.utcnow()
    if days <= 0:
        start_time = now - datetime.timedelta(hours=1)
    else:
        start_time = now - datetime.timedelta(days=days)

    if start_time >= now:
        raise ValueError("StartTime must be earlier than EndTime")

    return start_time, now

def get_age_days(creation_time):
    return (datetime.datetime.utcnow() - creation_time).days

def print_usage_message(name, age_days, days, used, usage_summary=None):
    if used:
        print(f"   {name} - {usage_summary}.")
    else:
        if age_days < 0:
            age_days = 0
        if age_days < days:
            print(f"   {name} - Created {age_days} days ago.")
        else:
            print(f" ❌ {name} - No activity in {days} days.")

def check_unused_lambdas(days):
    lambda_client = boto3.client("lambda")
    cloudwatch_client = boto3.client("cloudwatch")

    try:
        start_time, now = get_time_range(days)
    except ValueError as e:
        print(f"❌ Lambda Check Error: {e}")
        return

    print("\n Lambda Functions:")
    try:
        response = lambda_client.list_functions()
        functions = response.get("Functions", [])
        if not functions:
            print("  ️ No Lambda functions found.")
        for fn in functions:
            name = fn["FunctionName"]
            creation_str = fn.get("LastModified")
            creation_time = datetime.datetime.strptime(creation_str.split(".")[0], "%Y-%m-%dT%H:%M:%S")
            age_days = get_age_days(creation_time)

            try:
                metrics = cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Invocations',
                    Dimensions=[{'Name': 'FunctionName', 'Value': name}],
                    StartTime=start_time,
                    EndTime=now,
                    Period=86400,
                    Statistics=['Sum'],
                )
                datapoints = metrics.get("Datapoints", [])
                total = sum(dp['Sum'] for dp in datapoints)
                used = total > 0
                print_usage_message(name, age_days, days, used, f"{int(total)} invocations")
            except Exception as e:
                print(f"  ️ {name}: {e}")
    except Exception as e:
        print(f"❌ Lambda list error: {e}")

def check_unused_s3(days):
    s3 = boto3.resource("s3")
    cloudwatch_client = boto3.client("cloudwatch")

    try:
        start_time, now = get_time_range(days)
    except ValueError as e:
        print(f"❌ S3 Check Error: {e}")
        return

    print("\n S3 Buckets:")
    try:
        buckets = list(s3.buckets.all())
        if not buckets:
            print("  ️ No S3 buckets found.")
        for bucket in buckets:
            name = bucket.name
            creation_time = bucket.creation_date.replace(tzinfo=None)
            age_days = get_age_days(creation_time)

            try:
                metrics = cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': name},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                    ],
                    StartTime=start_time,
                    EndTime=now,
                    Period=86400,
                    Statistics=['Average'],
                )
                used = bool(metrics.get("Datapoints", []))
                print_usage_message(name, age_days, days, used, "Activity detected")
            except Exception as e:
                print(f"  ️ {name}: {e}")
    except Exception as e:
        print(f"❌ S3 list error: {e}")

def check_unused_dynamodb(days):
    dynamodb = boto3.client("dynamodb")
    cloudwatch = boto3.client("cloudwatch")

    try:
        start_time, now = get_time_range(days)
    except ValueError as e:
        print(f"❌ DynamoDB Check Error: {e}")
        return

    print("\n DynamoDB Tables:")
    try:
        tables = dynamodb.list_tables().get('TableNames', [])
        if not tables:
            print("  ️ No DynamoDB tables found.")
        for table in tables:
            try:
                desc = dynamodb.describe_table(TableName=table)
                creation_time = desc['Table']['CreationDateTime'].replace(tzinfo=None)
                age_days = get_age_days(creation_time)

                read = cloudwatch.get_metric_statistics(
                    Namespace='AWS/DynamoDB',
                    MetricName='ConsumedReadCapacityUnits',
                    Dimensions=[{'Name': 'TableName', 'Value': table}],
                    StartTime=start_time,
                    EndTime=now,
                    Period=86400,
                    Statistics=['Sum'],
                )
                write = cloudwatch.get_metric_statistics(
                    Namespace='AWS/DynamoDB',
                    MetricName='ConsumedWriteCapacityUnits',
                    Dimensions=[{'Name': 'TableName', 'Value': table}],
                    StartTime=start_time,
                    EndTime=now,
                    Period=86400,
                    Statistics=['Sum'],
                )
                read_sum = sum(dp['Sum'] for dp in read.get("Datapoints", []))
                write_sum = sum(dp['Sum'] for dp in write.get("Datapoints", []))
                used = (read_sum + write_sum) > 0
                print_usage_message(table, age_days, days, used, f"{int(read_sum)} reads / {int(write_sum)} writes")
            except Exception as e:
                print(f"  ️ {table}: {e}")
    except Exception as e:
        print(f"❌ DynamoDB list error: {e}")

def check_unused_glue(days):
    glue = boto3.client("glue")
    now = datetime.datetime.utcnow()
    cutoff = now - datetime.timedelta(days=days)

    print("\n AWS Glue Jobs:")
    try:
        jobs = glue.get_jobs().get('Jobs', [])
        if not jobs:
            print("  ️ No Glue jobs found.")
        for job in jobs:
            name = job["Name"]
            try:
                history = glue.get_job_runs(JobName=name, MaxResults=5).get('JobRuns', [])
                if not history:
                    age_days = 0
                    used = False
                else:
                    latest = max(run["StartedOn"] for run in history).replace(tzinfo=None)
                    creation_time = min(run["StartedOn"] for run in history).replace(tzinfo=None)
                    age_days = get_age_days(creation_time)
                    used = latest >= cutoff
                print_usage_message(name, age_days, days, used, "Recently run" if used else None)
            except Exception as e:
                print(f"  ️ {name}: {e}")
    except Exception as e:
        print(f"❌ Glue job list error: {e}")

def check_unused_ec2(days):
    ec2_client = boto3.client("ec2")
    cloudwatch_client = boto3.client("cloudwatch")

    try:
        start_time, now = get_time_range(days)
    except ValueError as e:
        print(f"❌ EC2 Check Error: {e}")
        return

    print("\n EC2 Instances:")
    try:
        reservations = ec2_client.describe_instances().get("Reservations", [])
        instances = [i for r in reservations for i in r.get("Instances", [])]
        if not instances:
            print("  ️ No EC2 instances found.")
        for instance in instances:
            instance_id = instance["InstanceId"]
            launch_time = instance["LaunchTime"].replace(tzinfo=None)
            age_days = get_age_days(launch_time)

            try:
                metrics = cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName='CPUUtilization',
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    StartTime=start_time,
                    EndTime=now,
                    Period=86400,
                    Statistics=['Average'],
                )
                datapoints = metrics.get("Datapoints", [])
                used = bool(datapoints and sum(dp['Average'] for dp in datapoints) > 0)
                avg_cpu = sum(dp['Average'] for dp in datapoints) / len(datapoints) if datapoints else 0
                print_usage_message(instance_id, age_days, days, used, f"CPU usage ({avg_cpu:.2f}%)")
            except Exception as e:
                print(f"  ️ {instance_id}: {e}")
    except Exception as e:
        print(f"❌ EC2 list error: {e}")

def unused_main(params=None):
    days = params.get("days") if params else DEFAULT_DAYS
    print(f" Scanning for resources unused in the past {days} days...")
    check_unused_lambdas(days)
    check_unused_s3(days)
    check_unused_dynamodb(days)
    check_unused_glue(days)
    check_unused_ec2(days)
    print("====================================================")
