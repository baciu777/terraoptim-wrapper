#!/usr/bin/env python3

import boto3
import json

from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan

FREE_TIER_S3_GB = 5  # 5 GB for free
FREE_TIER_PUT_REQUESTS = 10000  # 10,000 PUT requests for free
FREE_TIER_GET_REQUESTS = 100000  # 100,000 GET requests for free


def get_s3_price(region, storage_class="STANDARD"):
    pricing = boto3.client("pricing", region_name=region)
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

    STORAGE_CLASS_KEYWORDS = {
        "STANDARD": "Standard",
        "STANDARD_IA": "Standard - Infrequent Access",
        "ONEZONE_IA": "One Zone - Infrequent Access",
        "GLACIER": "Glacier Storage",
        "DEEP_ARCHIVE": "Glacier Deep Archive",
        "INTELLIGENT_TIERING": "Intelligent-Tiering",
        "GLACIER_IR": "Glacier Instant Retrieval"
    }

    keyword = STORAGE_CLASS_KEYWORDS.get(storage_class.upper())
    if not keyword:
        print(f"⚠️ Unsupported storage class: {storage_class}")
        return None

    try:
        response = pricing.get_products(
            ServiceCode="AmazonS3",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"}
            ],
        )
        for product in response["PriceList"]:
            data = json.loads(product)
            volumeType = data["product"]["attributes"].get("volumeType", "").lower()
            if keyword.lower() == volumeType:
                for term in data.get("terms", {}).get("OnDemand", {}).values():
                    for dim in term.get("priceDimensions", {}).values():
                        price = dim["pricePerUnit"]["USD"]
                        return float(price)

    except Exception as e:
        print(f"⚠️ Error fetching S3 price: {e}")
    return None


def get_s3_request_price(request_type, region):
    pricing = boto3.client("pricing", region_name=region)
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

    label = {
        "PUT": "PUT, COPY, POST, or LIST requests",
        "GET": "GET and all other requests"
    }.get(request_type)
    if request_type == "PUT":
        usage_type = "Requests-Tier1"
    else:
        usage_type = "Requests-Tier2"

    try:
        response = pricing.get_products(
            ServiceCode="AmazonS3",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage_type}
            ]
        )
        for product in response["PriceList"]:
            data = json.loads(product)
            terms = data.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                for dim in term.get("priceDimensions", {}).values():
                    description = dim.get("description", "")
                    if label in description:
                        price = dim["pricePerUnit"]["USD"]
                        return float(price)
    except Exception as e:
        print(f"⚠️ Error fetching {request_type} request pricing: {e}")
    return None


def extract_storage_class_from_lifecycle(terraform_data, bucket_name):
    for resource in terraform_data.get("resource_changes", []):
        # We only want to check the aws_s3_bucket resources
        if resource["type"] == "aws_s3_bucket":
            after = resource.get("change", {}).get("after", {})
            # Compare the actual bucket name (from the bucket attribute) with the provided bucket_name
            if after.get("bucket") == bucket_name:
                lifecycle_rules = after.get("lifecycle_rule", [])
                if lifecycle_rules:
                    rule = lifecycle_rules[0]
                    transitions = rule.get("transition", [])
                    if transitions:
                        transition = transitions[0]
                        storage_class = transition.get("storage_class")
                        days = transition.get("days")
                        return storage_class, days
    return None, None


def extract_s3_buckets(terraform_data):
    """Extract S3 bucket info from Terraform plan"""
    buckets = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_s3_bucket":
            bucket_name = resource["change"]["after"].get("bucket")
            buckets.append(bucket_name)

    return buckets


def s3_main(terraform_data=None, params=None):
    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    buckets = extract_s3_buckets(terraform_data) if terraform_data else []
    storage_gb = 100
    put_requests = 1000
    get_requests = 10000
    total_storage_cost = 0  # Total storage cost accumulator
    total_put_cost = 0  # Total PUT request cost accumulator
    total_get_cost = 0  # Total GET request cost accumulator
    total = 0  # Total cost accumulator

    if isinstance(params, dict):
        storage_gb = params.get("storage_gb", storage_gb)
        put_requests = params.get("put_requests", put_requests)
        get_requests = params.get("get_requests", get_requests)

    if buckets:
        print(f"📦 Storage: {storage_gb} GB")
        print(f"📥 PUT Requests: {put_requests} | 📤 GET Requests: {get_requests}")

        bucket_storage_class = "STANDARD"

        num_buckets = len(buckets)
        storage_price = get_s3_price(region, bucket_storage_class)
        put_price = get_s3_request_price("PUT", region)
        get_price = get_s3_request_price("GET", region)

        for bucket in buckets:
            print(f"📦 Bucket: {bucket} | Storage Class: {bucket_storage_class}")
            bucket_storage_class_transition, transition_days = extract_storage_class_from_lifecycle(terraform_data,
                                                                                                    bucket)

            if bucket_storage_class_transition and transition_days < 30:
                storage_price_transition = get_s3_price(region, bucket_storage_class_transition)
                print(f"Lifecycle found: Transition to {bucket_storage_class_transition} after {transition_days} days")
                storage_cost = round(storage_gb / 30 * transition_days * storage_price, 3) + \
                               round(storage_gb / 30 * (30 - transition_days) * storage_price_transition, 3)
                total_storage_cost += storage_cost
            else:
                storage_cost = round(storage_gb * storage_price, 3)
                total_storage_cost += storage_cost
            put_cost = round((put_requests / 1000) * put_price, 3)
            get_cost = round((get_requests / 1000) * get_price, 3)

            print(f"Bucket Cost Breakdown: Storage: ${storage_cost}, PUT: ${put_cost}, GET: ${get_cost}")

            total_put_cost += put_cost
            total_get_cost += get_cost

        # Print total costs before applying free tier
        print("\n📊 Total (before free tier):")
        print(f" - Storage Cost: ${total_storage_cost}")
        print(f" - PUT Requests Cost: ${total_put_cost}")
        print(f" - GET Requests Cost: ${total_get_cost}")

        # Free Tier Limits
        print("\n🧾 AWS Free Tier Limits:")
        print(f" - {FREE_TIER_S3_GB} GB of storage / month")
        print(f" - {FREE_TIER_PUT_REQUESTS} PUT requests / month")
        print(f" - {FREE_TIER_GET_REQUESTS} GET requests / month")

        # After Free Tier - Billable Costs
        billable_storage = max(storage_gb - FREE_TIER_S3_GB, 0)
        billable_put_requests = max(put_requests - FREE_TIER_PUT_REQUESTS, 0)
        billable_get_requests = max(get_requests - FREE_TIER_GET_REQUESTS, 0)

        cost_storage = round(max(total_storage_cost - FREE_TIER_S3_GB * storage_price, 0), 3)
        cost_put = round(max(total_put_cost - FREE_TIER_PUT_REQUESTS * put_price, 0), 3)
        cost_get = round(max(total_get_cost - FREE_TIER_GET_REQUESTS * get_requests, 0), 3)

        total = cost_storage + cost_put + cost_get

        print(f"\n📉 Total Usage This Month (Billable):")
        print(f" - Storage: {billable_storage} GB (Billable) | Cost: ${cost_storage}")
        print(f" - PUT Requests: {billable_put_requests} (Billable) | Cost: ${cost_put}")
        print(f" - GET Requests: {billable_get_requests} (Billable) | Cost: ${cost_get}")

        print(f"🪣 Number of Buckets: {num_buckets}")
        print(f"💰 Monthly Estimation For All Buckets: ${round(total, 3)}")

        print("\n🔗 More info: https://aws.amazon.com/s3/pricing/")
