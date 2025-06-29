#!/usr/bin/env python3

import boto3
import json

from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan, REGION_CODE_MAP

FREE_TIER_S3_GB = 5
FREE_TIER_PUT_REQUESTS = 10000
FREE_TIER_GET_REQUESTS = 100000


def get_s3_storage_price(region, storage_class="STANDARD"):
    """
    Retrieves the per-GB monthly price for a given S3 storage class in a specific region.
    Args:
        region (str): AWS region code
        storage_class (str): S3 storage class (e.g., 'STANDARD', 'GLACIER').
    Returns:
        float: Price per GB in USD, or None if not found.
    """
    try:
        pricing = boto3.client("pricing", region_name="us-east-1")
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
            print(f"️ Unsupported storage class: {storage_class}")
            return None


        response = pricing.get_products(
            ServiceCode="AmazonS3",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"}
            ],
        )

        for product in response["PriceList"]:
            data = json.loads(product)
            volume_type = data["product"]["attributes"].get("volumeType", "").lower()
            if keyword.lower() == volume_type:
                for term in data.get("terms", {}).get("OnDemand", {}).values():
                    for dim in term.get("priceDimensions", {}).values():
                        price = dim["pricePerUnit"]["USD"]
                        return float(price)

    except Exception as e:
        print(f"️ Failed to fetch price for S3")
        raise e
    return None


def get_s3_request_price(request_type, region):
    """
    Fetches the price per 1,000 PUT or GET requests in a specific region.
    Args:
        request_type (str): "PUT" or "GET".
        region (str): AWS region code.
    Returns:
        float: Price per 1,000 requests in USD, or None if not found.
    """
    try:
        pricing = boto3.client("pricing", region_name="us-east-1")
        location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

        label = {
            "PUT": "PUT, COPY, POST, or LIST requests",
            "GET": "GET and all other requests"
        }.get(request_type)
        usage_base = {
            "PUT": "Requests-Tier1",
            "GET": "Requests-Tier2"
        }.get(request_type)

        region_prefix = REGION_CODE_MAP.get(region, "")
        if region_prefix and region != "us-east-1":
            usage_type = f"{region_prefix}-{usage_base}"
        else:
            usage_type = usage_base


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
        print(f"️ Failed to fetch price for {request_type} request ")
        raise e
    return None


def extract_storage_class_from_lifecycle(terraform_data, bucket_name):
    """
    Extracts storage class transition settings from S3 lifecycle rules.
    Args:
        terraform_data (dict): Terraform plan in JSON format.
        bucket_name (str): Name of the S3 bucket.
    Returns:
        tuple: (storage_class, transition_days), or (None, None)
    """
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_s3_bucket":
            after = resource.get("change", {}).get("after", {})
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
    """
    Extracts the list of S3 bucket names from the Terraform plan.
    Args:
        terraform_data (dict): Terraform plan.
    Returns:
        list: List of bucket names.
    """
    buckets = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_s3_bucket":
            bucket_name = resource["change"]["after"].get("bucket")
            buckets.append(bucket_name)

    return buckets


def calculate_s3_bucket_costs(buckets, terraform_data, user_defaults, region):
    """
    Calculates estimated monthly cost for each S3 bucket based on usage and lifecycle rules.
    Args:
        buckets (list): List of S3 bucket names.
        terraform_data (dict): Terraform plan data.
        user_defaults (dict): Contains 'storage', 'put_requests', 'get_requests'.
        region (str): AWS region code.
    Returns:
        tuple: (results, total_storage_cost, total_put_cost, total_get_cost)
    """
    storage = user_defaults["storage"]
    put_requests = user_defaults["put_requests"]
    get_requests = user_defaults["get_requests"]

    storage_price = get_s3_storage_price(region, "STANDARD")
    put_price = get_s3_request_price("PUT", region)
    get_price = get_s3_request_price("GET", region)

    results = []
    total_storage_cost = total_put_cost = total_get_cost = 0

    for bucket in buckets:
        transition_class, transition_days = extract_storage_class_from_lifecycle(terraform_data, bucket)

        if transition_class and transition_days < 30:
            transition_price = get_s3_storage_price(region, transition_class)
            cost_standard = round(storage / 30 * transition_days * storage_price, 3)
            cost_transitioned = round(storage / 30 * (30 - transition_days) * transition_price, 3)
            storage_cost = round(cost_standard + cost_transitioned,3)
        else:
            storage_cost = round(storage * storage_price, 3)

        put_cost = round((put_requests / 1000) * put_price, 3)
        get_cost = round((get_requests / 1000) * get_price, 3)

        total_storage_cost += storage_cost
        total_put_cost += put_cost
        total_get_cost += get_cost

        results.append({
            "bucket": bucket,
            "storage_cost": storage_cost,
            "put_cost": put_cost,
            "get_cost": get_cost,
            "storage_class": transition_class or "STANDARD",
            "transition_days": transition_days if transition_class else None
        })

    return results, total_storage_cost, total_put_cost, total_get_cost

def print_s3_bucket_costs(results):
    """
    Prints detailed cost breakdown per bucket.
    Args:
        results (list): List of dictionaries with bucket cost breakdowns.
    """
    for r in results:
        print(f"\n  Bucket: {r['bucket']} | Storage Class: {r['storage_class']}")
        if r["transition_days"]:
            print(f"    Lifecycle: Transition to {r['storage_class']} after {r['transition_days']} days")
        print(f"    Storage Cost: ${r['storage_cost']}")
        print(f"    PUT Cost: ${r['put_cost']} | GET Cost: ${r['get_cost']}")

def summarize_s3_totals(no_buckets, user_defaults, total_storage_cost, total_put_cost, total_get_cost, region):
    """
    Prints overall usage and cost summary, including AWS Free Tier deductions.
    Args:
        no_buckets (int): number of buckets
        user_defaults (dict): User-specified usage numbers.
        total_storage_cost (float): Total storage cost.
        total_put_cost (float): Total PUT request cost.
        total_get_cost (float): Total GET request cost.
        region (str): AWS region code.
    """
    storage = user_defaults["storage"] * no_buckets
    put_requests = user_defaults["put_requests"] * no_buckets
    get_requests = user_defaults["get_requests"] * no_buckets

    storage_price = get_s3_storage_price(region, "STANDARD")
    put_price = get_s3_request_price("PUT", region)
    get_price = get_s3_request_price("GET", region)



    print("\n AWS Free Tier Limits:")
    print(f"   {FREE_TIER_S3_GB} GB of storage / month")
    print(f"   {FREE_TIER_PUT_REQUESTS} PUT requests / month")
    print(f"   {FREE_TIER_GET_REQUESTS} GET requests / month")

    billable_storage = max(storage - FREE_TIER_S3_GB, 0)
    billable_put = max(put_requests - FREE_TIER_PUT_REQUESTS, 0)
    billable_get = max(get_requests - FREE_TIER_GET_REQUESTS, 0)

    cost_storage = round(max(total_storage_cost - FREE_TIER_S3_GB * storage_price, 0), 3)
    cost_put = round(max(total_put_cost - FREE_TIER_PUT_REQUESTS / 1000 * put_price, 0), 3)
    cost_get = round(max(total_get_cost - FREE_TIER_GET_REQUESTS / 1000 * get_price, 0), 3)

    total = cost_storage + cost_put + cost_get

    print(f"\n Total Usage This Month:")
    print(f"   Storage: {storage} GB (Billable: {billable_storage})")
    print(f"   PUT Requests: {put_requests} (Billable: {billable_put})")
    print(f"   GET Requests: {get_requests} (Billable: {billable_get})")

    print(f"\n Final Monthly Cost After Free Tier:")
    print(f"   Storage: ${cost_storage}")
    print(f"   PUT Requests: ${cost_put}")
    print(f"   GET Requests: ${cost_get}")
    print(f" Total Estimated Monthly Cost For All Buckets: ${round(total, 3)}")

    print("\n More info: https://aws.amazon.com/s3/pricing/")
    print("====================================================")


def s3_main(terraform_data=None, params=None):
    try:
        region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
        buckets = extract_s3_buckets(terraform_data) if terraform_data else []

        if not buckets:
            print(" No S3 buckets found in Terraform plan.")
            return
        print(f"\n Found {len(buckets)} Buckets:")

        user_defaults = {
            "storage": 100,
            "put_requests": 1000,
            "get_requests": 10000
        }

        allowed_keys = set(user_defaults.keys())
        if isinstance(params, dict):
            unknown_keys = set(params.keys()) - allowed_keys
            if unknown_keys:
                print(f"️ Optimization Warning: Unrecognized parameter(s): {', '.join(unknown_keys)}")
            user_defaults["storage"] = params.get("storage", user_defaults["storage"])
            user_defaults["put_requests"] = params.get("put_requests", user_defaults["put_requests"])
            user_defaults["get_requests"] = params.get("get_requests", user_defaults["get_requests"])

        storage = user_defaults["storage"]
        put_requests = user_defaults["put_requests"]
        get_requests = user_defaults["get_requests"]

        print(f" Storage: {storage} GB")
        print(f" PUT Requests: {put_requests} |  GET Requests: {get_requests}")

        results, total_storage_cost, total_put_cost, total_get_cost = calculate_s3_bucket_costs(
            buckets, terraform_data, user_defaults, region
        )

        print_s3_bucket_costs(results)
        summarize_s3_totals(len(results), user_defaults, total_storage_cost, total_put_cost,
                            total_get_cost, region)
    except Exception as e:
        print(f"️ Error calculating s3 optimization")
