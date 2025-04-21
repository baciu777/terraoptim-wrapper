#!/usr/bin/env python3

import json
import boto3

from ..common.utils import extract_region_from_terraform_plan, REGION_NAME_MAP

FREE_TIER_REQUESTS = 1000000
FREE_TIER_GB_SEC = 400000  # 400,000 GB-s


def extract_lambda_functions(terraform_data):
    """Extract AWS Lambda functions from Terraform plan."""
    functions = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_lambda_function":
            after = resource["change"]["after"]
            memory = after.get("memory_size", 128)
            timeout = after.get("timeout", 3)
            architecture = after.get("architectures", ["x86_64"])[0]
            functions.append({
                "name": after.get("function_name"),
                "memory": memory,
                "timeout": timeout,
                "architecture": architecture,
            })
    return functions


def get_lambda_price(region, architecture="x86_64"):
    """Fetch Lambda pricing per GB-second and per request."""
    client = boto3.client("pricing", region_name="us-east-1")

    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")
    filters = [
        {"Type": "TERM_MATCH", "Field": "location", "Value": location},
        {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Lambda"},
        {"Type": "TERM_MATCH", "Field": "operation", "Value": "Invoke"},
    ]

    if architecture == "arm64":
        filters.append({"Type": "TERM_MATCH", "Field": "processorArchitecture", "Value": "AWS Graviton"})

    response = client.get_products(
        ServiceCode="AWSLambda",
        Filters=filters,
        MaxResults=100,
    )

    gb_sec_price = 0.0000166667  # fallback default
    request_price = 0.20 / 1000000  # $0.20 per 1M requests

    for product_json in response["PriceList"]:
        product = json.loads(product_json)
        terms = product["terms"]["OnDemand"]
        for term in terms.values():
            for dim in term["priceDimensions"].values():
                desc = dim["description"].lower()
                if "duration" in desc:
                    gb_sec_price = float(dim["pricePerUnit"]["USD"])
                elif "requests" in desc:
                    request_price = float(dim["pricePerUnit"]["USD"])

    return gb_sec_price, request_price

def estimate_lambda_cost(lambda_func, monthly_requests, avg_duration, region):
    """Estimate Lambda function monthly cost without applying free tier."""
    mem_gb = lambda_func["memory"] / 1024
    total_gb_seconds = monthly_requests * mem_gb * avg_duration

    gb_sec_price, request_price = get_lambda_price(region, lambda_func["architecture"])

    request_cost = monthly_requests * request_price
    compute_cost = total_gb_seconds * gb_sec_price
    total = request_cost + compute_cost

    return {
        "function_name": lambda_func["name"],
        "memory": lambda_func["memory"],
        "architecture": lambda_func["architecture"],
        "requests": monthly_requests,
        "duration": avg_duration,
        "compute_cost": round(compute_cost, 4),
        "request_cost": round(request_cost, 4),
        "total_cost": round(total, 4),
        "raw_gb_sec": total_gb_seconds,
        "raw_requests": monthly_requests
    }
def lambda_main(terraform_data, params=None):
    """Main entry for Lambda analysis (requires usage_data: invocations + duration)."""

    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    functions = extract_lambda_functions(terraform_data) if terraform_data else []

    if not functions:
        print("❌ No Lambda functions found in Terraform plan.")
        return

    # Default usage assumptions
    user_defaults = {
        "invocations": 1_000_000,
        "duration": None  # will default to function timeout per-function
    }

    if isinstance(params, dict):
        user_defaults["invocations"] = params.get("invocations", user_defaults["invocations"])
        user_defaults["duration"] = params.get("duration", user_defaults["duration"])

    print(f"\n🔍 Found {len(functions)} Lambda function(s):")
    total_gb_seconds = 0
    total_invocations = 0

    for func in functions:
        invocations = user_defaults["invocations"]
        duration = user_defaults["duration"] if user_defaults["duration"] is not None else func["timeout"]

        cost = estimate_lambda_cost(func, invocations, duration, region)

        total_gb_seconds += cost["raw_gb_sec"]
        total_invocations += cost["raw_requests"]

        print(f"\n⚙️ Function: {cost['function_name']} ({cost['architecture']})")
        print(f"💾 Memory: {cost['memory']} MB | Avg Duration: {cost['duration']} s")
        print(f"📈 Invocations: {cost['requests']} / month")
        print(f"💸 Compute Cost (before free tier): ${cost['compute_cost']}")
        print(f"💸 Request Cost (before free tier): ${cost['request_cost']}")
        print(f"📊 Total (before free tier): ${cost['total_cost']}")

        if func["architecture"] == "x86_64":
            print("💡 Tip: Consider switching to `arm64` (Graviton2) for ~20% lower cost.")

    print("\n🧾 AWS Free Tier Limits:")
    print(f" - {FREE_TIER_REQUESTS:,} requests / month")
    print(f" - {FREE_TIER_GB_SEC:,} GB-seconds / month")

    billable_requests = max(total_invocations - FREE_TIER_REQUESTS, 0)
    billable_gb_sec = max(total_gb_seconds - FREE_TIER_GB_SEC, 0)

    gb_sec_price, request_price = get_lambda_price(region)
    final_request_cost = round(billable_requests * request_price, 3)
    final_compute_cost = round(billable_gb_sec * gb_sec_price, 3)
    final_total_cost = round(final_request_cost + final_compute_cost, 3)

    print(f"\n📉 Total Usage This Month:")
    print(f" - GB-seconds used: {round(total_gb_seconds):,} (Billable: {round(billable_gb_sec):,})")
    print(f" - Invocations: {total_invocations:,} (Billable: {billable_requests:,})")

    print(f"\n💰 Final Monthly Cost After Free Tier:")
    print(f" - Compute Cost: ${final_compute_cost}")
    print(f" - Request Cost: ${final_request_cost}")
    print(f" - Total Estimated Monthly Cost: ${final_total_cost}")
