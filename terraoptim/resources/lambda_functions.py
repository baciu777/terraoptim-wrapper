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
    client = boto3.client("pricing", region_name="us-east-1")  # Lambda pricing is global

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


def estimate_lambda_cost(lambda_func, monthly_requests, avg_duration_ms, region):
    """Estimate Lambda function monthly cost."""
    mem_gb = lambda_func["memory"] / 1024
    avg_duration_sec = avg_duration_ms / 1000
    total_gb_seconds = monthly_requests * mem_gb * avg_duration_sec

    gb_sec_price, request_price = get_lambda_price(region, lambda_func["architecture"])

    # Remove free tier
    billable_requests = max(monthly_requests - FREE_TIER_REQUESTS, 0)
    billable_gb_sec = max(total_gb_seconds - FREE_TIER_GB_SEC, 0)

    request_cost = round(billable_requests * request_price, 4)
    compute_cost = round(billable_gb_sec * gb_sec_price, 4)
    total = round(request_cost + compute_cost, 4)

    return {
        "function_name": lambda_func["name"],
        "memory": lambda_func["memory"],
        "architecture": lambda_func["architecture"],
        "requests": monthly_requests,
        "duration_ms": avg_duration_ms,
        "compute_cost": compute_cost,
        "request_cost": request_cost,
        "total_cost": total
    }


def lambda_main(terraform_data, usage_data=None):
    """Main entry for Lambda analysis (requires usage_data: invocations + duration)."""
    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    functions = extract_lambda_functions(terraform_data)

    if not functions:
        print("❌ No Lambda functions found in Terraform plan.")
        return

    print(f"\n🔍 Found {len(functions)} Lambda function(s):")

    for func in functions:
        usage = usage_data.get(func["name"], {}) if usage_data else {}
        monthly_invocations = usage.get("monthly_requests", 1000000)
        avg_duration = usage.get("avg_duration_ms", func["timeout"] * 1000)

        cost = estimate_lambda_cost(func, monthly_invocations, avg_duration, region)

        print(f"\n⚙️ Function: {cost['function_name']} ({cost['architecture']})")
        print(f"💾 Memory: {cost['memory']} MB | Avg Duration: {cost['duration_ms']} ms")
        print(f"📈 Invocations: {cost['requests']} / month")
        print(f"💸 Compute Cost: ${cost['compute_cost']}")
        print(f"💸 Request Cost: ${cost['request_cost']}")
        print(f"📊 Total Estimated Monthly Cost: ${cost['total_cost']}")
        if func["architecture"] == "x86_64":
            print("💡 Tip: Consider switching to `arm64` (Graviton2) for ~20% lower cost.")

