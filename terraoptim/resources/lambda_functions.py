#!/usr/bin/env python3

import json
import boto3

from ..common.utils import extract_region_from_terraform_plan, REGION_NAME_MAP

FREE_TIER_REQUESTS = 1000000
FREE_TIER_GB_SEC = 400000


def extract_lambda_functions(terraform_data):
    """
    Extract AWS Lambda functions from Terraform plan.

    Args:
        terraform_data (dict): Parsed Terraform plan data.

    Returns:
        list of dict: List of Lambda function info dictionaries, each containing
                      'name', 'memory', 'timeout', and 'architecture'.
    """
    functions = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_lambda_function":
            after = resource["change"]["after"]
            memory = after.get("memory_size", 128)
            timeout = after.get("timeout", 3)
            architecture = after.get("architectures", ["x86_64"])[0]
            functions.append({
                "name": after.get("name"),
                "memory": memory,
                "timeout": timeout,
                "architecture": architecture,
            })
    return functions


def get_lambda_price(region, architecture="x86_64"):
    """
    Fetch Lambda pricing per GB-second and per request from AWS Pricing API.

    Args:
        region (str): AWS region code.
        architecture (str): Processor architecture, default is 'x86_64'.

    Returns:
        tuple: (gb_sec_price (float), request_price (float)) - prices in USD.
    """
    try:
        client = boto3.client("pricing", region_name="us-east-1")

        location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")
        filters = [
            {"Type": "TERM_MATCH", "Field": "location", "Value": location},
            {"Type": "TERM_MATCH", "Field": "servicename", "Value": "AWS Lambda"},
        ]
        gb_sec_desc = "AWS Lambda - Total Compute (Provisioned) -"

        if architecture == "arm64":
            gb_sec_desc = "AWS Lambda - Total Compute (Provisioned) for ARM -"
        response = client.get_products(
            ServiceCode="AWSLambda",
            Filters=filters,
            MaxResults=100,
        )
        gb_sec_price = None
        request_price = None
        for product_json in response["PriceList"]:
            product = json.loads(product_json)
            terms = product["terms"]["OnDemand"]
            for term in terms.values():

                for dim in term["priceDimensions"].values():
                    desc = dim["description"].lower()
                    if gb_sec_desc.lower() in desc.lower():
                        gb_sec_price = float(dim["pricePerUnit"]["USD"])
                    elif "requests" in desc.lower():
                        request_price = float(dim["pricePerUnit"]["USD"])

        return gb_sec_price, request_price
    except Exception as e:
        print(f"️  Failed to fetch lambda price: {e}")
    return None, None


def estimate_lambda_cost(lambda_func, monthly_requests, avg_duration, region):
    """
    Estimate Lambda function monthly cost without applying free tier.

    Args:
        lambda_func (dict): Lambda function metadata.
        monthly_requests (int): Number of invocations per month.
        avg_duration (float): Average execution duration in seconds.
        region (str): AWS region code.

    Returns:
        dict: Cost breakdown including compute, request, and total costs.
    """

    mem_gb = lambda_func["memory"] / 1024
    total_gb_seconds = monthly_requests * mem_gb * avg_duration

    gb_sec_price, request_price = get_lambda_price(region, lambda_func["architecture"])

    request_cost = monthly_requests * request_price
    compute_cost = total_gb_seconds * gb_sec_price
    total = request_cost + compute_cost

    return {
        "name": lambda_func["name"],
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


def calculate_lambda_costs(functions, user_defaults, region):
    """
    Calculate per-function Lambda costs and aggregate total usage.

    Args:
        functions (list): List of Lambda function dicts.
        user_defaults (dict): Contains 'invocations' and optional 'duration'.
        region (str): AWS region code.

    Returns:
        tuple: (cost_results (list), total_gb_seconds (float), total_invocations (int))
    """
    total_gb_seconds = 0
    total_invocations = 0
    cost_results = []

    for func in functions:
        invocations = user_defaults["invocations"]
        duration = user_defaults["duration"] if user_defaults["duration"] is not None else func["timeout"]

        cost = estimate_lambda_cost(func, invocations, duration, region)
        total_gb_seconds += cost["raw_gb_sec"]
        total_invocations += cost["raw_requests"]
        cost_results.append(cost)

    return cost_results, total_gb_seconds, total_invocations


def suggest_graviton_alternative(lambda_func, monthly_requests, avg_duration, region):
    """
    Print a side-by-side comparison of current architecture vs Graviton (arm64) for cost savings.

    Args:
        lambda_func (dict): Current Lambda function metadata.
        monthly_requests (int): Monthly invocations.
        avg_duration (float): Average duration in seconds.
        region (str): AWS region code.
    """
    current_cost = lambda_func['total_cost']
    graviton_func = lambda_func.copy()
    graviton_func["architecture"] = "arm64"
    graviton_cost = estimate_lambda_cost(graviton_func, monthly_requests, avg_duration, region)

    print(f"\n    ➤ Potential Graviton Savings:")
    print(f"       Architecture       | Total Monthly Cost")
    print(f"       -------------------|-------------------")
    print(f"       x86_64             | ${current_cost}")
    print(f"       arm64              | ${graviton_cost['total_cost']}")

    if graviton_cost["total_cost"] < current_cost:
        savings = round(current_cost - graviton_cost["total_cost"], 4)
        print(f"        You could save ~${savings}/month by switching to arm64.\n")
    else:
        print("        No cost savings from switching to arm64 in this case.\n")


def print_lambda_function_costs(cost_results, region):
    """Print detailed cost breakdown per Lambda function."""
    for cost in cost_results:
        print(f"\n️ Function: {cost['name']} ({cost['architecture']})")
        print(f"    Memory: {cost['memory']} MB | Avg Duration: {cost['duration']} s")
        print(f"    Invocations: {cost['requests']} / month")
        print(f"    Compute Cost (before free tier): ${cost['compute_cost']}")
        print(f"    Request Cost (before free tier): ${cost['request_cost']}")
        print(f"    Total (before free tier): ${cost['total_cost']}")

        if cost["architecture"] == "x86_64":
            suggest_graviton_alternative(
                lambda_func=cost,
                monthly_requests=cost["requests"],
                avg_duration=cost["duration"],
                region=region
            )

def summarize_lambda_totals(total_gb_seconds, total_invocations, region):
    """
    Apply AWS Free Tier and print total monthly Lambda costs.

    Args:
        total_gb_seconds (float): Total GB-seconds used by all Lambdas.
        total_invocations (int): Total number of Lambda invocations.
        region (str): AWS region code.

    """
    print("\n AWS Free Tier Limits:")
    print(f"   {FREE_TIER_REQUESTS:,} requests / month")
    print(f"   {FREE_TIER_GB_SEC:,} GB-seconds / month")

    billable_requests = max(total_invocations - FREE_TIER_REQUESTS, 0)
    billable_gb_sec = max(total_gb_seconds - FREE_TIER_GB_SEC, 0)

    gb_sec_price, request_price = get_lambda_price(region)
    final_request_cost = round(billable_requests * request_price, 3)
    final_compute_cost = round(billable_gb_sec * gb_sec_price, 3)
    final_total_cost = round(final_request_cost + final_compute_cost, 3)

    print(f"\n Total Usage This Month:")
    print(f"   GB-seconds used: {round(total_gb_seconds):,} (Billable: {round(billable_gb_sec):,})")
    print(f"   Requests: {total_invocations:,} (Billable: {billable_requests:,})")

    print(f"\n Final Monthly Cost After Free Tier:")
    print(f"  Compute Cost: ${final_compute_cost}")
    print(f"  Request Cost: ${final_request_cost}")
    print(f"  Total Estimated Monthly Cost For All Lambdas: ${final_total_cost}")
    print("\n More info: https://aws.amazon.com/lambda/pricing/")
    print("====================================================")


def lambda_main(terraform_data, params=None):
    try:
        region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
        functions = extract_lambda_functions(terraform_data) if terraform_data else []

        if not functions:
            print(" No Lambda functions found in Terraform plan.")
            return
        print(f"\n Found {len(functions)} Lambda functions:")

        user_defaults = {
            "invocations": 1_000_000,
            "duration": None
        }

        allowed_keys = set(user_defaults.keys())
        if isinstance(params, dict):
            unknown_keys = set(params.keys()) - allowed_keys
            if unknown_keys:
                print(f"️ EC2 Optimization Warning: Unrecognized parameter(s): {', '.join(unknown_keys)}")
            user_defaults["invocations"] = params.get("invocations", user_defaults["invocations"])
            user_defaults["duration"] = params.get("duration", user_defaults["duration"])

        invocations = user_defaults["invocations"]
        print(f" Invocations: {invocations}")

        cost_results, total_gb_seconds, total_invocations = calculate_lambda_costs(functions, user_defaults, region)
        print_lambda_function_costs(cost_results, region)
        summarize_lambda_totals(total_gb_seconds, total_invocations, region)
    except Exception as e:
        print(f"️ Error calculating lambda optimization: {e}")