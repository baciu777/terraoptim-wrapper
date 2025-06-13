#!/usr/bin/env python3

import json
import boto3
from datetime import datetime, timedelta
from ..common.utils import extract_region_from_terraform_plan, REGION_NAME_MAP


def fetch_ec2_instance_types(region):
    """
    Fetch all EC2 instance types, including vCPU, memory, and architecture, using AWS pagination.

    Args:
        region (str): AWS region code (e.g., "us-east-1").

    Returns:
        dict: Mapping of instance type to its vCPU, memory, and architecture info.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    instance_data = {}
    next_token = None

    while True:
        params = {"MaxResults": 100}
        if next_token:
            params["NextToken"] = next_token

        response = ec2_client.describe_instance_types(**params)

        for instance in response["InstanceTypes"]:
            instance_type = instance["InstanceType"]
            vcpu = instance["VCpuInfo"]["DefaultVCpus"]
            memory = instance["MemoryInfo"]["SizeInMiB"] // 1024
            category = instance.get("ProcessorInfo", {}).get("SupportedArchitectures", ["Unknown"])[0]
            instance_data[instance_type] = {
                "vCPU": vcpu,
                "memory": f"{memory} GB",
                "category": category
            }

        next_token = response.get("NextToken")
        if not next_token:
            break

    return instance_data




def extract_ec2_instances(terraform_data):
    """
    Extract EC2 instance types and spot status from Terraform plan JSON.

    Args:
        terraform_data (dict): Parsed JSON from Terraform plan output.

    Returns:
        list: A list of tuples in the format (instance_type, is_spot_instance).
    """
    instances = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_instance":
            instance_type = resource["change"]["after"].get("instance_type")
            spot = resource["change"]["after"].get("spot_instance")
            if instance_type:
                instances.append((instance_type, spot))
    return instances

def suggest_alternatives(instance_type, hours_per_month, region, instance_categories):
    """
    Suggest 2 cheaper and 2 more powerful EC2 alternatives within the same family.

    Args:
        instance_type (str): The original EC2 instance type.
        hours_per_month (int): Estimated usage hours per month.
        region (str): AWS region code.
        instance_categories (dict): Dictionary of instance metadata from AWS.
    """
    if instance_type not in instance_categories:
        print("️ No recommendations available for this instance type.")
        return


    family_prefix = instance_type.split('.')[0]
    candidates = []
    for name, info in instance_categories.items():
        if not name.startswith(family_prefix):
            continue

        price = get_ec2_on_demand_price(name, region)
        if price and price > 0:
            monthly = round(price * (hours_per_month), 3)
            candidates.append({
                "name": name,
                "vCPU": info["vCPU"],
                "memory": info["memory"],
                "hourly": round(price, 3),
                "monthly": monthly
            })

    # Sort by price, then CPU, then memory
    candidates.sort(key=lambda x: (x["hourly"], x["vCPU"], int(x["memory"].split()[0])))
    index = next((i for i, c in enumerate(candidates) if c["name"] == instance_type), None)
    # Select 2 cheaper and 2 stronger alternatives
    cheaper = candidates[max(0, index - 2):index]
    stronger = candidates[index + 1:index + 3]

    suggestions = cheaper + stronger
    if suggestions:
        print("\n   Suggested Alternatives:")
        print(f"   {'Instance':<15} {'vCPU':<5} {'Memory':<10} {'Hourly ($)':<12} {'Monthly ($)':<12}")
        print("   "+"-" * 57)
        for inst in suggestions:
            print(f"   {inst['name']:<15} {inst['vCPU']:<5} {inst['memory']:<10} {inst['hourly']:<12} {inst['monthly']:<12}")
    else:
        print("️ No valid pricing data found for suggested alternatives.")

def get_ec2_on_demand_price(instance_type, region):
    """
    Retrieve the hourly on-demand price for a specific EC2 instance type.

    Args:
        instance_type (str): EC2 instance type (e.g., "t3.micro").
        region (str): AWS region code.

    Returns:
        float or None: Hourly price in USD, or None if not found.
    """
    try:
        client = boto3.client("pricing", region_name="us-east-1")
        response = client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": REGION_NAME_MAP.get(region, "US East (N. Virginia)")},#####put region
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"}
            ]
        )

        for price_item in response["PriceList"]:
            price_data = json.loads(price_item)
            for term in price_data.get("terms", {}).get("OnDemand", {}).values():
                for price_dimension in term.get("priceDimensions", {}).values():
                    description = price_dimension.get("description", "").lower()

                    if "on demand" in description and "linux" in description \
                            and "sql" not in description and "reservation" not in description:
                        return float(price_dimension["pricePerUnit"]["USD"])

    except Exception as e:
        print(f"️  Failed to fetch on-demand price for {instance_type}: {e}")
    return None





def get_spot_price(instance_type, region):
    """
    Calculate the average spot price over the past 10 hours for a specific EC2 instance type.

    Args:
        instance_type (str): EC2 instance type.
        region (str): AWS region code.

    Returns:
        float or None: Average hourly spot price, or None if no data available.
    """
    try:
        ec2 = boto3.client("ec2", region_name=region)

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=10)

        response = ec2.describe_spot_price_history(
            InstanceTypes=[instance_type],
            ProductDescriptions=["Linux/UNIX"],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=100,
        )

        prices = [float(item["SpotPrice"]) for item in response["SpotPriceHistory"]]
        if prices:
            # Average over available records in the past 10 hours
            avg_hourly_price = sum(prices) / len(prices)
            return avg_hourly_price
    except Exception as e:
        print(f"️  Failed to fetch spot price for {instance_type}: {e}")
    return None


def calculate_ec2_costs(instances, hours_per_month, region, instance_categories):
    """
    Compute estimated monthly costs (on-demand and spot) for a list of EC2 instances and suggest alternatives.

    Args:
        instances (list): List of (instance_type, is_spot_instance) tuples.
        hours_per_month (int): Number of hours per month to calculate cost.
        region (str): AWS region code.
        instance_categories (dict): Dictionary of instance metadata.

    Returns:
        tuple: (total_on_demand_cost, total_spot_cost)
    """
    total_on_demand = 0.0
    total_spot = 0.0
    for instance, is_spot in instances:
        on_demand_price = get_ec2_on_demand_price(instance, region)
        spot_price = get_spot_price(instance, region)
        on_demand_price = round(on_demand_price, 3) if on_demand_price else "N/A"
        spot_price = round(spot_price, 3) if spot_price else "N/A"

        print(f"\n  Instance: {instance} ({instance_categories.get(instance, {}).get('category', 'Unknown')})")
        print(f"    RAM: {instance_categories.get(instance, {}).get('memory', 'N/A')}, CPU: {instance_categories.get(instance, {}).get('vCPU', 'N/A')}")
        print(f"    On-Demand Price: {on_demand_price} USD/hour")
        print(f"    Spot Price:      {spot_price} USD/hour")

        if is_spot:
            print("️  Using Spot Instances – consider switching to On-Demand if stability is needed.")

        cost_monthly_on_demand = round(on_demand_price * hours_per_month, 3) if on_demand_price else "N/A"
        cost_monthly_spot = round(spot_price * hours_per_month, 3) if spot_price else "N/A"

        print(f"    Monthly Estimation (On-Demand): ${cost_monthly_on_demand}")
        print(f"    Monthly Estimation (Spot):      ${cost_monthly_spot}")

        if isinstance(cost_monthly_on_demand, float):
            total_on_demand += cost_monthly_on_demand
        if isinstance(cost_monthly_spot, float):
            total_spot += cost_monthly_spot

        suggest_alternatives(instance, hours_per_month, region, instance_categories)
    return total_on_demand, total_spot



def summarize_ec2_totals(total_on_demand, total_spot):
    """
    Print a summary of total estimated monthly costs for all EC2 instances.

    Args:
        total_on_demand (float): Total cost using on-demand pricing.
        total_spot (float): Total cost using spot pricing.
    """
    print(f" Total Estimated Monthly Cost For All Instances (On-Demand): ${round(total_on_demand, 3)}")
    print(f" Total Estimated Monthly Cost For All Instances (Spot):      ${round(total_spot, 3)}")
    print("\n More info: https://aws.amazon.com/ec2/pricing/")
    print("====================================================")

def ec2_main(terraform_data, params=None):
    try:
        region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
        instances = extract_ec2_instances(terraform_data) if terraform_data else []

        if not instances:
            print(" No EC2 instances found in Terraform plan.")
            return
        print(f"\n Found {len(instances)} EC2 instances:")

        user_defaults = {
            "hours": 720
        }
        allowed_keys = set(user_defaults.keys())
        if isinstance(params, dict):
            unknown_keys = set(params.keys()) - allowed_keys
            if unknown_keys:
                print(f"️ EC2 Optimization Warning: Unrecognized parameter(s): {', '.join(unknown_keys)}")

            user_defaults["hours"] = params.get("hours", user_defaults["hours"])

        hours = user_defaults["hours"]
        print(f" Hours: {hours}")

        instance_categories = fetch_ec2_instance_types(region)

        total_on_demand, total_spot = calculate_ec2_costs(instances, hours, region, instance_categories)
        summarize_ec2_totals(total_on_demand, total_spot)
    except Exception as e:
        print(f"️ Error calculating ec2 optimization: {e}")
