#!/usr/bin/env python3

import json
import boto3

from ..common.utils import extract_region_from_terraform_plan, REGION_NAME_MAP


def fetch_ec2_instance_types(region):
    """ Fetch all EC2 instance types from AWS API using pagination """
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

    print(f"✅ Total instance types fetched: {len(instance_data)}")
    return instance_data




def extract_ec2_instances(terraform_data):
    """ Extract EC2 instance types from Terraform plan """
    instances = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_instance":
            instance_type = resource["change"]["after"].get("instance_type")
            spot = resource["change"]["after"].get("spot_instance")
            if instance_type:
                instances.append((instance_type, spot))
    return instances

def suggest_alternatives(instance_type, hours_per_month, region):
    """ Suggest 2 cheaper and 2 more powerful EC2 instances from the same family """
    if instance_type not in INSTANCE_CATEGORIES:
        print("ℹ️ No recommendations available for this instance type.")
        return


    family_prefix = instance_type.split('.')[0]
    candidates = []
    for name, info in INSTANCE_CATEGORIES.items():
        if not name.startswith(family_prefix):
            continue

        print(name)
        price = get_ec2_on_demand_price(name, region)
        print(price)
        if price and price > 0:
            monthly = round(price * (hours_per_month or 720), 3)
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
        print("\n🧠 Suggested Alternatives:")
        print(f"{'Instance':<15} {'vCPU':<5} {'Memory':<10} {'Hourly ($)':<12} {'Monthly ($)':<12}")
        print("-" * 60)
        for inst in suggestions:
            print(f"{inst['name']:<15} {inst['vCPU']:<5} {inst['memory']:<10} {inst['hourly']:<12} {inst['monthly']:<12}")
    else:
        print("⚠️ No valid pricing data found for suggested alternatives.")

def get_ec2_on_demand_price(instance_type, region):
    """ Fetch EC2 price using AWS Pricing API """
    client = boto3.client("pricing", region_name=region)
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
    return None


from datetime import datetime, timedelta


def get_spot_price(instance_type, region):
    ec2 = boto3.client("ec2", region_name=region)

    # Define the time range: last 10 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=10)

    # Fetch spot price history
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
    else:
        return None


def calculate_costs(instances, hours_per_month, region):
    """ Calculate costs and suggest alternatives """
    for instance, is_spot in instances:
        on_demand_price = get_ec2_on_demand_price(instance, region)
        spot_price = get_spot_price(instance, region)
        on_demand_price = round(on_demand_price, 3) if on_demand_price else "N/A"
        spot_price = round(spot_price, 3) if spot_price else "N/A"

        print(f"\n🔹 Instance: {instance} ({INSTANCE_CATEGORIES.get(instance, {}).get('category', 'Unknown')})")
        print(f"💾 {INSTANCE_CATEGORIES.get(instance, {}).get('memory', 'N/A')} RAM, CPU: {INSTANCE_CATEGORIES.get(instance, {}).get('vCPU', 'N/A')}")
        print(f"💰 On-Demand Price: {on_demand_price} USD/hour")
        print(f"💰 Spot Price: {spot_price} USD/hour")

        if is_spot:
            print("⚠️ Using Spot Instances – consider switching to On-Demand if stability is needed.")

        if hours_per_month is not None:
            cost_monthly_on_demand = round(on_demand_price * hours_per_month, 3) if on_demand_price else "N/A"
            cost_monthly_spot = round(spot_price * hours_per_month, 3) if spot_price else "N/A"
            print(f"📊 Estimated Monthly Cost: {cost_monthly_on_demand} USD (On-Demand)")
            print(f"📊 Estimated Monthly Cost: {cost_monthly_spot} USD (Spot)")

            print("🔗 More Details:")
            print(" - Reserved Instances: https://aws.amazon.com/ec2/pricing/reserved-instances/")
            print(" - Spot Instances: https://aws.amazon.com/ec2/spot/")

        # Additional: show hourly cost and monthly cost if no hours_per_month is provided
        else:
            default_hours_per_month = 720  # Assume 720 hours (30 days * 24 hours)
            cost_monthly_default = on_demand_price * default_hours_per_month
            print(f"📊 Estimated Monthly Cost (30 Days Non-Stop): {cost_monthly_default} USD (On-Demand)")
            print(f"📊 Estimated Monthly Cost (30 Days Non-Stop): {spot_price * default_hours_per_month} USD (Spot)")

        suggest_alternatives(instance, hours_per_month or 720, region)

def ec2_main(terraform_data, hours_per_month=None):
    """ Main function to run EC2 cost optimization logic """
    instances = extract_ec2_instances(terraform_data)
    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    if instances:
        global INSTANCE_CATEGORIES
        INSTANCE_CATEGORIES = fetch_ec2_instance_types(region)
        calculate_costs(instances, hours_per_month, region)
    else:
        print("❌ No EC2 instances found in Terraform plan.")
