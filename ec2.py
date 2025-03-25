#!/usr/bin/env python3

import json
import subprocess
import boto3
import argparse


def fetch_ec2_instance_types():
    """ Fetch EC2 instance types from AWS API """
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    response = ec2_client.describe_instance_types()

    instance_data = {}
    for instance in response["InstanceTypes"]:
        instance_type = instance["InstanceType"]
        vcpu = instance["VCpuInfo"]["DefaultVCpus"]
        memory = instance["MemoryInfo"]["SizeInMiB"] // 1024
        category = instance.get("ProcessorInfo", {}).get("SupportedArchitectures", ["Unknown"])[0]
        instance_data[instance_type] = {"vCPU": vcpu, "memory": f"{memory} GB", "category": category}

    return instance_data


INSTANCE_CATEGORIES = fetch_ec2_instance_types()


def get_terraform_plan():
    """ Run Terraform plan and return JSON output """
    subprocess.run(["terraform", "plan", "-out=tfplan"], check=True)
    output = subprocess.run(["terraform", "show", "-json", "tfplan"], capture_output=True, text=True)
    return json.loads(output.stdout)


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


def get_ec2_price(instance_type, region="us-east-1", pricing_model="OnDemand"):
    """ Fetch EC2 price using AWS Pricing API """
    client = boto3.client("pricing", region_name="us-east-1")
    response = client.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "location", "Value": "US East (N. Virginia)"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"}
        ]
    )

    for price_item in response["PriceList"]:
        price_data = json.loads(price_item)
        for term in price_data["terms"][pricing_model].values():
            for price_dimension in term["priceDimensions"].values():
                return float(price_dimension["pricePerUnit"]["USD"])
    return None


def calculate_costs(instances, hours_per_month):
    """ Calculate costs and suggest alternatives """
    for instance, is_spot in instances:
        on_demand_price = get_ec2_price(instance, pricing_model="OnDemand")
        reserved_price = get_ec2_price(instance, pricing_model="Reserved")
        spot_price = get_ec2_price(instance, pricing_model="Spot")

        print(f"\n🔹 Instance: {instance} ({INSTANCE_CATEGORIES.get(instance, {}).get('category', 'Unknown')})")
        print(f"💾 {INSTANCE_CATEGORIES.get(instance, {}).get('memory', 'N/A')} RAM, CPU: {INSTANCE_CATEGORIES.get(instance, {}).get('vCPU', 'N/A')}")
        print(f"💰 On-Demand Price: {on_demand_price} USD/hour")
        print(f"💰 Spot Price: {spot_price} USD/hour")
        print(f"💰 Reserved Price: {reserved_price} USD/hour")

        if is_spot:
            print("⚠️ Using Spot Instances – consider switching to On-Demand if stability is needed.")

        cost_monthly_on_demand = on_demand_price * hours_per_month
        cost_monthly_spot = spot_price * hours_per_month if spot_price else "N/A"
        print(f"📊 Estimated Monthly Cost: {cost_monthly_on_demand} USD (On-Demand)")
        print(f"📊 Estimated Monthly Cost: {cost_monthly_spot} USD (Spot)")

        print("🔗 More Details:")
        print(" - Reserved Instances: https://aws.amazon.com/ec2/pricing/reserved-instances/")
        print(" - Spot Instances: https://aws.amazon.com/ec2/spot/")


def main():
    parser = argparse.ArgumentParser(description="Terraform EC2 Cost Optimization Tool")
    parser.add_argument("-optimization", type=str, required=True, help="Optimization type (e.g., 'ec2')")
    parser.add_argument("plan", nargs="?", help="Run Terraform plan", default=None)

    args = parser.parse_args()

    if args.optimization == "ec2" and args.plan == "plan":
        hours_per_month = int(input("🔢 How many hours per month will you use EC2 instances? "))
        terraform_data = get_terraform_plan()
        instances = extract_ec2_instances(terraform_data)
        if instances:
            calculate_costs(instances, hours_per_month)
        else:
            print("❌ No EC2 instances found in Terraform plan.")
    else:
        print("❌ Invalid command. Use: terraoptim -optimization ec2 plan")


if __name__ == "__main__":
    main()
