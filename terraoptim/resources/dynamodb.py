#!/usr/bin/env python3

import boto3
import json

from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan

# AWS Free Tier for DynamoDB
FREE_TIER_READ_CAPACITY = 25  # units
FREE_TIER_WRITE_CAPACITY = 25  # units
FREE_TIER_STORAGE_GB = 25  # GB
HOURS_PER_MONTH = 730

def get_dynamodb_price_provisioned(region, usage_type):
    pricing = boto3.client("pricing", region_name=region)
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

    try:
        response = pricing.get_products(
            ServiceCode="AmazonDynamoDB",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage_type}
            ]
        )
        for product in response["PriceList"]:
            data = json.loads(product)
            terms = data.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                price_dimensions = term.get("priceDimensions", {})

                for dim in price_dimensions.values():
                    description = dim.get("description", "")
                    price_per_unit = dim.get("pricePerUnit", {}).get("USD")
                    if description and 'beyond the free tier' in description and price_per_unit:
                        return float(price_per_unit)

    except Exception as e:
        print(f"⚠️ Error fetching price for {usage_type}: {e}")
    return None


def get_dynamodb_price_on_demand(region, usage_type):
    pricing = boto3.client("pricing", region_name=region)  # must be us-east-1 for Pricing API
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

    try:
        response = pricing.get_products(
            ServiceCode="AmazonDynamoDB",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage_type}

            ]
        )
        print(response)
        for product_json in response["PriceList"]:
            product = json.loads(product_json)
            terms = product.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                price_dimensions = term.get("priceDimensions", {})
                for dim in price_dimensions.values():
                    desc = dim.get("description", "").lower()
                    price = dim.get("pricePerUnit", {}).get("USD")
                    return float(price)

    except Exception as e:
        print(f"⚠️ Error: {e}")

    return None

def extract_dynamodb_tables(terraform_data):
    tables = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_dynamodb_table":
            after = resource.get("change", {}).get("after", {})
            table_name = after.get("name")
            billing_mode = after.get("billing_mode", "PROVISIONED")
            read_capacity = after.get("read_capacity", 0)
            write_capacity = after.get("write_capacity", 0)
            storage_gb = after.get("estimated_storage_gb", 0)  # You may want to estimate this differently
            tables.append({
                "name": table_name,
                "billing_mode": billing_mode,
                "read_capacity": read_capacity,
                "write_capacity": write_capacity,
                "storage_gb": storage_gb
            })
    return tables

def calculate_table_cost(table, prices, on_demand_defaults):
    billing = table.get("billing_mode", "PROVISIONED").upper()
    storage = table.get("storage_gb", 5)
    storage_cost = round(storage * HOURS_PER_MONTH * prices["storage"], 3)

    if billing == "PROVISIONED":
        read = table.get("read_capacity")
        write = table.get("write_capacity")
        cost_read = round(read * HOURS_PER_MONTH * prices["read_prov"], 3)
        cost_write = round(write * HOURS_PER_MONTH * prices["write_prov"], 3)
        return {
            "mode": billing,
            "read": read,
            "write": write,
            "storage": storage,
            "cost_read": cost_read,
            "cost_write": cost_write,
            "cost_storage": storage_cost
        }

    elif billing == "PAY_PER_REQUEST":
        read = on_demand_defaults["reads"]
        write = on_demand_defaults["writes"]
        cost_read = round(read * prices["read_ondemand"], 3)
        cost_write = round(write * prices["write_ondemand"], 3)
        return {
            "mode": billing,
            "read": read,
            "write": write,
            "storage": storage,
            "cost_read": cost_read,
            "cost_write": cost_write,
            "cost_storage": storage_cost
        }

    return None  # Unknown billing mode



def apply_free_tier(total_read, total_write, total_storage, prices):
    billable_read = max(total_read - FREE_TIER_READ_CAPACITY, 0)
    billable_write = max(total_write - FREE_TIER_WRITE_CAPACITY, 0)
    billable_storage = max(total_storage - FREE_TIER_STORAGE_GB, 0)

    discount = (
        round((total_read - billable_read) * HOURS_PER_MONTH * prices["read_prov"], 3) +
        round((total_write - billable_write) * HOURS_PER_MONTH * prices["write_prov"], 3) +
        round((total_storage - billable_storage) * HOURS_PER_MONTH * prices["storage"], 3)
    )

    return {
        "billable_read": billable_read,
        "billable_write": billable_write,
        "billable_storage": billable_storage,
        "discount": discount
    }



def dynamodb_main(terraform_data=None, params=None):
    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    tables = extract_dynamodb_tables(terraform_data) if terraform_data else []

    on_demand_defaults = {
        "reads": 1_000_000,
        "writes": 500_000
    }
    if isinstance(params, dict):
        on_demand_defaults["reads"] = params.get("on_demand_reads", on_demand_defaults["reads"])
        on_demand_defaults["writes"] = params.get("on_demand_writes", on_demand_defaults["writes"])

    if not tables:
        print("ℹ️ No DynamoDB tables found in Terraform data.")
        return

    prices = {
        "read_prov": get_dynamodb_price_provisioned(region, "ReadCapacityUnit-Hrs"),
        "write_prov": get_dynamodb_price_provisioned(region, "WriteCapacityUnit-Hrs"),
        "storage": get_dynamodb_price_provisioned(region, "TimedStorage-ByteHrs"),
        "read_ondemand": get_dynamodb_price_on_demand(region, "ReadRequestUnits"),
        "write_ondemand": get_dynamodb_price_on_demand(region, "WriteRequestUnits"),
    }

    if not all(prices.values()):
        print("❌ Unable to retrieve all required prices.")
        return

    total_cost = 0
    total_prov_read = 0
    total_prov_write = 0
    total_storage = 0

    print(f"🗄️ Tables Found: {len(tables)}\n")

    for table in tables:
        name = table["name"]
        result = calculate_table_cost(table, prices, on_demand_defaults)

        if result is None:
            print(f"🔹 Table: {name} | ⚠️ Unknown billing mode. Skipping...\n")
            continue

        print(f"🔹 Table: {name} | Mode: {result['mode']}")
        if result['mode'] == "PROVISIONED":
            total_prov_read += result["read"]
            total_prov_write += result["write"]
        print(f"   📖 Reads: {result['read']} | ✍️ Writes: {result['write']} | 💾 Storage: {result['storage']} GB")

        subtotal = result["cost_read"] + result["cost_write"] + result["cost_storage"]
        total_cost += subtotal
        total_storage += result["storage"]

        print(f"   💵 Cost: Read ${result['cost_read']}, Write ${result['cost_write']}, "
              f"Storage ${result['cost_storage']}, Total ${subtotal}\n")

    # Apply free tier to provisioned usage
    tier = apply_free_tier(total_prov_read, total_prov_write, total_storage, prices)
    adjusted_cost = round(total_cost - tier["discount"], 3)

    print("📊 Totals After Free Tier (provisioned tables only):")
    print(f" - Read Units (billable): {tier['billable_read']}")
    print(f" - Write Units (billable): {tier['billable_write']}")
    print(f" - Storage GB (billable): {tier['billable_storage']}")
    print(f" - Free Tier Discount: ${tier['discount']}")
    print(f"💰 Estimated Monthly Cost (All Tables): ${adjusted_cost}")
    print("\n🔗 https://aws.amazon.com/dynamodb/pricing/")