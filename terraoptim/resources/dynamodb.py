#!/usr/bin/env python3
import math

import boto3
import json

from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan, REGION_CODE_MAP

# AWS Free Tier for DynamoDB
FREE_TIER_READ_CAPACITY = 25  # units
FREE_TIER_WRITE_CAPACITY = 25  # units
FREE_TIER_STORAGE_GB = 25  # GB
HOURS_PER_MONTH = 730


def get_dynamodb_price_provisioned(region, usage_type):
    pricing = boto3.client("pricing", region_name="us-east-1")
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")
    region_prefix = REGION_CODE_MAP.get(region, "")
    if region_prefix and region != "us-east-1":  # for us-east-1 there is no prefix
        usage_type = f"{region_prefix}-{usage_type}"

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
                    price = dim.get("pricePerUnit", {}).get("USD")
                    if description and 'beyond the free tier' in description or 'storage used beyond' in description:
                        return float(price)

    except Exception as e:
        print(f"️ Error fetching price for {usage_type}: {e}")
    return None


def get_dynamodb_price_on_demand(region, usage_type):
    pricing = boto3.client("pricing", region_name="us-east-1")
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")
    region_prefix = REGION_CODE_MAP.get(region, "")
    if region_prefix and region != "us-east-1":  # for us-east-1 there is no prefix
        usage_type = f"{region_prefix}-{usage_type}"
    try:
        response = pricing.get_products(
            ServiceCode="AmazonDynamoDB",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "usagetype", "Value": usage_type}

            ]
        )
        for product_json in response["PriceList"]:
            product = json.loads(product_json)
            terms = product.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                price_dimensions = term.get("priceDimensions", {})
                for dim in price_dimensions.values():
                    price = dim.get("pricePerUnit", {}).get("USD")
                    return float(price)

    except Exception as e:
        print(f"️ Error: {e}")

    return None


def extract_dynamodb_tables(terraform_data):
    tables = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_dynamodb_table":
            after = resource.get("change", {}).get("after", {})
            billing_mode = after.get("billing_mode", "PROVISIONED")
            read_capacity = after.get("read_capacity", 0)
            write_capacity = after.get("write_capacity", 0)
            tables.append({
                "billing_mode": billing_mode,
                "read_capacity": read_capacity,
                "write_capacity": write_capacity
            })
    return tables


def calculate_table_cost(table, prices, user_defaults):
    billing = table.get("billing_mode", "PROVISIONED").upper()
    storage = user_defaults["storage"]
    storage_cost = round(storage * prices["storage"], 3)

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
        read = user_defaults["reads"]
        write = user_defaults["writes"]

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

    return None


def apply_free_tier(total_read, total_write, total_storage, prices):
    billable_read = max(total_read - FREE_TIER_READ_CAPACITY, 0)
    billable_write = max(total_write - FREE_TIER_WRITE_CAPACITY, 0)
    billable_storage = max(total_storage - FREE_TIER_STORAGE_GB, 0)

    discount = (
            round((total_read - billable_read) * HOURS_PER_MONTH * prices["read_prov"], 3) +
            round((total_write - billable_write) * HOURS_PER_MONTH * prices["write_prov"], 3) +
            round((total_storage - billable_storage) * prices["storage"], 3)
    )

    return {
        "billable_read": billable_read,
        "billable_write": billable_write,
        "billable_storage": billable_storage,
        "discount": discount
    }


def recommend_billing_mode(reads, writes, prices):
    seconds_per_month = 30 * 24 * 60 * 60
    provisioned_read_units = math.ceil(reads / seconds_per_month)
    provisioned_write_units = math.ceil(writes / seconds_per_month)

    cost_provisioned = (
            provisioned_read_units * HOURS_PER_MONTH * prices["read_prov"] +
            provisioned_write_units * HOURS_PER_MONTH * prices["write_prov"]
    )
    cost_on_demand = (
            reads * prices["read_ondemand"] +
            writes * prices["write_ondemand"]
    )

    recommendation = "PAY_PER_REQUEST" if cost_on_demand < cost_provisioned else "PROVISIONED"

    return {
        "estimated_cost_provisioned": round(cost_provisioned, 3),
        "estimated_cost_on_demand": round(cost_on_demand, 3),
        "recommendation": recommendation
    }


def calculate_dynamodb_table_costs(tables, prices, user_defaults):
    total_cost = 0
    total_prov_read = 0
    total_prov_write = 0
    total_storage = 0
    results = []

    for i, table in enumerate(tables):
        result = calculate_table_cost(table, prices, user_defaults)
        if result is None:
            results.append({
                "index": i,
                "skipped": True
            })
            continue

        subtotal = result["cost_read"] + result["cost_write"] + result["cost_storage"]
        total_cost += subtotal
        total_storage += result["storage"]

        if result["mode"] == "PROVISIONED":
            total_prov_read += result["read"]
            total_prov_write += result["write"]

        results.append({
            "index": i,
            "skipped": False,
            "mode": result["mode"],
            "read": result["read"],
            "write": result["write"],
            "storage": result["storage"],
            "cost_read": result["cost_read"],
            "cost_write": result["cost_write"],
            "cost_storage": result["cost_storage"],
            "subtotal": round(subtotal,3)
        })

    return results, total_prov_read, total_prov_write, total_storage, total_cost

def print_dynamodb_table_costs(results):
    for r in results:
        if r["skipped"]:
            print(f" Table {r['index']} | ⚠️ Unknown billing mode. Skipping...\n")
            continue

        print(f"  Table {r['index']} | Mode: {r['mode']}")
        print(f"    Reads: {r['read']} | Writes: {r['write']} | Storage: {r['storage']} GB")
        print(f"    Cost: Read ${r['cost_read']}, Write ${r['cost_write']}, "
              f"Storage ${r['cost_storage']}, Total ${r['subtotal']}\n")

def summarize_dynamodb_totals(total_prov_read, total_prov_write, total_storage, total_cost, prices, user_defaults):
    tier = apply_free_tier(total_prov_read, total_prov_write, total_storage, prices)
    adjusted_cost = round(total_cost - tier["discount"], 3)

    print(" Totals After Free Tier (provisioned tables only):")
    print(f"   Read Units (billable): {tier['billable_read']}")
    print(f"   Write Units (billable): {tier['billable_write']}")
    print(f"   Storage GB (billable): {tier['billable_storage']}")
    print(f"   Free Tier Discount: ${tier['discount']}")
    print(f" Total Estimated Monthly Cost For All Tables: ${adjusted_cost}")

    rec = recommend_billing_mode(
        reads=user_defaults["reads"],
        writes=user_defaults["writes"],
        prices=prices
    )

    print("\n  Recommendation Based on Usage (WITHOUT STORAGE COSTS):")
    print(f"   Cost if Provisioned: ${rec['estimated_cost_provisioned']}")
    print(f"   Cost if Pay-Per-Request: ${rec['estimated_cost_on_demand']}")
    print(f"  Recommended Billing Mode: {rec['recommendation']}")
    print("\n🔗 More info: https://aws.amazon.com/dynamodb/pricing/")
    print("====================================================")

def dynamodb_main(terraform_data=None, params=None):
    region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"
    tables = extract_dynamodb_tables(terraform_data) if terraform_data else []

    if not tables:
        print("️ No DynamoDB tables found in Terraform data.")
        return
    print(f"\n Found {len(tables)} DynamoDB Tables:")

    user_defaults = {
        "reads": 1_000_000,
        "writes": 500_000,
        "storage": 10
    }
    if isinstance(params, dict):
        user_defaults["reads"] = params.get("reads", user_defaults["reads"])
        user_defaults["writes"] = params.get("writes", user_defaults["writes"])
        user_defaults["storage"] = params.get("storage", user_defaults["storage"])

    reads = user_defaults["reads"]
    writes = user_defaults["writes"]
    storage = user_defaults["storage"]

    print(f" Reads: {reads}")
    print(f" Writes: {writes}")
    print(f" Storage: {storage}")
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


    results, total_prov_read, total_prov_write, total_storage, total_cost = calculate_dynamodb_table_costs(
        tables, prices, user_defaults
    )

    print_dynamodb_table_costs(results)
    summarize_dynamodb_totals(total_prov_read, total_prov_write, total_storage, total_cost, prices, user_defaults)
