#!/usr/bin/env python3
import math
import boto3
import json
from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan, REGION_CODE_MAP


FREE_TIER_READ_CAPACITY = 25
FREE_TIER_WRITE_CAPACITY = 25
FREE_TIER_STORAGE_GB = 25
HOURS_PER_MONTH = 720


def get_dynamodb_price_provisioned(region, usage_type):
    """
    Retrieves the DynamoDB provisioned pricing for a specific usage type and region.
    Filters to only include pricing that applies beyond the free tier.
    """
    return get_dynamodb_price(region, usage_type, filter_description=True)

def get_dynamodb_price_on_demand(region, usage_type):
    """
    Retrieves the DynamoDB on-demand pricing for a specific usage type and region.
    Returns the first available price without filtering for free tier conditions.
    """
    return get_dynamodb_price(region, usage_type)


def get_dynamodb_price(region, usage_type, filter_description=False):
    """
    Generic helper function to retrieve DynamoDB pricing for a given usage type and region.

    Args:
        region (str): AWS region code (e.g., 'us-west-2').
        usage_type (str): Usage type string (e.g., 'ReadCapacityUnit-Hrs').
        filter_description (bool): If True, filters price entries for post-free-tier usage.

    Returns:
        float or None: Price in USD or None if not found or on error.
    """
    pricing = boto3.client("pricing", region_name="us-east-1")
    location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")
    region_prefix = REGION_CODE_MAP.get(region, "")
    if region_prefix and region != "us-east-1":
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
                    if filter_description:
                        if description and 'beyond the free tier' in description or 'storage used beyond' in description:
                            return float(price)
                    else:
                        return float(price)
    except Exception as e:
        print(f"️ Failed to fetch price for {usage_type}")
        raise e
    return None



def extract_dynamodb_tables(terraform_data):
    """
    Extracts DynamoDB table configuration from Terraform plan data.

    Args:
        terraform_data (dict): Parsed Terraform plan JSON.

    Returns:
        list: A list of dictionaries containing billing mode, read, and write capacity for each table.
    """
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
    """
    Calculates the monthly cost of a DynamoDB table based on pricing and usage.

    Args:
        table (dict): DynamoDB table details (billing mode, read/write capacity).
        prices (dict): Dictionary with pricing info for provisioned and on-demand modes.
        user_defaults (dict): Default read/write/storage values for PAY_PER_REQUEST mode.

    Returns:
        dict: Cost breakdown including mode, read/write/storage values, and their respective costs.
    """
    billing = table.get("billing_mode", "PROVISIONED").upper()
    storage = user_defaults["storage"]
    storage_cost = round(storage * prices["storage"], 3)

    read = table.get("read_capacity") if billing == "PROVISIONED" else user_defaults["reads"]
    write = table.get("write_capacity") if billing == "PROVISIONED" else user_defaults["writes"]

    cost_read = round(read * (HOURS_PER_MONTH if billing == "PROVISIONED" else 1) * prices[
        "read_prov" if billing == "PROVISIONED" else "read_ondemand"], 3)
    cost_write = round(write * (HOURS_PER_MONTH if billing == "PROVISIONED" else 1) * prices[
        "write_prov" if billing == "PROVISIONED" else "write_ondemand"], 3)

    return {
        "mode": billing,
        "read": read,
        "write": write,
        "storage": storage,
        "cost_read": cost_read,
        "cost_write": cost_write,
        "cost_storage": storage_cost
    }


def apply_free_tier(total_read, total_write, total_storage, prices):
    """
    Applies the AWS free tier limits to DynamoDB usage and calculates the discount.

    Args:
        total_read (int): Total read capacity units used per month.
        total_write (int): Total write capacity units used per month.
        total_storage (float): Total storage used in GB.
        prices (dict): Dictionary with provisioned read/write and storage prices.

    Returns:
        dict: Billable units after free tier and total discount value.
    """

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
    """
    Recommends the most cost-effective DynamoDB billing mode based on usage.

    Args:
        reads (int): Total number of read requests per month.
        writes (int): Total number of write requests per month.
        prices (dict): Dictionary with on-demand and provisioned read/write prices.

    Returns:
        dict: Estimated monthly cost for both billing modes and the recommended mode.
    """
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
    """
    Calculates the cost breakdown for each DynamoDB table based on billing mode, capacity, and storage.

    Args:
        tables (list): List of table configurations extracted from Terraform.
        prices (dict): DynamoDB pricing details for various usage types.
        user_defaults (dict): Default read/write/storage values for on-demand tables.

    Returns:
        tuple: List of table cost breakdowns and overall usage totals (read, write, storage, cost).
    """
    total_cost = 0
    total_prov_read = 0
    total_prov_write = 0
    total_storage = 0
    results = []

    for i, table in enumerate(tables):
        result = calculate_table_cost(table, prices, user_defaults)
        if result is None:
            results.append({
                "index": i+1,
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
    """
    Prints a formatted summary of the cost details for each DynamoDB table.

    Args:
        results (list): List of dictionaries containing cost breakdowns for each table.
    """
    for r in results:
        if r["skipped"]:
            print(f" Table {r['index']+1} | ️ Unknown billing mode. Skipping...\n")
            continue

        print(f"  Table {r['index']+1} | Mode: {r['mode']}")
        print(f"    Reads: {r['read']} | Writes: {r['write']} | Storage: {r['storage']} GB")
        print(f"    Cost: Read ${r['cost_read']}, Write ${r['cost_write']}, "
              f"Storage ${r['cost_storage']}, Total ${r['subtotal']}\n")

def summarize_dynamodb_totals(total_prov_read, total_prov_write, total_storage, total_cost, prices, user_defaults):
    """
    Applies free tier discount, summarizes overall usage and cost, and provides billing mode recommendation.

    Args:
        total_prov_read (int): Total provisioned read capacity units.
        total_prov_write (int): Total provisioned write capacity units.
        total_storage (float): Total storage used in GB.
        total_cost (float): Total unadjusted cost across all tables.
        prices (dict): DynamoDB pricing details.
        user_defaults (dict): Default read/write values for cost recommendations.
    """
    tier = apply_free_tier(total_prov_read, total_prov_write, total_storage, prices)
    adjusted_cost = round(total_cost - tier["discount"], 3)
    read_final = round(tier['billable_read'] * HOURS_PER_MONTH * prices["read_prov"],3)
    write_final = round(tier['billable_write'] * HOURS_PER_MONTH * prices["write_prov"],3)
    storage_final = round(tier['billable_storage'] * prices["storage"],3)

    print("\n AWS Free Tier Limits:")
    print(f"   {FREE_TIER_READ_CAPACITY} read units")
    print(f"   {FREE_TIER_WRITE_CAPACITY} write units")
    print(f"   {FREE_TIER_STORAGE_GB} GB storage\n")

    print(f"\n Total Usage This Month:")
    print(f"   Read Units: {total_prov_read} (Billable: {tier['billable_read']})")
    print(f"   Write Units: {total_prov_write} (Billable: {tier['billable_write']})")
    print(f"   Storage GB: {total_storage} (Billable: {tier['billable_storage']})")

    print("\n Final Monthly Cost After Free Tier (provisioned tables only):")
    print(f"   Read Units: ${read_final}")
    print(f"   Write Units: ${write_final}")
    print(f"   Storage: ${storage_final}")
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
    print("\n More info: https://aws.amazon.com/dynamodb/pricing/")
    print("====================================================")

def dynamodb_main(terraform_data=None, params=None):
    try:
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
        allowed_keys = set(user_defaults.keys())
        if isinstance(params, dict):
            unknown_keys = set(params.keys()) - allowed_keys
            if unknown_keys:
                print(f"️ Optimization Warning: Unrecognized parameter(s): {', '.join(unknown_keys)}")
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
            print(" Unable to retrieve all required prices.")
            return


        results, total_prov_read, total_prov_write, total_storage, total_cost = calculate_dynamodb_table_costs(
            tables, prices, user_defaults
        )

        print_dynamodb_table_costs(results)
        summarize_dynamodb_totals(total_prov_read, total_prov_write, total_storage, total_cost, prices, user_defaults)
    except Exception as e:
        print(f"️ Error calculating dynamodb optimization")