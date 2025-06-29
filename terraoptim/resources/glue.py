import json
import boto3
from terraoptim.common.utils import REGION_NAME_MAP, extract_region_from_terraform_plan, REGION_CODE_MAP

DPU_PER_WORKER = {
    "Standard": 1,
    "G.025X": 0.25,
    "G.1X": 1,
    "G.2X": 2,
    "G.4X": 4,
    "G.8X": 8,
    "Z.2X": 8
}

GLUE_WORKER_SPECS = {
    "Standard": {"vCPU": 4, "Memory": "16 GB", "Disk": "64 GB"},
    "G.025X": {"vCPU": 1, "Memory": "4 GB", "Disk": "64 GB"},
    "G.1X": {"vCPU": 4, "Memory": "16 GB", "Disk": "64 GB"},
    "G.2X": {"vCPU": 8, "Memory": "32 GB", "Disk": "128 GB"},
    "G.4X": {"vCPU": 16, "Memory": "64 GB", "Disk": "256 GB"},
    "G.8X": {"vCPU": 32, "Memory": "128 GB", "Disk": "512 GB"},
    "Z.2X": {"vCPU": 8, "Memory": "64 GB", "Disk": "128 GB"},
}

def get_glue_price(region, worker_type="Standard"):
    """
    Fetch AWS Glue price per DPU hour for a given region and worker type.

    Args:
        region (str): AWS region code.
        worker_type (str): Glue worker type (default: "Standard").

    Returns:
        float or None: Price per DPU hour in USD, or None if fetch fails.
    """
    try:
        client = boto3.client("pricing", region_name="us-east-1")

        location = REGION_NAME_MAP.get(region, "US East (N. Virginia)")

        region_code = REGION_CODE_MAP.get(region, "USE1")
        usage_type = f"{region_code}-ETL-DPU-Hour"

        if not usage_type:
            print(f"️ Unsupported worker type: {worker_type}")
            return None

        response = client.get_products(
            ServiceCode="AWSGlue",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "usageType", "Value": usage_type},
            ]
        )
        for price_item in response["PriceList"]:
            price_data = json.loads(price_item)
            for term in price_data.get("terms", {}).get("OnDemand", {}).values():
                for dim in term.get("priceDimensions", {}).values():
                    return float(dim["pricePerUnit"]["USD"])
    except Exception as e:
        print(f"️  Failed to fetch glue price for {worker_type}")
        raise e
    return None


def extract_glue_jobs(terraform_data):
    """
    Extract AWS Glue job configurations from Terraform plan data.

    Args:
        terraform_data (dict): Terraform plan data.

    Returns:
        list of dict: List of Glue jobs with 'name', 'worker_type', and 'num_workers'.
    """
    jobs = []
    for resource in terraform_data.get("resource_changes", []):
        if resource["type"] == "aws_glue_job":
            after = resource["change"]["after"]
            worker_type = after.get("worker_type", "Standard")
            num_workers = after.get("number_of_workers", 10)
            name = after.get("name", "Unnamed")
            jobs.append({
                "name": name,
                "worker_type": worker_type,
                "num_workers": num_workers,
            })
    return jobs


def calculate_glue_cost(worker_type, num_workers, monthly_hours, region):
    """
    Calculate the estimated monthly cost of an AWS Glue job.

    Args:
        worker_type (str): Glue worker type.
        num_workers (int): Number of workers.
        monthly_hours (int or float): Monthly usage hours.
        region (str): AWS region code.

    Returns:
        float or None: Estimated monthly cost in USD, or None if price not found.
    """
    price_per_dpu_hr = get_glue_price(region, worker_type)
    if not price_per_dpu_hr:
        return None

    dpu_per_worker = DPU_PER_WORKER.get(worker_type, 1)
    total_dpu_hours = num_workers * dpu_per_worker * monthly_hours
    return round(total_dpu_hours * price_per_dpu_hr, 2)



def suggest_glue_alternatives(job, region, monthly_hours):
    """
    Suggest one smaller and one larger worker type alternative for a Glue job.

    Args:
        job (dict): Glue job dict with 'worker_type' and 'num_workers'.
        region (str): AWS region code.
        monthly_hours (int or float): Monthly usage hours.

    Returns:
        list of dict: Suggested alternatives with 'worker_type' and 'monthly_cost'.
    """
    types = ["G.025X", "Standard", "G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"]

    current_idx = types.index(job["worker_type"])

    suggestions = []
    for offset in [-1, 1]:
        new_idx = current_idx + offset
        if 0 <= new_idx < len(types):
            new_type = types[new_idx]

            if {job["worker_type"], new_type} == {"Standard", "G.1X"}:
                continue

            cost = calculate_glue_cost(
                new_type,
                job["num_workers"],
                monthly_hours,
                region
            )
            suggestions.append({
                "worker_type": new_type,
                "monthly_cost": cost
            })
    return suggestions
def calculate_glue_job_costs(jobs, hours, region):
    """
    Print detailed cost and specification info for each Glue job.

    Args:
        jobs (list): List of Glue jobs.
        hours (int or float): Monthly usage hours.
        region (str): AWS region code.

    Returns:
        float: Total estimated monthly cost for all jobs.
    """

    total_glue_cost = 0.0

    for job in jobs:
        print(f"\n  Glue Job: {job['name']}")
        print(f"    Worker Type: {job['worker_type']}")
        print(f"    Number of Workers: {job['num_workers']}")
        print(f"    Usage: {hours} hours/month")

        specs = GLUE_WORKER_SPECS.get(job["worker_type"], {})
        print(f"    CPU: {specs.get('vCPU')} vCPU")
        print(f"    Memory: {specs.get('Memory')}")
        print(f"    Disk: {specs.get('Disk')}")

        cost = calculate_glue_cost(
            job["worker_type"],
            job["num_workers"],
            hours,
            region
        )

        if cost is not None:
            print(f"    Cost: ${cost}")
            total_glue_cost += cost

        suggestions = suggest_glue_alternatives(job, region, hours)
        if suggestions:
            print("    Alternatives:")
            print(f"    {'Worker Type':<12} | {'vCPU':<4} | {'Memory':<10} | {'Disk':<8} | {'Monthly Cost'}")
            print("    " + "-" * 60)
            for alt in suggestions:
                alt_specs = GLUE_WORKER_SPECS.get(alt["worker_type"], {})
                print(f"    {alt['worker_type']:<12} | "
                      f"{alt_specs.get('vCPU', '?'):>4} | "
                      f"{alt_specs.get('Memory', '?'):>10} | "
                      f"{alt_specs.get('Disk', '?'):>8} | "
                      f"${alt['monthly_cost']}")
    print(f"\n Total Estimated Monthly Cost: ${round(total_glue_cost, 3)}")
    print("\n More info: https://aws.amazon.com/glue/pricing/")
    print("=====================================================")




def glue_main(terraform_data, params=None):
    try:
        region = extract_region_from_terraform_plan(terraform_data) or "us-east-1"

        jobs = extract_glue_jobs(terraform_data)
        if not jobs:
            print(" No Glue jobs found in Terraform plan.")
            return
        print(f"\n Found {len(jobs)} Glue Jobs:")

        user_defaults = {
            "hours": 10,
        }

        allowed_keys = set(user_defaults.keys())
        if isinstance(params, dict):
            unknown_keys = set(params.keys()) - allowed_keys
            if unknown_keys:
                print(f"️ Optimization Warning: Unrecognized parameter(s): {', '.join(unknown_keys)}")
            user_defaults["hours"] = params.get("hours", user_defaults["hours"])
        hours = user_defaults["hours"]
        print(f" Hours: {hours}")
        calculate_glue_job_costs(jobs, hours, region)
    except Exception as e:
        print(f"️ Error calculating glue optimization")