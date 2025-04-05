REGION_NAME_MAP = {
    "us-east-1": "US East (N. Virginia)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "eu-west-1": "EU (Ireland)",
    "eu-central-1": "EU (Frankfurt)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
}


def extract_region_from_terraform_plan(terraform_data):
    """ Extract region from Terraform plan """
    region = None
    for resource in terraform_data.get("provider_changes", []):
        if resource["type"] == "aws":
            region = resource["change"]["after"].get("region")
            if region:
                break
    return region


