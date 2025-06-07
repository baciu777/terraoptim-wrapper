REGION_NAME_MAP = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "ca-central-1": "Canada (Central)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-central-2": "EU (Zurich)",
    "eu-north-1": "EU (Stockholm)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-south-2": "Asia Pacific (Hyderabad)",
    "af-south-1": "Africa (Cape Town)",
    "me-south-1": "Middle East (Bahrain)",
    "me-central-1": "Middle East (UAE)",
    "sa-east-1": "South America (São Paulo)",
    "us-gov-west-1": "AWS GovCloud (US-West)",
    "us-gov-east-1": "AWS GovCloud (US-East)"
}

REGION_CODE_MAP = {
    "us-east-1": "USE1",
    "us-east-2": "USE2",
    "us-west-1": "USW1",
    "us-west-2": "USW2",
    "ca-central-1": "CAN1",
    "eu-west-1": "EU",
    "eu-west-2": "EUW2",
    "eu-west-3": "EUW3",
    "eu-central-1": "EUC1",
    "eu-central-2": "EUC2",
    "eu-north-1": "EUN1",
    "ap-northeast-1": "APN1",
    "ap-northeast-2": "APN2",
    "ap-northeast-3": "APN3",
    "ap-southeast-1": "APS1",
    "ap-southeast-2": "APS2",
    "ap-south-1": "APS3",
    "ap-south-2": "APS4",
    "af-south-1": "AFS1",
    "me-south-1": "MES1",
    "me-central-1": "MEC1",
    "sa-east-1": "SAE1",
    "us-gov-west-1": "USG1",
    "us-gov-east-1": "USG2"
}


def extract_region_from_terraform_plan(terraform_data):
    """
    Extracts the AWS region from a Terraform plan's provider configuration.

    Args:
        terraform_data (dict): The parsed JSON data from a Terraform plan.

    Returns:
        str or None: The AWS region if found, otherwise None.
    """
    providers = terraform_data.get("configuration", {}).get("provider_config", {})
    aws_provider = providers.get("aws", {})
    region_expr = aws_provider.get("expressions", {}).get("region", {})
    region = region_expr.get("constant_value")

    if region:
        return region
    return region


