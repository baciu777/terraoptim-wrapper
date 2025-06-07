#!/usr/bin/env python3

import argparse
import subprocess
import json
from terraoptim.resources.ec2 import ec2_main
from terraoptim.resources.glue import glue_main
from terraoptim.resources.lambda_functions import lambda_main
from terraoptim.resources.s3 import s3_main
from terraoptim.resources.dynamodb import dynamodb_main
from terraoptim.resources.unused import unused_main


def run_terraform_command(terraform_args):
    """Function to run terraform commands directly."""
    try:
        print(f"Running terraform {' '.join(terraform_args)} ...")

        # Run the terraform command with provided arguments
        subprocess.run(["terraform"] + terraform_args, check=True)

    except subprocess.CalledProcessError as e:
        return


def run_plan(terraform_args):
    try:
        print(f"Running terraform {' '.join(terraform_args)} ...")
        subprocess.run(["terraform"] + terraform_args + ["-out=terraform.tfplan"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        return False


def run_apply(terraform_args):
    try:
        print(f"Running terraform apply ...")
        terraform_args = ["plan" if arg == "apply" else arg for arg in terraform_args]

        success = run_plan(terraform_args)
        if not success:
            return False

        subprocess.run(["terraform", "apply", "terraform.tfplan"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        return False

def load_terraform_plan():
    """Load and parse the saved terraform plan."""
    try:
        print("Loading terraform plan...")
        result = subprocess.run(
            ["terraform", "show", "-json", "terraform.tfplan"], capture_output=True, text=True, check=True
        )
        plan_data = json.loads(result.stdout)
        return plan_data
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to load terraform plan: {e}")
        raise e


def process_optimizations(optimization_types, plan_data):
    """Process and execute the optimizations provided in the argument list."""

    if optimization_types == []:
        print("Running optimization without specific arguments...")
        ec2_main(plan_data, None)
        lambda_main(plan_data, None)
        s3_main(plan_data, None)
        dynamodb_main(plan_data, None)
        glue_main(plan_data, None)
        unused_main(None)
        return
    i = 0
    while i < len(optimization_types):
        optimization_type = optimization_types[i].lstrip("-")
        i += 1
        params = {}

        # Collect all key=value args until the next optimization type (e.g., ec2, lambda, etc.)
        while i < len(optimization_types) and "=" in optimization_types[i]:
            key, value = optimization_types[i].split("=", 1)
            # Try to convert numeric values
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass  # Keep as string
            params[key] = value
            i += 1

        # Process the optimization types
        if optimization_type == "ec2":
            print(f" Running EC2 optimization with parameters: {params}")
            ec2_main(plan_data, params)

        elif optimization_type == "lambda":
            print(f" Running Lambda optimization with parameters: {params}")
            lambda_main(plan_data, params)

        elif optimization_type == "s3":
            print(f" Running S3 optimization with parameters: {params}")
            s3_main(plan_data, params)
        elif optimization_type == "dynamodb":
            print(f" Running DynamoDB optimization with parameters: {params}")
            dynamodb_main(plan_data, params)
        elif optimization_type == "glue":
            print(f" Running Glue optimization with parameters: {params}")
            glue_main(plan_data, params)
        elif optimization_type == "unused":
            print(f" Running Unused Resource Check with parameters: {params}")
            unused_main(params)
        else:
            print(f" Unsupported optimization type: {optimization_type}")


def main():
    try:
        parser = argparse.ArgumentParser(description="Terraform Cost Optimization Tool")

        # Allow optimizations to be optional
        parser.add_argument(
            "--optimization", "-o", help="Optimization types with parameters"
        )

        # Allow additional arguments to be passed for the terraform command (like -var, etc.)
        parser.add_argument(
            "additional_args", nargs=argparse.REMAINDER, help="Additional arguments for the terraform command"
        )

        args = parser.parse_args()

        terraform_args = []
        optimization_args = []
        found_optimization = False
        for arg in args.additional_args:
            if not found_optimization:
                if arg in ["--optimization", "-o"]:
                    found_optimization = True
                else:
                    terraform_args.append(arg)
            else:
                optimization_args.append(arg)
        if any(command in terraform_args for command in ["plan", "apply"]) and found_optimization:
            plan_success = False
            if "plan" in terraform_args:
                plan_success = run_plan(terraform_args)
            elif "apply" in terraform_args:
                plan_success = run_apply(terraform_args)

            if not plan_success:
                return

            plan_data = load_terraform_plan()
            if not plan_data:
                return

            process_optimizations(optimization_args, plan_data)
        else:
            run_terraform_command(terraform_args)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
