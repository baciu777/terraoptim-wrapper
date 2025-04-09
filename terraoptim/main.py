#!/usr/bin/env python3

import argparse
import subprocess
import json
from terraoptim.resources.ec2 import ec2_main
from terraoptim.resources.lambda_functions import lambda_main
from terraoptim.resources.s3 import s3_main


def run_terraform_command(terraform_args):
    """Function to run terraform commands directly."""
    try:
        print(f"Running terraform {' '.join(terraform_args)} ...")

        # Run the terraform command with provided arguments
        subprocess.run(["terraform"] + terraform_args  , check=True)

    except subprocess.CalledProcessError as e:
        exit()

def run_terraform_command_out(terraform_args):
    """Function to run terraform commands directly."""
    try:
        print(f"Running terraform {' '.join(terraform_args)} ...")

        # Run the terraform command with provided arguments
        subprocess.run(["terraform"] + terraform_args + ["-out=terraform.tfplan"], check=True)
    except subprocess.CalledProcessError as e:
        exit()


def load_terraform_plan():
    """Load and parse the saved terraform plan."""
    try:
        print("Loading terraform plan...")
        # Convert the terraform plan to a JSON format
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

    print(optimization_types)
    if optimization_types == []:
        print("Running optimization without specific arguments...")
        ec2_main(plan_data, None)
        lambda_main(plan_data, None)
        s3_main(plan_data, None)
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
            print(f"🔧 Running EC2 optimization with parameters: {params}")
            ec2_main(plan_data, params)

        elif optimization_type == "lambda":
            print(f"🔧 Running Lambda optimization with parameters: {params}")
            lambda_main(plan_data, params)

        elif optimization_type == "s3":
            print(f"🔧 Running S3 optimization with parameters: {params}")
            s3_main(plan_data, params)

        else:
            print(f"❌ Unsupported optimization type: {optimization_type}")


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
        print(terraform_args)
        print(optimization_args)
        if any(command in terraform_args for command in ["plan", "apply"]) and found_optimization:
            run_terraform_command_out(terraform_args)
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
