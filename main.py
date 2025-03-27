#!/usr/bin/env python3

import argparse
from ec2 import main as ec2_main  # Import the main function from ec2.py

def main():
    parser = argparse.ArgumentParser(description="Terraform Cost Optimization Tool")
    parser.add_argument("-optimization", type=str, required=True, help="Optimization type")
    parser.add_argument("plan", nargs="?", help="Run Terraform plan", default=None)

    args = parser.parse_args()

    # Directly pass the optimization to the respective optimization method
    if args.optimization == "ec2":
        # If optimization is ec2, call the main function from ec2.py
        ec2_main(args)
    else:
        print(f"❌ Unsupported optimization type: {args.optimization}")

if __name__ == "__main__":
    main()
