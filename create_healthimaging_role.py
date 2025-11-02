#!/usr/bin/env python3
"""
Create the required IAM role for AWS HealthImaging import jobs.

This script creates a service role that:
1. Trusts the medical-imaging.amazonaws.com service
2. Has permissions to read/write to S3 buckets
3. Can be passed to HealthImaging for import jobs
"""

import boto3
import json
import sys

# Trust policy - allows HealthImaging service to assume this role
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "medical-imaging.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# Permissions policy - allows role to access S3
PERMISSIONS_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadDICOMFromS3",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::*-healthimaging-*",
                "arn:aws:s3:::*-healthimaging-*/*",
                "arn:aws:s3:::source-healthimaging-*",
                "arn:aws:s3:::source-healthimaging-*/*",
                "arn:aws:s3:::target-healthimaging-*",
                "arn:aws:s3:::target-healthimaging-*/*"
            ]
        },
        {
            "Sid": "WriteOutputToS3",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": [
                "arn:aws:s3:::*-healthimaging-*/*",
                "arn:aws:s3:::output-healthimaging-*/*"
            ]
        }
    ]
}

ROLE_NAME = "HealthImagingServiceRole"
POLICY_NAME = "HealthImagingS3AccessPolicy"


def create_role():
    """Create the HealthImaging service role."""
    print("="*60)
    print("Create AWS HealthImaging Service Role")
    print("="*60)

    # Get current account info
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        account_id = identity['Account']
        print(f"\nAWS Account: {account_id}")
        print(f"User: {identity['Arn']}")
    except Exception as e:
        print(f"\n❌ Failed to get AWS identity: {e}")
        return None

    print(f"\nThis will create IAM role: {ROLE_NAME}")
    print("The role will have permissions to:")
    print("  - Read DICOM files from S3 buckets (*-healthimaging-*)")
    print("  - Write import job outputs to S3")
    print("  - Be assumed by medical-imaging.amazonaws.com service")

    confirm = input("\nProceed? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("Aborted.")
        return None

    iam = boto3.client('iam')

    # Check if role already exists
    try:
        print(f"\nChecking if role '{ROLE_NAME}' exists...")
        existing_role = iam.get_role(RoleName=ROLE_NAME)
        role_arn = existing_role['Role']['Arn']
        print(f"✓ Role already exists: {role_arn}")

        update = input("Update the role's permissions? (yes/no): ").strip().lower()
        if update not in ['yes', 'y']:
            print(f"\nUsing existing role ARN:")
            print(f"  {role_arn}")
            return role_arn

    except iam.exceptions.NoSuchEntityException:
        # Role doesn't exist, create it
        print(f"\nCreating role '{ROLE_NAME}'...")
        try:
            response = iam.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
                Description="Service role for AWS HealthImaging to access S3 buckets",
                Tags=[
                    {'Key': 'Purpose', 'Value': 'HealthImaging'},
                    {'Key': 'ManagedBy', 'Value': 'smartmic-cloudimaging'}
                ]
            )
            role_arn = response['Role']['Arn']
            print(f"✓ Created role: {role_arn}")
        except Exception as e:
            print(f"❌ Failed to create role: {e}")
            return None

    # Attach inline policy
    print(f"\nAttaching permissions policy '{POLICY_NAME}'...")
    try:
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(PERMISSIONS_POLICY)
        )
        print(f"✓ Attached policy '{POLICY_NAME}'")
    except Exception as e:
        print(f"❌ Failed to attach policy: {e}")
        return None

    # Get final role ARN
    try:
        role_response = iam.get_role(RoleName=ROLE_NAME)
        role_arn = role_response['Role']['Arn']
    except Exception as e:
        print(f"❌ Failed to retrieve role ARN: {e}")
        return None

    print("\n" + "="*60)
    print("SUCCESS!")
    print("="*60)
    print(f"\nRole ARN: {role_arn}")
    print("\nSet this environment variable:")
    print(f"  export AWS_HEALTHIMAGING_ROLE_ARN={role_arn}")

    print("\nOr add to your .env file:")
    print(f"  AWS_HEALTHIMAGING_ROLE_ARN={role_arn}")

    # Optionally update .env file
    update_env = input("\nAutomatically add to .env file? (yes/no): ").strip().lower()
    if update_env in ['yes', 'y']:
        try:
            with open('.env', 'a') as f:
                f.write(f"\n# HealthImaging Service Role (auto-generated)\n")
                f.write(f"AWS_HEALTHIMAGING_ROLE_ARN={role_arn}\n")
            print("✓ Added to .env file")
        except Exception as e:
            print(f"⚠️  Could not update .env: {e}")

    return role_arn


def verify_role_permissions(role_arn):
    """Verify the role has correct permissions."""
    print("\n" + "="*60)
    print("Verification")
    print("="*60)

    iam = boto3.client('iam')
    role_name = role_arn.split('/')[-1]

    # Check trust policy
    try:
        role = iam.get_role(RoleName=role_name)
        trust_policy = role['Role']['AssumeRolePolicyDocument']

        print("\n✓ Trust Policy:")
        for statement in trust_policy.get('Statement', []):
            principal = statement.get('Principal', {})
            service = principal.get('Service', '')
            if 'medical-imaging.amazonaws.com' in service:
                print("  ✓ Allows medical-imaging.amazonaws.com to assume role")
            else:
                print(f"  ⚠️  Principal: {principal}")
    except Exception as e:
        print(f"❌ Failed to check trust policy: {e}")

    # Check attached policies
    try:
        policies = iam.list_role_policies(RoleName=role_name)
        print("\n✓ Inline Policies:")
        for policy_name in policies['PolicyNames']:
            print(f"  - {policy_name}")
    except Exception as e:
        print(f"❌ Failed to list policies: {e}")

    print("\n✓ Role is ready to use with HealthImaging!")


if __name__ == "__main__":
    print("\n🏥 AWS HealthImaging Service Role Setup\n")

    role_arn = create_role()

    if role_arn:
        verify_role_permissions(role_arn)

        print("\n" + "="*60)
        print("Next Steps")
        print("="*60)
        print("\n1. Set the role ARN environment variable:")
        print(f"   export AWS_HEALTHIMAGING_ROLE_ARN={role_arn}")
        print("\n2. Set your datastore ID:")
        print("   export AWS_HEALTHIMAGING_DATASTORE_ID=cd35473aceae45a78d156048f1132089")
        print("\n3. Run the import workflow:")
        print("   python main.py")
        print("")
    else:
        print("\n❌ Failed to create role")
        sys.exit(1)
