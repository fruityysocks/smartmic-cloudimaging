#!/usr/bin/env python3
"""
Check current AWS configuration and diagnose role issues.
"""

import boto3
import os
from dotenv import load_dotenv
load_dotenv()

print("="*60)
print("AWS Configuration Check")
print("="*60)

# Get current AWS identity
try:
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()

    print("\n✓ Current AWS Identity:")
    print(f"  Account ID: {identity['Account']}")
    print(f"  User ARN: {identity['Arn']}")
    print(f"  User ID: {identity['UserId']}")
except Exception as e:
    print(f"\n❌ Failed to get AWS identity: {e}")
    exit(1)

# Check environment variables
print("\n" + "="*60)
print("Environment Variables")
print("="*60)

datastore_id = os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')
role_arn = os.getenv('AWS_HEALTHIMAGING_ROLE_ARN')
region = os.getenv('AWS_REGION', 'us-east-1')

print(f"\nAWS_HEALTHIMAGING_DATASTORE_ID: {datastore_id or 'Not set'}")
print(f"AWS_HEALTHIMAGING_ROLE_ARN: {role_arn or 'Not set'}")
print(f"AWS_REGION: {region}")

# Check if role ARN is from same account
if role_arn:
    print("\n" + "="*60)
    print("Role ARN Analysis")
    print("="*60)

    # Extract account from role ARN
    # Format: arn:aws:iam::123456789012:role/RoleName
    try:
        parts = role_arn.split(':')
        if len(parts) >= 5:
            role_account = parts[4]
            print(f"\nRole Account: {role_account}")
            print(f"Your Account: {identity['Account']}")

            if role_account != identity['Account']:
                print("\n❌ PROBLEM: Role is from a different AWS account!")
                print("   This causes 'Cross-account pass role is not allowed' error")
                print("\n   Solution: Use a role from your account or configure cross-account access")
            else:
                print("\n✓ Role is in the same account")

                # Check if role exists
                iam = boto3.client('iam')
                role_name = parts[5].split('/')[-1]
                print(f"\nChecking if role exists: {role_name}")

                try:
                    role_response = iam.get_role(RoleName=role_name)
                    print(f"✓ Role exists: {role_name}")

                    # Check trust policy
                    trust_policy = role_response['Role']['AssumeRolePolicyDocument']
                    print("\nTrust policy allows these principals:")
                    for statement in trust_policy.get('Statement', []):
                        principal = statement.get('Principal', {})
                        if isinstance(principal, dict):
                            for key, value in principal.items():
                                print(f"  - {key}: {value}")

                except iam.exceptions.NoSuchEntityException:
                    print(f"❌ Role does not exist: {role_name}")
                except Exception as e:
                    print(f"⚠️  Cannot check role: {e}")
    except Exception as e:
        print(f"\n⚠️  Cannot parse role ARN: {e}")

# List available roles in your account
print("\n" + "="*60)
print("Available IAM Roles (first 20)")
print("="*60)

try:
    iam = boto3.client('iam')
    roles_response = iam.list_roles(MaxItems=20)

    healthimaging_roles = []
    for role in roles_response['Roles']:
        role_name = role['RoleName']
        role_arn = role['Arn']

        # Highlight HealthImaging-related roles
        if 'healthimaging' in role_name.lower() or 'medical' in role_name.lower():
            healthimaging_roles.append((role_name, role_arn))
            print(f"  ✓ {role_name}")
            print(f"    ARN: {role_arn}")

    if not healthimaging_roles:
        print("\n⚠️  No HealthImaging-related roles found")
        print("\nYou need to create a role with:")
        print("  1. Trust policy allowing medical-imaging.amazonaws.com")
        print("  2. Permissions for S3 read/write")
    else:
        print(f"\nFound {len(healthimaging_roles)} HealthImaging-related role(s)")
        print("\nRecommended role ARN to use:")
        print(f"  export AWS_HEALTHIMAGING_ROLE_ARN={healthimaging_roles[0][1]}")

except Exception as e:
    print(f"❌ Cannot list roles: {e}")

# Recommendations
print("\n" + "="*60)
print("Recommendations")
print("="*60)

if not datastore_id:
    print("\n⚠️  Set your datastore ID:")
    print("  export AWS_HEALTHIMAGING_DATASTORE_ID=cd35473aceae45a78d156048f1132089")

if not role_arn:
    print("\n⚠️  No role ARN configured. You need to:")
    print("  1. Create a HealthImaging service role (run: python create_healthimaging_role.py)")
    print("  2. Set the role ARN as environment variable")
elif role_arn and 'PROBLEM' in locals():
    print("\n❌ Your role ARN is incorrect. Fix by:")
    print("  1. Using a role from your account")
    print("  2. Or run: python create_healthimaging_role.py")

print("\n" + "="*60)
