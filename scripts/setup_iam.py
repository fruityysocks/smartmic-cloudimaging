#!/usr/bin/env python3
"""
Setup IAM permissions for AWS HealthImaging access.

This generalized script configures IAM permissions for AWS HealthImaging.
It can be used for any user, role, or datastore.

Usage:
    python setup_iam.py --datastore-id <ID> [--user <username>] [--roles <role1,role2>]
"""

import boto3
import json
import sys
import argparse
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


def get_policy_document(datastore_id=None, bucket_pattern="*-healthimaging-*"):
    """
    Generate IAM policy document for AWS HealthImaging.

    Args:
        datastore_id: Specific datastore ID, or None for all datastores
        bucket_pattern: S3 bucket pattern for access
    """

    # Resource ARN - use specific datastore or wildcard
    if datastore_id:
        datastore_arn = f"arn:aws:medical-imaging:*:*:datastore/{datastore_id}"
    else:
        datastore_arn = "arn:aws:medical-imaging:*:*:datastore/*"

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ManageImportJobs",
                "Effect": "Allow",
                "Action": [
                    "medical-imaging:StartDICOMImportJob",
                    "medical-imaging:GetDICOMImportJob",
                    "medical-imaging:ListDICOMImportJobs"
                ],
                "Resource": datastore_arn
            },
            {
                "Sid": "SearchAndAccessImageSets",
                "Effect": "Allow",
                "Action": [
                    "medical-imaging:SearchImageSets",
                    "medical-imaging:GetImageSetMetadata",
                    "medical-imaging:GetImageSet",
                    "medical-imaging:GetImageFrame",
                    "medical-imaging:ListImageSetVersions",
                    "medical-imaging:DeleteImageSet"
                ],
                "Resource": datastore_arn
            },
            {
                "Sid": "ListDatastores",
                "Effect": "Allow",
                "Action": [
                    "medical-imaging:ListDatastores",
                    "medical-imaging:GetDatastore"
                ],
                "Resource": "*"
            },
            {
                "Sid": "S3AccessForHealthImaging",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_pattern}",
                    f"arn:aws:s3:::{bucket_pattern}/*"
                ]
            }
        ]
    }


def attach_policy_to_user(iam_client, user_name, policy_name, policy_document):
    """Attach inline policy to IAM user."""
    try:
        print(f"\n📝 Attaching policy to user: {user_name}")

        # Check if user exists
        try:
            iam_client.get_user(UserName=user_name)
        except iam_client.exceptions.NoSuchEntityException:
            print(f"❌ User '{user_name}' does not exist!")
            return False

        # Put user policy (creates or updates)
        iam_client.put_user_policy(
            UserName=user_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )

        print(f"✅ Successfully attached policy '{policy_name}' to user '{user_name}'")
        return True

    except Exception as e:
        print(f"❌ Error attaching policy to user '{user_name}': {e}")
        return False


def attach_policy_to_role(iam_client, role_name, policy_name, policy_document):
    """Attach inline policy to IAM role."""
    try:
        print(f"\n📝 Attaching policy to role: {role_name}")

        # Check if role exists
        try:
            iam_client.get_role(RoleName=role_name)
        except iam_client.exceptions.NoSuchEntityException:
            print(f"❌ Role '{role_name}' does not exist!")
            return False

        # Put role policy (creates or updates)
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )

        print(f"✅ Successfully attached policy '{policy_name}' to role '{role_name}'")
        return True

    except Exception as e:
        print(f"❌ Error attaching policy to role '{role_name}': {e}")
        return False


def get_current_user(iam_client, sts_client):
    """Get current IAM user from credentials."""
    try:
        # Try to get caller identity
        identity = sts_client.get_caller_identity()
        arn = identity['Arn']

        # Extract username from ARN (e.g., arn:aws:iam::123456789012:user/username)
        if ':user/' in arn:
            username = arn.split(':user/')[-1]
            return username
        else:
            return None

    except Exception as e:
        print(f"⚠️  Could not determine current user: {e}")
        return None


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Setup IAM permissions for AWS HealthImaging',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use current user and env datastore
  python setup_iam.py --current-user

  # Specific user and datastore
  python setup_iam.py --user myuser --datastore-id 12e041ae...

  # Multiple roles
  python setup_iam.py --roles HealthImagingServiceRole,AppRole --datastore-id 12e041ae...

  # All datastores (wildcard)
  python setup_iam.py --current-user --all-datastores
        """
    )

    parser.add_argument('--user', help='IAM user name to configure')
    parser.add_argument('--current-user', action='store_true', help='Use current IAM user from credentials')
    parser.add_argument('--roles', help='Comma-separated list of IAM role names')
    parser.add_argument('--datastore-id', help='Specific datastore ID (or use env var)')
    parser.add_argument('--all-datastores', action='store_true', help='Grant access to all datastores (use wildcard)')
    parser.add_argument('--bucket-pattern', default='*-healthimaging-*', help='S3 bucket pattern')
    parser.add_argument('--policy-name', default='HealthImagingAccessPolicy', help='Policy name')

    args = parser.parse_args()

    print("="*60)
    print("AWS HealthImaging IAM Permissions Setup")
    print("="*60)

    # Initialize clients
    try:
        iam_client = boto3.client('iam')
        sts_client = boto3.client('sts')
        print("\n✓ AWS clients initialized")
    except Exception as e:
        print(f"\n❌ Failed to initialize AWS clients: {e}")
        print("\nMake sure you have:")
        print("  1. AWS credentials configured (~/.aws/credentials)")
        print("  2. IAM permissions to manage users and roles")
        sys.exit(1)

    # Determine user
    user_name = None
    if args.current_user:
        user_name = get_current_user(iam_client, sts_client)
        if not user_name:
            print("❌ Could not determine current user. Use --user instead.")
            sys.exit(1)
    elif args.user:
        user_name = args.user

    # Determine roles
    role_names = []
    if args.roles:
        role_names = [r.strip() for r in args.roles.split(',')]

    # Check that at least one target is specified
    if not user_name and not role_names:
        print("❌ Must specify at least one of: --user, --current-user, or --roles")
        print("\nRun with --help for examples")
        sys.exit(1)

    # Determine datastore
    datastore_id = None
    if not args.all_datastores:
        datastore_id = args.datastore_id or os.getenv('AWS_HEALTHIMAGING_DATASTORE_ID')
        if not datastore_id:
            print("❌ Must specify --datastore-id or set AWS_HEALTHIMAGING_DATASTORE_ID env var")
            print("   Or use --all-datastores to grant access to all datastores")
            sys.exit(1)

    # Generate policy
    policy_document = get_policy_document(datastore_id, args.bucket_pattern)

    # Display what will be done
    print(f"\nThis will attach the '{args.policy_name}' policy to:")
    if user_name:
        print(f"  - User: {user_name}")
    for role in role_names:
        print(f"  - Role: {role}")

    print("\nPolicy permissions include:")
    print("  - Manage DICOM import jobs")
    print("  - Search and access image sets")
    print("  - List and describe datastores")
    print("  - Delete image sets")
    print(f"  - S3 access for buckets matching: {args.bucket_pattern}")

    if datastore_id:
        print(f"\nDatastore: {datastore_id}")
    else:
        print(f"\nDatastore: * (ALL DATASTORES)")

    # Confirm before proceeding
    confirm = input("\nProceed? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("Aborted.")
        sys.exit(0)

    # Track success
    results = []

    # Attach policy to user
    if user_name:
        success = attach_policy_to_user(iam_client, user_name, args.policy_name, policy_document)
        results.append(("user", user_name, success))

    # Attach policy to roles
    for role_name in role_names:
        success = attach_policy_to_role(iam_client, role_name, args.policy_name, policy_document)
        results.append(("role", role_name, success))

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    success_count = sum(1 for _, _, success in results if success)
    total_count = len(results)

    print(f"\nAttached {success_count}/{total_count} policies successfully:")
    for entity_type, entity_name, success in results:
        status = "✅" if success else "❌"
        print(f"  {status} {entity_type.capitalize()}: {entity_name}")

    if success_count == total_count:
        print("\n🎉 All permissions configured successfully!")
        print("\nConfigured entities can now:")
        print("  - Import DICOM files from S3")
        print("  - Access and manage image sets")
        print("  - Use the API: python api.py")
        if datastore_id:
            print(f"  - Access datastore: {datastore_id}")
        else:
            print("  - Access ALL datastores")
    else:
        print("\n⚠️  Some policies failed to attach. Check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
