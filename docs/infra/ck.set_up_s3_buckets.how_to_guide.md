# Setup S3 Buckets with Terraform

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
   - [Provider Configuration](#provider-configuration)
3. [Setup Instructions](#setup-instructions)
   - [Module Structure](#module-structure)
   - [Deployment with Terraform](#deployment-with-terraform)
4. [Tips & Considerations](#tips-considerations)

## Introduction <a name="introduction"></a>

This guide provides step-by-step instructions for setting up S3 buckets using
the Terraform module defined in the `terraform/modules/s3` directory. This
module allows for the creation and management of S3 buckets with a range of
configurations including access control, lifecycle rules, versioning,
encryption, and replication.

## Prerequisites <a name="prerequisites"></a>

- **Terraform** v1.5 or later. You can download it from
  [here](https://developer.hashicorp.com/terraform/downloads).
- **AWS CLI** v2 or later. You can download it from
  [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

### Provider Configuration <a name="provider-configuration"></a>

- This project leverages the AWS provider version `~> 4.0`. It's pre-configured
  for the `eu-north-1` region and uses the `ck` AWS profile. Ensure the profile
  has appropriate permissions.

- Prior to execution, ensure your AWS credentials are properly set up.
  Instructions on setting up AWS credentials can be found
  [here](https://registry.terraform.io/providers/hashicorp/aws/latest/docs).

## Setup Instructions <a name="setup-instructions"></a>

### Module Structure <a name="module-structure"></a>

The S3 module is structured as follows:

- `main.tf`: Contains the main set of configurations for creating S3 buckets.
- `outputs.tf`: Defines the output variables.
- `variables.tf`: Defines the input variables.
- `templates/policies`: Contains JSON templates for S3 bucket policies.

### Deployment with Terraform <a name="deployment-with-terraform"></a>

**Step 1: Define S3 Buckets in `terraform.tfvars`**

1. **Specify Bucket Configurations:** Define each bucket's configuration in the
   `buckets` variable. Example:

```hcl
buckets = {
  "S3Bucket1" = {
    name          = "example-bucket-1"
    acl           = "private"
    force_destroy = true
    # Additional configurations...
  },
  # Define more buckets as needed
}
```

2. **Set Access Control Lists (ACL):** Specify the ACL for each bucket
   (`private`, `public-read`, etc.).

3. **Enable Versioning:** Choose to enable or disable versioning for each
   bucket.

4. **Define Lifecycle Rules:** Configure rules for object lifecycle management.

5. **Set Replication and Encryption:** If needed, specify replication and
   encryption configurations.

**Step 2: Initialize and Apply Terraform Configuration**

1. **Initialize Terraform:** Run `terraform init` in the directory containing
   your Terraform configuration to initialize the project.

2. **Apply Configuration:** Run `terraform plan` to review the changes. Execute
   `terraform apply` to create the S3 buckets as per the defined configuration.

**Step 3: Verify Bucket Creation**

1. **Check Terraform Output:** Ensure that Terraform applies the configuration
   without errors.

2. **Verify in AWS Console:** Optionally, log in to the AWS S3 console to
   visually verify that the buckets have been created with the correct settings.

**Step 4: Updating or Deleting Buckets**

- **To Update:** Modify the `terraform.tfvars` file and re-run terraform apply.
- **To Delete:** Remove the bucket definition from `terraform.tfvars` and run
  `terraform apply`.

## Tips & Considerations <a name="tips-considerations"></a>

- **Security:** Ensure that your bucket policies and ACLs are set to restrict
  access appropriately.
- **Naming Conventions:** Use clear and consistent naming for buckets.
- **Versioning:** Enable versioning to safeguard against accidental deletions or
  overwrites.
- **Lifecycle Policies:** Use lifecycle policies to manage object lifecycles and
  reduce costs.
- **Backup and Replication:** Implement bucket replication for backup and
  disaster recovery purposes.
- **Encryption:** Use server-side encryption for sensitive data (Enabled by
  default).
