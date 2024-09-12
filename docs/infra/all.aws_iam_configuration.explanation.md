# AWS IAM Configuration Best Practices Guide

<!-- toc -->

- [Introduction](#introduction)
- [Principle of Least Privilege (PoLP)](#principle-of-least-privilege-polp)
- [Structuring IAM Policies](#structuring-iam-policies)
- [Segmentation of Permissions](#segmentation-of-permissions)
- [Naming Conventions](#naming-conventions)
  * [IAM Roles](#iam-roles)
  * [IAM Groups](#iam-groups)
  * [IAM Group Policies](#iam-group-policies)
  * [IAM Policies](#iam-policies)
  * [Example Naming Conventions](#example-naming-conventions)
      - [Explanation](#explanation)

<!-- tocstop -->

## Introduction

This document outlines the best practices for structuring, segmenting, and
naming AWS Identity and Access Management (IAM) roles, groups, and policies.
Adhering to these practices will ensure that IAM configurations remain scalable,
auditable, and aligned with the principle of least privilege (PoLP), supporting
both current needs and future expansions.

## Principle of Least Privilege (PoLP)

- **Definition**: Ensure that IAM entities (users, roles, groups) have only the
  permissions necessary to perform their assigned tasks, no more, no less. Each
  IAM policy should grant only the permissions necessary for the user or service
  to perform its intended tasks.
- **Application**: Review existing permissions regularly and adjust to fit
  changing requirements while adhering to this minimal access principle. Employ
  conditions in policies to restrict access further, such as limiting actions to
  specific IP ranges, or restricting access between environments by leveraging
  tags.

Ensure that IAM policies grant only the permissions necessary for users to
perform their job functions.

- **Restrictive Resource Access**: Instead of granting broad permissions like
  `"Resource": "*"`, specify resources more explicitly wherever possible. For
  instance, avoid definitions such as `secretsmanager:*`, and `kms:*`. Consider
  specifying only required actions unless absolutely necessary, and always
  restrict the resources to specific ARNs when possible.
- **Action-Specific Policies**: Limit actions to those absolutely necessary. For
  example, if a user or service only needs to read from an S3 bucket, they
  should not have write access.
- **Condition Statements**: Use condition statements to enforce policy
  application under specific circumstances, adding an additional layer of
  security.

## Structuring IAM Policies

- **Atomic Policies**: Create policies that are specific to a single purpose or
  service.
- **Minimize Wildcards**: Use specific resource ARNs instead of broad wildcards
  where practical to limit access scope.
- **Use Conditions**: Apply conditions to control when and how permissions are
  granted.
- **Organize Statements Logically**: Group related permissions into the same
  statement where it makes sense for clarity and manageability. DRY!
- **Separate Critical and Non-critical Access**: Clearly differentiate policies
  handling critical resources (like production databases) from non-critical
  resources. For instance, avoid using `s3:*` permissions on non-critical
  buckets; specify allowed actions.

## Segmentation of Permissions

Segmentation involves dividing IAM policies based on the type of access or
function they serve. This makes policies easier to manage and understand.

- **Functional Segmentation**: Group permissions by AWS service (e.g., ECR, S3,
  ECS) and by the nature of access (read-only, read-write). This would make it
  easier to manage and audit permissions.
- **Resource-Specific Policies**: Instead of using wildcards, specify which
  resources a group or user can access. This minimizes the risk of unintentional
  access.
- **Environment Segmentation**: Differentiate between production and
  non-production environments within policies to prevent accidental access to
  critical resources.
- **Role-Based Access Control (RBAC)**: Assign users to groups based on their
  job function and assign policies to these groups.
- **Temporary Credentials**: Use roles and temporary credentials for short-term
  access, minimizing long-term security risks.

## Naming Conventions

Clear naming conventions help in quickly identifying the purpose of a policy,
which resources it relates to, and the permissions level it grants.

1. **Environment Prefix**: Use prefixes such as `Dev`, `Preprod`, or `Prod` to
   indicate the environment.
2. **Service or Functional Descriptors**: Include the AWS service or the
   function (e.g., `ECR`, `S3`) in the policy name.
3. **Access Level**: Specify the access level (e.g., `ReadOnly`, `ReadWrite`) in
   the policy name.
4. **Resource Type or Identifier**: Where applicable, include a resource
   identifier to specify the scope, to provide additional context about the type
   or specific identifiers of resources involved (e.g., `DataBuckets`).

### IAM Roles

IAM roles should clearly reflect the service and purpose they are designed to
support.

- **Format**: `[Environment][Service][Purpose]Role`
- **Example for EC2**: `ProdEC2InstanceManagementRole`
- **Example for EKS**: `PreprodEKSClusterAdminRole`

This format identifies the environment (Prod, Preprod), the AWS service (EC2,
EKS), the role's purpose (InstanceManagement, ClusterAdmin), and it ends with
the word "Role" to distinguish it as an IAM role.

### IAM Groups

Groups often represent a collection of users with similar permissions. Names
should reflect the organizational units (OUs) or user role they are intended
for:

- **Format**: `[userRole]-[permissionLevel]-group`
- **Example for Developer**: `developer-limited-group`
- **Example for DevOps**: `devops-extended-group`

This convention highlights the role (e.g., Developer, DevOps) and the level of
privilege (e.g., 'Limited' for basic access, 'Extended' for broader permissions,
'Custom' for specially crafted permissions), which are crucial for understanding
what the users in the group can do.

### IAM Group Policies

Group policies should be named similarly to individual IAM policies but should
indicate they are associated with a group.

- **Format**:
  `[Environment][Service][AccessLevel][ResourceIdentifier]GroupPolicy`
- **Example for S3 access**: `PreprodS3ReadOnlyDataBackupGroupPolicy`
- **Example for ECS access**: `DevECSReadOnlyServicesGroupPolicy`

This naming convention makes it clear which environment the policy applies to,
what service it pertains to, the level of access provided, and that it is a
group policy.

### IAM Policies

For IAM policies that apply to specific services, the name should indicate the
environment, service, and scope of access.

- **Format**: `[Environment][Service][AccessLevel][ResourceIdentifier]Policy`
- **Example for DynamoDB**: `PreprodDynamoDBReadWriteTablePolicy`
- **Example for IAM access**: `ProdIAMFullAccessUserPolicy`
- **Example for EC2 access**: `ProdEC2ReadOnlyAirflowPolicy`

### Example Naming Conventions

Here’s an example naming convention for an IAM policy intended for the
development environment, with read-only access to S3 Data Buckets:

- `DevS3ReadOnlyDataBucketsPolicy`

##### Explanation

- `Dev` indicates that this policy is intended for use in the development
  environment.
- `S3` specifies that the policy pertains to Amazon S3 service.
- `ReadOnly` clearly states the permission level, which is read-only access.
- `DataBuckets` tells us that the policy is specifically for actions related to
  data storage buckets.
