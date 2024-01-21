<!-- toc -->

- [S3 Buckets overview](#s3-buckets-overview)
  * [S3 bucket list](#s3-bucket-list)
    + [s3://xyz-data/](#s3xyz-data)
    + [s3://xyz-data.preprod/](#s3xyz-datapreprod)
    + [s3://xyz-data.test/](#s3xyz-datatest)
    + [s3://xyz-data-access-logs/](#s3xyz-data-access-logs)
    + [s3://xyz-data-backup/](#s3xyz-data-backup)
    + [s3://xyz-html/](#s3xyz-html)
    + [s3://xyz-public/](#s3xyz-public)
    + [s3://xyz-unit-test/](#s3xyz-unit-test)

<!-- tocstop -->

# S3 Buckets overview

Many parts of `cmamp` codebase assume and utilize AWS S3 buckets in order to
provide persistent storage and adjacent features of S3.

S3 bucket is an excellent choice for persistent storage medium because of it's
simple interface, versatility and vast number of features. It's important to
note that it is not suitable for time sensitive pipelines, such as realtime data
consumption for forecast computation. This is not what S3 was built for.

In this document we provide and overview of all S3 buckets utilized by our
systems. For each bucket we will describe its purposes from the point of view.
For detailed information related to the configuration of each bucket (lifecycle
rules, replication) refer to
`docs/infra/ck.set_up_s3_buckets.how_to_guide.md`

_Note: based on Kaizen naming conventions each bucket name is prefixed with an
organization identifier, e.g. for a company identifier `xyz` there will be a
main data bucket `xyz-data`_

## S3 bucket list

Here is a list of buckets. Some of them are directly used when performing
operations using the interfaces provided by `cmamp` while others are related to
AWS processes such as accounting, access control, backups etc.:

- S3://xyz-data/
- S3://xyz-data.preprod/
- S3://xyz-data.test/
- S3://xyz-data-access-logs/
- S3://xyz-data-backup/
- S3://xyz-html/
- S3://xyz-public/
- S3://xyz-unit-test/

### s3://xyz-data/

This is the main production data bucket. The main use cases are:

- The main (and only) bucket used for storing business related data
  - Historical data used for running simulations/backtests etc.
  - Archived data moved from more expensive storage such as RDS in order to save
    costs and reduce load on the relational DB.
- We follow a good practice of access restrictions in order to prevent
  accidental overwrites/deletion of data, more on the IAM at this link:
  TODO(Juraj): #CmTask5736
-

### s3://xyz-data.preprod/

- Conceptually the same as `s3://xyz-data/`. The difference is stage usage -
  this bucket is used in preprod stage (More on the concept of stages in the
  corresponding documentation.)

### s3://xyz-data.test/

- Conceptually the same as `s3://xyz-data/` applies, the difference is stage
  usage - this bucket is used in the local/test stage.

### s3://xyz-data-access-logs/

- The main data bucket `s3://xyz-data/` has
  [server access logging](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html?icmpid=docs_amazons3_console)
  enabled. It is an S3 feature useful for accounting, security and access
  audits. More information

### s3://xyz-data-backup/

- Bucket used for any type of data backup beeds
- For disaster recovery purposes the main data bucket `s3://xyz-data/` has
  replication rule enabled, such that every new object or new version of an
  existing object is backed-up to this bucket. (usually the object is replicated
  within a couple of minutes of its upload, depending on its size and the
  current service load)
- Data stored on EFS older than specified amount of days are automatically
  transferred to this bucket for a cheaper storage alternative, more in the EFS
  documentation (TODO(Juraj): #CmTask5740)

### s3://xyz-html/

- A lot of workflows support publishing of html, jupyter notebook or graphical
  files to share results of analyses, PnL graphs, test coverage etc. This bucket
  is configured to privately host a
  [static website](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html)
  to serve the above mentioned files. More information in TODO(Juraj): add link
  #CmTask5741.

### s3://xyz-public/

- Bucket configured to publicly host a
  [static website](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html)
  in order to share information such as PnL charts, with outside
  stakeholders/investors/contributors

### s3://xyz-unit-test/

- Bucket dedicated to operations related to unit testing
  - Golden test outcomes which are too large to store in the repository
  - Scratch space to store temporary S3 files used during tests
